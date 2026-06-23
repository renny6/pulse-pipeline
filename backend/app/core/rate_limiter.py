"""
Atomic Token Bucket Rate Limiter
=================================
[MANDATE — master_system_mandates.md §2 — CONCURRENCY & TIME]
[MANDATE — master_solution_blueprint.md §1 — STATE & CONCURRENCY SOLUTIONS]
[MANDATE — system_hurdles_and_guardrails.md — Challenge 3 & 4]
[MANDATE — advanced_distributed_hurdles.md — Challenge 11 & 13]

Design decisions enforced here:

  [M1] ATOMIC LUA EXECUTION — The entire check-and-consume cycle runs inside
       a single Redis EVAL call. Redis executes Lua scripts on its
       single-threaded engine, making the read-modify-write UNINTERRUPTIBLE
       across multiple concurrent FastAPI nodes. No race conditions possible.

  [M2] GLOBAL CLOCK SOURCE — redis.call('TIME') is the only chronological
       truth. Container clocks MUST NOT be used (datetime.utcnow() is banned).
       This prevents token-bucket drift across distributed FastAPI nodes.

  [M3] EFFECTS-BASED REPLICATION — Redis 7.x (our deployed version) natively
       replicates the EVALUATED RESULT of redis.call('TIME'), not the
       non-deterministic script itself. Replicas converge on the same state.
       No application-level workaround is needed.

  [M4] CONTINUOUS FRACTIONAL REPLENISHMENT — Tokens refill as a smooth
       continuous ramp: new_tokens = old_tokens + (elapsed_seconds × rate).
       This eliminates "thundering herd" spikes caused by rigid block-time
       refill steps where all blocked clients rush in the same microsecond.

  [M5] FAIL-OPEN on Redis error — If Redis is temporarily unreachable, the
       rate limiter returns allowed=True rather than blocking all traffic.
       This preserves gateway availability at the cost of brief un-throttled
       throughput. The error is logged with HIGH severity for operator action.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# ==============================================================================
# LUA SCRIPT — embedded as a module constant, registered once per process.
# Using redis.register_script() caches the SHA1 and uses EVALSHA on subsequent
# calls — avoids re-transmitting the full script body on every request.
# ==============================================================================
_LUA_TOKEN_BUCKET: str = r"""
--
-- ============================================================================
-- PULSE PIPELINE: Atomic Token Bucket — Redis Lua Script
-- ============================================================================
-- KEYS[1]  → Redis key for this bucket, e.g. "pulse:tb:{client_id_hash}"
-- ARGV[1]  → capacity     (integer: maximum tokens a full bucket holds)
-- ARGV[2]  → refill_rate  (float string: tokens-per-second replenishment)
-- ARGV[3]  → requested    (integer: tokens to consume, normally 1)
--
-- RETURNS  → {allowed, remaining_tokens_floor, retry_after_ms}
--   allowed              → 1 (request passes) | 0 (request throttled)
--   remaining_tokens     → floor(tokens left after this call)
--   retry_after_ms       → ms until 'requested' tokens available (0 if allowed)
-- ============================================================================

local key        = KEYS[1]
local capacity   = tonumber(ARGV[1])
local rate       = tonumber(ARGV[2])   -- tokens / second
local requested  = tonumber(ARGV[3])

-- [M2] Single source of chronological truth.
-- TIME returns {unix_seconds (integer), microseconds_within_second (integer)}.
-- Combining them gives microsecond-precision float timestamps.
-- [M3] Redis 7+ replicates the result of TIME, not the call — safe for replicas.
local t    = redis.call('TIME')
local now  = tonumber(t[1]) + tonumber(t[2]) * 0.000001

-- Read existing bucket state (nil on first request for this client).
local raw     = redis.call('HMGET', key, 'tokens', 'ts')
local tokens  = tonumber(raw[1])
local last_ts = tonumber(raw[2])

-- Initialize bucket at full capacity for first-time clients.
if tokens == nil or last_ts == nil then
    tokens  = capacity
    last_ts = now
end

-- [M4] CONTINUOUS FRACTIONAL REPLENISHMENT.
-- elapsed is always >= 0 (math.max guards against any minor clock anomaly).
-- Tokens accumulate as a smooth ramp — NO discrete refill steps.
local elapsed  = math.max(0, now - last_ts)
local refilled = math.min(capacity, tokens + elapsed * rate)

-- Attempt token consumption.
local allowed        = 0
local retry_after_ms = 0

if refilled >= requested then
    refilled = refilled - requested
    allowed  = 1
else
    -- Compute wait time until enough tokens accumulate.
    local deficit    = requested - refilled
    retry_after_ms   = math.ceil(deficit / rate * 1000)
end

-- Persist updated state.
-- TTL = full empty-to-full refill time + 2s cushion. Keys expire naturally
-- when clients go quiet — no manual cleanup needed.
local ttl = math.ceil(capacity / rate) + 2
redis.call('HSET', key, 'tokens', tostring(refilled), 'ts', tostring(now))
redis.call('EXPIRE', key, ttl)

-- Redis automatically truncates Lua floats to integers on return.
-- math.floor is explicit here for clarity.
return {allowed, math.floor(refilled), retry_after_ms}
"""


# ==============================================================================
# RESULT TYPE
# ==============================================================================

@dataclass(frozen=True)
class RateLimitResult:
    """Immutable result from a single token bucket check."""
    allowed: bool
    remaining_tokens: int      # -1 signals Redis failure (fail-open path)
    retry_after_ms: int        # 0 when allowed=True


# ==============================================================================
# RATE LIMITER CLASS
# ==============================================================================

class TokenBucketRateLimiter:
    """
    Distributed, race-condition-free Token Bucket rate limiter.

    One instance is created at application startup and stored in app.state.
    All FastAPI worker coroutines share it via dependency injection.
    """

    _BUCKET_KEY_PREFIX = "pulse:tb:"

    def __init__(
        self,
        redis_client: aioredis.Redis,
        capacity: int,
        refill_rate: float,
    ) -> None:
        self._redis = redis_client
        self._capacity = capacity
        self._refill_rate = refill_rate

        # Register the Lua script with Redis.
        # On first call: SCRIPT LOAD (transmits + caches).
        # On subsequent calls: EVALSHA (just the SHA1 hash — minimal network overhead).
        self._script = redis_client.register_script(_LUA_TOKEN_BUCKET)

        logger.info(
            "TokenBucketRateLimiter ready — capacity=%d, rate=%.1f t/s",
            capacity,
            refill_rate,
        )

    async def check_and_consume(
        self,
        client_id: str,
        tokens_requested: int = 1,
    ) -> RateLimitResult:
        """
        Atomically check and consume tokens for the given client.

        Args:
            client_id: Namespaced identifier string (typically SHA-256 of client IP).
            tokens_requested: Tokens to deduct (defaults to 1 per request).

        Returns:
            RateLimitResult — never raises. On Redis failure, returns
            allowed=True (fail-open) and logs the error at ERROR level.
        """
        key = f"{self._BUCKET_KEY_PREFIX}{client_id}"
        try:
            # [M1] Single EVAL call — atomic across all concurrent gateways
            result: list[int] = await self._script(
                keys=[key],
                args=[
                    str(self._capacity),
                    str(self._refill_rate),
                    str(tokens_requested),
                ],
            )
            return RateLimitResult(
                allowed=bool(result[0]),
                remaining_tokens=int(result[1]),
                retry_after_ms=int(result[2]),
            )

        except Exception as exc:  # noqa: BLE001
            # [M5] Fail-open: preserve gateway availability over rate-limit accuracy
            logger.error(
                "[RATE LIMITER] Redis failure — failing OPEN to preserve availability. "
                "client_id=%s error=%s",
                client_id,
                exc,
                exc_info=True,
            )
            return RateLimitResult(allowed=True, remaining_tokens=-1, retry_after_ms=0)
