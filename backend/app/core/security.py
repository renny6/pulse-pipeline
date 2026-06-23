"""
Security Utilities
==================
Zero-trust security primitives used across the gateway.

Covers three mandatory concerns from the architecture documents:

  1. IP MASKING    — SHA-256 one-way hash of client IPs before any logging,
                     Redis key creation, or database write.
                     [MANDATE — zero_trust_security.md §5 / backend_schema.md]

  2. SSRF DEFENCE  — Strict blocklist for any outbound httpx/aiohttp call
                     initiated from user-provided payload URLs.
                     [MANDATE — master_system_mandates.md §6 / zero_trust §4]

  3. WS JWT AUTH   — JWT validation for WebSocket upgrade handshakes.
                     [MANDATE — master_system_mandates.md §6 / zero_trust §5]
"""
from __future__ import annotations

import hashlib
import ipaddress
import logging
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from jose import JWTError, jwt

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. IP MASKING
# ==============================================================================

def mask_ip(raw_ip: str) -> str:
    """
    Return the SHA-256 hex digest of a raw IP address.

    [MANDATE — zero_trust_security.md §5]
    Client IP addresses are PII. They must NEVER be stored as plaintext in
    Redis keys, Kafka payloads, or database rows. This one-way hash is used
    as the Token Bucket Redis key suffix and as the `client_ip_hash` DB column.

    Args:
        raw_ip: The literal IP string (e.g. "192.168.1.10" or "::1").

    Returns:
        64-character hex digest (SHA-256), e.g. "a9f3e1...".
    """
    return hashlib.sha256(raw_ip.encode("utf-8")).hexdigest()


def mask_sensitive_fields(payload: dict) -> dict:
    """
    Scrub known sensitive field names from an event payload before it is
    written to Kafka or logs.

    [MANDATE — operations_and_observability_mandates.md §2]
    Kafka logs are IMMUTABLE. Writing raw API keys, tokens, or PII creates
    an irreversible compliance breach.

    Fields replaced with "***REDACTED***":
        authorization, api_key, apikey, secret, token, password,
        passwd, access_token, refresh_token, ssn, credit_card.

    Args:
        payload: The raw event payload dict (shallow copy is made).

    Returns:
        A new dict with sensitive keys replaced.
    """
    _SENSITIVE = frozenset({
        "authorization", "api_key", "apikey", "secret", "token",
        "password", "passwd", "access_token", "refresh_token",
        "ssn", "credit_card", "creditcard", "cvv",
    })

    return {
        k: ("***REDACTED***" if k.lower() in _SENSITIVE else v)
        for k, v in payload.items()
    }


# ==============================================================================
# 2. SSRF DEFENCE
# ==============================================================================

# Exact hostname strings that are always blocked regardless of resolution
_BLOCKED_HOSTNAMES: frozenset[str] = frozenset({
    "localhost",
    "169.254.169.254",          # AWS EC2 / Azure instance metadata
    "metadata.google.internal", # GCP metadata
    "metadata.internal",        # Generic cloud metadata
})

# Docker-internal DNS names — workers must never call these as external targets
_BLOCKED_DNS_PATTERN = re.compile(
    r"^(redis|kafka|zookeeper|timescaledb|db|api|worker)$",
    re.IGNORECASE,
)

# CIDR ranges that are always private/internal
_BLOCKED_NETWORKS: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = [
    ipaddress.ip_network("127.0.0.0/8"),      # IPv4 loopback
    ipaddress.ip_network("10.0.0.0/8"),        # RFC 1918 private
    ipaddress.ip_network("172.16.0.0/12"),     # RFC 1918 private (includes Docker 172.28.x.x)
    ipaddress.ip_network("192.168.0.0/16"),    # RFC 1918 private
    ipaddress.ip_network("169.254.0.0/16"),    # Link-local (cloud metadata)
    ipaddress.ip_network("::1/128"),           # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),          # IPv6 unique local
    ipaddress.ip_network("fe80::/10"),         # IPv6 link-local
]


def is_ssrf_safe(url: str) -> bool:
    """
    Returns True ONLY if the URL is safe for outbound worker requests.

    [MANDATE — master_system_mandates.md §6 / zero_trust_security.md §4]
    Any URL that resolves to an internal network, Docker DNS name, or cloud
    metadata endpoint MUST be rejected to prevent Server-Side Request Forgery.

    Fails CLOSED — an unparseable URL returns False.

    Args:
        url: The full URL string to evaluate.

    Returns:
        True if safe, False if blocked.
    """
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower().strip()

        if not host:
            return False

        if host in _BLOCKED_HOSTNAMES:
            logger.warning("SSRF block: hostname '%s' is on the explicit blocklist.", host)
            return False

        if _BLOCKED_DNS_PATTERN.match(host):
            logger.warning("SSRF block: '%s' matches internal Docker DNS pattern.", host)
            return False

        # Attempt IP-based network check
        try:
            addr = ipaddress.ip_address(host)
            for network in _BLOCKED_NETWORKS:
                if addr in network:
                    logger.warning(
                        "SSRF block: IP %s falls within blocked network %s.", addr, network
                    )
                    return False
        except ValueError:
            pass  # Not a raw IP — hostname checks above cover it

        return True

    except Exception:  # noqa: BLE001
        logger.error("SSRF check failed for URL '%s' — failing closed.", url, exc_info=True)
        return False


# ==============================================================================
# 3. WEBSOCKET JWT AUTHENTICATION
# ==============================================================================

_WS_JWT_ALGORITHM = "HS256"


def validate_ws_token(token: str, secret: str) -> bool:
    """
    Validate a signed JWT for the WebSocket upgrade handshake.

    [MANDATE — master_system_mandates.md §6 / zero_trust_security.md §5]
    Standard ws:// connections have no built-in auth. The React client must
    pass a short-lived JWT as a query parameter on connect. Invalid or expired
    tokens cause the connection to be rejected with WS code 1008.

    Args:
        token:  The raw JWT string from the ?token= query parameter.
        secret: The WS_JWT_SECRET from settings.

    Returns:
        True if the token is valid and not expired, False otherwise.
    """
    if not token or not secret:
        return False
    try:
        jwt.decode(token, secret, algorithms=[_WS_JWT_ALGORITHM])
        return True
    except JWTError as exc:
        logger.warning("WebSocket JWT validation failed: %s", exc)
        return False


def create_dev_ws_token(secret: str, expires_hours: int = 24) -> str:
    """
    Generate a development JWT for the WebSocket connection.

    This endpoint is ONLY for local development convenience. In production,
    tokens should be issued by a proper auth service with short expiry (≤15m).

    Args:
        secret:        The WS_JWT_SECRET from settings.
        expires_hours: Token validity window (default 24h for dev).

    Returns:
        Signed JWT string.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": "pulse_dashboard",
        "iat": now,
        "exp": now + timedelta(hours=expires_hours),
        "scope": "ws:metrics",
    }
    return jwt.encode(payload, secret, algorithm=_WS_JWT_ALGORITHM)
