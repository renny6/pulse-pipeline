"""
WebSocket Broadcast Manager — 100ms Batching Window
=====================================================
[MANDATE — system_hurdles_and_guardrails.md — Challenge 2]
[MANDATE — 03_appflow.md — WebSocket stream every 100ms]

THE PROBLEM THIS SOLVES
-----------------------
At 5,000 RPS, sending one WebSocket message per event would fire 5,000
JSON packets per second to the browser. This saturates the client's TCP
receive buffer and causes severe network backpressure. The browser event
loop falls behind, leading to UI freezing and eventual page crash.

THE SOLUTION
-----------
This manager runs a background asyncio task that:
  1. Accumulates `accepted` and `blocked` counters as events flow through.
  2. Every 100ms, atomically snapshots and resets the counters.
  3. Emits ONE consolidated JSON packet to ALL connected WebSocket clients.

The browser receives at most 10 packets/second regardless of RPS, keeping
the UI responsive even during a 5,000 RPS synthetic traffic spike.

CONCURRENCY MODEL
-----------------
A single asyncio.Lock protects the counter state and connection set.
Since FastAPI runs on a single-threaded asyncio event loop, the lock is
lightweight (no OS thread blocking). Counter updates (record_event) yield
the lock within microseconds — zero impact on gateway response time.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class _MetricsWindow:
    """Mutable accumulator for a single 100ms metrics window."""
    __slots__ = ("accepted", "blocked")

    def __init__(self) -> None:
        self.accepted: int = 0
        self.blocked: int = 0

    def is_empty(self) -> bool:
        return self.accepted == 0 and self.blocked == 0


class WebSocketManager:
    """
    Singleton WebSocket connection manager with 100ms metric batching.

    Lifecycle:
        startup()  → called from FastAPI lifespan on app start
        shutdown() → called from FastAPI lifespan on app stop

    Usage in handlers:
        await ws_manager.record_event(accepted=True)

    Usage in WebSocket endpoint:
        await ws_manager.connect(websocket)
        ...
        await ws_manager.disconnect(websocket)
    """

    def __init__(self, window_ms: int = 100) -> None:
        self._window_ms = window_ms
        self._connections: set[WebSocket] = set()
        self._window = _MetricsWindow()
        self._lock = asyncio.Lock()
        self._broadcaster_task: asyncio.Task | None = None  # type: ignore[type-arg]

    # --------------------------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------------------------

    async def startup(self) -> None:
        """Start the background broadcaster loop. Call from app lifespan."""
        self._broadcaster_task = asyncio.create_task(
            self._broadcast_loop(),
            name="ws-broadcaster",
        )
        logger.info("WebSocket broadcaster started (window=%dms).", self._window_ms)

    async def shutdown(self) -> None:
        """Cancel the broadcaster and close all open connections gracefully."""
        if self._broadcaster_task and not self._broadcaster_task.done():
            self._broadcaster_task.cancel()
            try:
                await self._broadcaster_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            connections_snapshot = set(self._connections)
            self._connections.clear()

        for ws in connections_snapshot:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass

        logger.info("WebSocket broadcaster stopped. All connections closed.")

    # --------------------------------------------------------------------------
    # Connection management
    # --------------------------------------------------------------------------

    async def connect(self, websocket: WebSocket) -> None:
        """Register a new, already-accepted WebSocket connection."""
        async with self._lock:
            self._connections.add(websocket)
        logger.info(
            "WebSocket client connected. Active connections: %d",
            len(self._connections),
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        """Deregister a WebSocket connection (called on disconnect or error)."""
        async with self._lock:
            self._connections.discard(websocket)
        logger.info(
            "WebSocket client disconnected. Active connections: %d",
            len(self._connections),
        )

    # --------------------------------------------------------------------------
    # Metric accumulation — called on EVERY ingestion event
    # --------------------------------------------------------------------------

    async def record_event(self, *, accepted: bool) -> None:
        """
        Thread-safe metric counter increment.

        Designed to be called from every POST /ingest handler coroutine.
        The lock acquisition is sub-microsecond on the asyncio event loop —
        no measurable impact on gateway response time.
        """
        async with self._lock:
            if accepted:
                self._window.accepted += 1
            else:
                self._window.blocked += 1

    # --------------------------------------------------------------------------
    # Background broadcast loop
    # --------------------------------------------------------------------------

    async def _broadcast_loop(self) -> None:
        """
        Runs indefinitely: sleeps 100ms, then emits one consolidated packet.

        [MANDATE — system_hurdles_and_guardrails.md Challenge 2]
        This is the core anti-backpressure mechanism. Maximum WebSocket
        throughput to the browser is capped at 10 packets/second, regardless
        of the upstream RPS being processed by the gateway.
        """
        while True:
            await asyncio.sleep(self._window_ms / 1000.0)

            async with self._lock:
                # Skip emission if no events occurred or no clients connected
                if self._window.is_empty() or not self._connections:
                    continue

                # Atomically snapshot and reset the window under the lock
                payload = {
                    "accepted": self._window.accepted,
                    "blocked": self._window.blocked,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self._window = _MetricsWindow()
                connections_snapshot = set(self._connections)

            # Broadcast OUTSIDE the lock to avoid blocking counter updates
            dead: set[WebSocket] = set()
            for ws in connections_snapshot:
                try:
                    await ws.send_json(payload)
                except Exception:  # noqa: BLE001
                    # Mark dead connections for cleanup; don't crash the loop
                    dead.add(ws)

            if dead:
                async with self._lock:
                    self._connections -= dead
                logger.info(
                    "Cleaned up %d dead WebSocket connection(s).", len(dead)
                )


# ---------------------------------------------------------------------------
# Module-level singleton — imported by endpoint and main.py lifespan
# ---------------------------------------------------------------------------
ws_manager = WebSocketManager(window_ms=100)
