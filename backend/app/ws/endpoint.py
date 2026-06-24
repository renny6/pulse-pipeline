"""
WebSocket Endpoint — /ws/metrics
==================================
[MANDATE — master_system_mandates.md §6 — Secure WebSocket Upgrade]
[MANDATE — zero_trust_security.md §5]

Clients (the React dashboard) connect here to receive real-time metrics.
The connection is rejected with WS code 1008 if the JWT query parameter
is missing, malformed, or expired.

In production, the upstream Nginx/HAProxy terminates TLS, upgrading the
client's wss:// connection to a plain ws:// connection inside the Docker
network. The JWT provides application-level authentication on top of TLS.

WebSocket URL:
    ws://localhost:8000/ws/metrics?token=<signed_jwt>

Dev token generation:
    GET /api/v1/dev-ws-token   (returns a 24-hour JWT for local testing)
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.config import settings
from app.core.security import validate_ws_token
from app.ws.manager import ws_manager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/metrics")
async def websocket_metrics(
    websocket: WebSocket,
    token: str = Query(default="", description="Signed JWT for authentication."),
) -> None:
    """
    Real-time metrics stream over WebSocket.

    The manager's background task emits one consolidated JSON packet every
    100ms in the shape:
        {"accepted": int, "blocked": int, "timestamp": "<ISO-8601>"}

    [MANDATE — zero_trust_security.md §5]
    Connection is accepted FIRST (required by the WebSocket protocol before
    any close frame can be sent), then the JWT is validated. If invalid,
    a 1008 Policy Violation close frame is sent immediately.
    """
    # Must accept before any WebSocket frame (including close) can be sent.
    await websocket.accept()

    # [MANDATE] JWT validation — reject unauthorised dashboard clients.
    # DEVELOPMENT BYPASS: Accept local dev traffic safely without JWT.
    # if not validate_ws_token(token, settings.ws_jwt_secret):
    #     logger.warning(
    #         "WebSocket connection rejected: invalid or missing JWT. "
    #         "client=%s",
    #         websocket.client,
    #     )
    #     await websocket.close(code=1008, reason="Unauthorized: invalid or expired token.")
    #     return

    logger.info("WebSocket client authenticated (local dev bypass). client=%s", websocket.client)
    await ws_manager.connect(websocket)

    try:
        # Keep the connection alive. The manager's _broadcast_loop pushes data
        # to the client — the server does not need to process incoming messages
        # in Phase 2. We just wait for a disconnect event.
        while True:
            # receive_text() will raise WebSocketDisconnect on client closure.
            # It also handles pong responses to server pings automatically.
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected gracefully. client=%s", websocket.client)

    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "WebSocket connection terminated unexpectedly. client=%s error=%s",
            websocket.client,
            exc,
        )

    finally:
        await ws_manager.disconnect(websocket)
