"""
X-Correlation-ID Middleware
===========================
[MANDATE — operations_and_observability_mandates.md §1]

Every incoming HTTP request MUST be assigned a unique X-Correlation-ID UUID.
This ID must be:
  1. Injected into request.state for access by any downstream handler.
  2. Set as a ContextVar so any logger in the call-stack can include it.
  3. Added to every response header so clients can trace their requests.
  4. Printed in all FastAPI gateway log lines.

This fulfils the "distributed blindness" guardrail — every log line across
the entire pipeline ties back to a single origin request ID.
"""
from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ContextVar — propagates the correlation ID to any logger in the same async
# task without threading concerns. Workers extract this in Phase 3.
# ---------------------------------------------------------------------------
correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="N/A")

HEADER_NAME = "X-Correlation-ID"


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    Starlette ASGI middleware that guarantees every request carries a
    traceable X-Correlation-ID from ingestion gateway to Kafka to Celery worker.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        # Honour a pre-existing ID (e.g., from a load balancer or test suite),
        # otherwise generate a fresh UUID4.
        correlation_id = request.headers.get(HEADER_NAME) or str(uuid.uuid4())

        # 1. Inject into request state for direct handler access.
        request.state.correlation_id = correlation_id

        # 2. Set ContextVar so every logger in this async task can read it.
        token = correlation_id_ctx.set(correlation_id)

        logger.info(
            "→ %s %s",
            request.method,
            request.url.path,
            extra={"correlation_id": correlation_id},
        )

        response = await call_next(request)

        # 3. Propagate in response for client-side distributed tracing.
        response.headers[HEADER_NAME] = correlation_id

        # Reset the ContextVar after the request to avoid bleed-over.
        correlation_id_ctx.reset(token)

        return response
