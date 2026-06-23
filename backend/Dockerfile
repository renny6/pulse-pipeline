# ==============================================================================
# PULSE PIPELINE — FastAPI Gateway Dockerfile
# ==============================================================================
# [MANDATE — zero_trust_security.md §5]
# Absolutely no credentials in ENV instructions. All secrets are injected at
# container runtime via docker-compose env_file / environment overrides.
# ==============================================================================

FROM python:3.11-slim

# Install curl for the Docker health check probe.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install dependencies BEFORE source code to maximise Docker layer
# caching — a source change won't invalidate the pip layer.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/

EXPOSE 8000

# Single uvicorn worker for Phase 2.
# The gateway is stateless; horizontal scaling is handled by docker compose
# --scale flag or an upstream load balancer (Phase 3).
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--log-level", "info", \
     "--access-log", \
     "--no-server-header"]
