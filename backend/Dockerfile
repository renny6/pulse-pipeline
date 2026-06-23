# ==============================================================================
# PULSE PIPELINE — Multi-Stage Dockerfile
# ==============================================================================
# [MANDATE — zero_trust_security.md §5]
# Absolutely no credentials in ENV instructions. All secrets are injected at
# container runtime via docker-compose env_file / environment overrides.
# ==============================================================================

# --- Stage 1: Build Stage ---
FROM python:3.12-slim AS builder

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Install compiler/build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment to isolate dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install and upgrade pip/wheel
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip wheel \
    && pip install --no-cache-dir -r requirements.txt


# --- Stage 2: Final Runtime Stage ---
FROM python:3.12-slim AS runtime

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Install runtime-only dependencies (curl for health check probes, procps for process checks)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create a non-privileged system user and group for security hardening
RUN groupadd --gid 10001 pulse_group \
    && useradd --uid 10001 --gid pulse_group --shell /bin/bash --create-home pulse_user

# Copy application source code
COPY app/ ./app/

# Set safe permissions for the non-root user
RUN chown -R pulse_user:pulse_group /app /opt/venv

# Run the container under the non-privileged user context
USER pulse_user

# Expose FastAPI Gateway port
EXPOSE 8000

# Default entrypoint runs uvicorn FastAPI gateway.
# Overridden in docker-compose.yml for celery-worker and kafka-consumer.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--log-level", "info", \
     "--access-log", \
     "--no-server-header"]
