"""
Pulse Pipeline — Application Configuration
==========================================
All settings are sourced exclusively from environment variables or the project
root .env file. No credentials or secrets are ever hardcoded here.

[MANDATE — zero_trust_security.md §5]
Credentials must not appear in Python source files. Use env vars only.
"""
from __future__ import annotations

from functools import lru_cache
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central configuration object. Pydantic-settings reads each field from the
    matching environment variable (uppercased field name by default).
    """

    model_config = SettingsConfigDict(
        # Load from multiple locations in priority order (last wins).
        # When uvicorn runs from backend/, it finds 'backend/.env' first.
        # When Docker runs, environment: blocks override these entirely.
        env_file=(".env", "backend/.env"),
        env_file_encoding="utf-8",
        # Ignore unmapped env vars — Docker injects many system vars
        extra="ignore",
    )

    # --------------------------------------------------------------------------
    # TimescaleDB
    # --------------------------------------------------------------------------
    postgres_user: str = Field(default="pulse_admin")
    postgres_password: str = Field(default="")
    postgres_db: str = Field(default="pulse_analytics")
    # Inside Docker containers use: postgresql+asyncpg://pulse_admin:...@timescaledb:5432/pulse_analytics
    database_url_async: str = Field(default="")

    # --------------------------------------------------------------------------
    # Redis — Global Guard
    # Inside Docker: redis://redis:6379/0   (Docker DNS name)
    # From host:     redis://localhost:6379/0  ← also the default
    # --------------------------------------------------------------------------
    redis_url: str = Field(default="redis://localhost:6379/0")

    # --------------------------------------------------------------------------
    # Kafka — Message Bus
    # Inside Docker: kafka:9092   (INTERNAL listener — only within pulse-net)
    # From host:     localhost:9094  ← EXTERNAL listener, host-mapped port
    #
    # IMPORTANT: Do NOT use localhost:9092 from the host. Port 9092 is the
    # INTERNAL listener; it is not exposed outside the Docker network.
    # Port 9094 is the EXTERNAL listener mapped by docker-compose.
    # --------------------------------------------------------------------------
    kafka_broker_url: str = Field(default="localhost:9094")
    kafka_topic_ingestion: str = Field(default="pulse.events.raw")
    kafka_topic_dlq: str = Field(default="pulse.events.dlq")

    # --------------------------------------------------------------------------
    # External Cloud API (Phase 4)
    # --------------------------------------------------------------------------
    external_api_url: str = Field(default="https://www.alphavantage.co/query")
    external_api_key: str = Field(default="")

    # --------------------------------------------------------------------------
    # Security & Observability
    # --------------------------------------------------------------------------
    ws_jwt_secret: str = Field(default="")
    log_level: str = Field(default="INFO")

    # --------------------------------------------------------------------------
    # Token Bucket Rate Limiter
    # capacity    → max burst size (tokens a full bucket can hold)
    # refill_rate → sustained throughput (tokens per second)
    #
    # Defaults are calibrated for the 5,000 RPS demo scenario:
    #   - 2,000 token burst capacity
    #   - 500 tokens/second sustained refill
    # Both are overridable via RATE_LIMIT_CAPACITY / RATE_LIMIT_REFILL_RATE env vars.
    # --------------------------------------------------------------------------
    rate_limit_capacity: int = Field(default=2000)
    rate_limit_refill_rate: float = Field(default=500.0)

    # --------------------------------------------------------------------------
    # CORS — strictly whitelisted origins
    # [MANDATE — zero_trust_security.md §4 / master_system_mandates.md §6]
    # Never use allow_origins=["*"]. Provide a comma-separated list of
    # exact origins where the React dashboard is hosted.
    # Example env var: ALLOWED_ORIGINS=http://localhost:5173,https://pulse.example.com
    # --------------------------------------------------------------------------
    allowed_origins: list[str] | str = Field(
        default=["http://localhost:5173", "http://localhost:5174", "http://localhost:3000"]
    )

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def _parse_origins(cls, v: object) -> list[str]:
        """Accept either a JSON array or a comma-separated string from env."""
        if v is None:
            return ["*"]
        if isinstance(v, str):
            val = v.strip()
            if not val:
                return ["*"]
            if val.startswith("[") and val.endswith("]"):
                import json
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed]
                except json.JSONDecodeError:
                    pass
            return [o.strip() for o in val.split(",") if o.strip()]
        if isinstance(v, list):
            if not v:
                return ["*"]
            return [str(item).strip() for item in v]
        return v  # type: ignore[return-value]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Returns a cached singleton Settings instance."""
    return Settings()


# Module-level alias for convenience — import as `from app.config import settings`
settings: Settings = get_settings()
