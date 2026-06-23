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
        # Look for .env in the project root (one level above backend/)
        env_file=".env",
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
    # From host:     redis://localhost:6379/0
    # --------------------------------------------------------------------------
    redis_url: str = Field(default="redis://redis:6379/0")

    # --------------------------------------------------------------------------
    # Kafka — Message Bus
    # Inside Docker: kafka:9092            (INTERNAL listener)
    # From host:     localhost:9094        (EXTERNAL listener)
    # --------------------------------------------------------------------------
    kafka_broker_url: str = Field(default="kafka:9092")
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
    allowed_origins: list[str] = Field(
        default=["http://localhost:5173", "http://localhost:5174", "http://localhost:3000"]
    )

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def _parse_origins(cls, v: object) -> list[str]:
        """Accept either a JSON array or a comma-separated string from env."""
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v  # type: ignore[return-value]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Returns a cached singleton Settings instance."""
    return Settings()


# Module-level alias for convenience — import as `from app.config import settings`
settings: Settings = get_settings()
