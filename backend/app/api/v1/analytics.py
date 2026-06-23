"""
Analytics Read Path Endpoint — GET /api/v1/analytics/aggregate
==============================================================
Provides aggregated metrics using TimescaleDB native time-bucketing mechanics.
"""
from __future__ import annotations

import logging
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_session_factory
from app.db.repository import get_aggregated_metrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# Database Session Dependency
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


@router.get(
    "/aggregate",
    summary="Get aggregated metrics from TimescaleDB",
    status_code=status.HTTP_200_OK,
)
async def get_aggregate(
    metric_name: str = Query(default="cpu_load", description="Metric type to aggregate"),
    bucket_minutes: int = Query(default=1, ge=1, le=1440, description="Bucket size in minutes"),
    session: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    """
    Returns time-bucketed aggregation statistics for a given metric.

    Executes a high-performance native TimescaleDB bucket query.
    """
    logger.info(
        "Fetching aggregation. metric_name=%s, bucket_minutes=%d",
        metric_name,
        bucket_minutes,
    )
    data = await get_aggregated_metrics(
        session=session,
        metric_name=metric_name,
        bucket_minutes=bucket_minutes,
    )
    return data
