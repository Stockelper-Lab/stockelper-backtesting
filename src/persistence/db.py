from __future__ import annotations

import os
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .schema import metadata

_ASYNC_ENGINE: Optional[AsyncEngine] = None


def _to_async_db_url(url: str | None) -> str | None:
    """Convert Prisma-style DATABASE_URL to SQLAlchemy async URL if needed."""
    if not url:
        return None
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


def get_async_engine() -> AsyncEngine:
    """Get a cached AsyncEngine for the primary app database (llm_users)."""
    global _ASYNC_ENGINE
    if _ASYNC_ENGINE is not None:
        return _ASYNC_ENGINE

    async_db_url = os.getenv("ASYNC_DATABASE_URL") or _to_async_db_url(
        os.getenv("DATABASE_URL")
    )
    if not async_db_url:
        raise RuntimeError("ASYNC_DATABASE_URL 또는 DATABASE_URL 이 설정되어 있지 않습니다.")

    _ASYNC_ENGINE = create_async_engine(async_db_url, echo=False)
    return _ASYNC_ENGINE


async def init_app_schema(engine: Optional[AsyncEngine] = None) -> None:
    """Create required tables if they do not exist (idempotent)."""
    engine = engine or get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


