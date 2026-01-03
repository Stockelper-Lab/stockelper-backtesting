from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import desc, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from persistence.db import get_async_engine, init_app_schema
from persistence.schema import notifications


async def create_notification(
    *,
    user_id: int,
    type: str,
    title: str,
    message: str,
    data: Optional[dict[str, Any]] = None,
) -> int:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        stmt = (
            insert(notifications)
            .values(
                user_id=user_id,
                type=type,
                title=title,
                message=message,
                data=data,
                is_read=False,
            )
            .returning(notifications.c.id)
        )
        result = await session.execute(stmt)
        await session.commit()
        return int(result.scalar_one())


async def list_notifications(
    *,
    user_id: int,
    unread_only: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        stmt = select(notifications).where(notifications.c.user_id == user_id)
        if unread_only:
            stmt = stmt.where(notifications.c.is_read.is_(False))
        stmt = stmt.order_by(desc(notifications.c.created_at)).limit(limit)
        rows = (await session.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def mark_notification_read(*, user_id: int, notification_id: int) -> bool:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        stmt = (
            update(notifications)
            .where(notifications.c.id == notification_id)
            .where(notifications.c.user_id == user_id)
            .where(notifications.c.is_read.is_(False))
            .values(is_read=True, read_at=datetime.utcnow())
        )
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount > 0


