from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import desc, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.service import create_notification
from persistence.db import get_async_engine, init_app_schema
from persistence.schema import backtest_jobs, backtest_results


async def create_backtest_job(
    *,
    user_id: int,
    stock_ticker: Optional[str],
    strategy_type: Optional[str],
    input_json: Optional[dict[str, Any]] = None,
) -> int:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        stmt = (
            insert(backtest_jobs)
            .values(
                user_id=user_id,
                stock_ticker=stock_ticker,
                strategy_type=strategy_type,
                status="pending",
                input_json=input_json,
                retry_count=0,
            )
            .returning(backtest_jobs.c.id)
        )
        result = await session.execute(stmt)
        await session.commit()
        job_id = int(result.scalar_one())

    # (선택) 시작 알림은 UX에 따라 추가 가능. MVP에서는 완료 알림만 사용.
    return job_id


async def get_backtest_job(*, job_id: int, user_id: int) -> Optional[dict[str, Any]]:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        row = (
            await session.execute(
                select(backtest_jobs)
                .where(backtest_jobs.c.id == job_id)
                .where(backtest_jobs.c.user_id == user_id)
            )
        ).mappings().one_or_none()
        return dict(row) if row else None


async def get_backtest_result(*, job_id: int, user_id: int) -> Optional[dict[str, Any]]:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        row = (
            await session.execute(
                select(backtest_results)
                .where(backtest_results.c.job_id == job_id)
                .where(backtest_results.c.user_id == user_id)
                .order_by(desc(backtest_results.c.generated_at))
                .limit(1)
            )
        ).mappings().one_or_none()
        return dict(row) if row else None


async def finalize_backtest_job_success(
    *,
    job_id: int,
    user_id: int,
    stock_ticker: Optional[str],
    strategy_type: Optional[str],
    results_json: dict[str, Any],
) -> None:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        await session.execute(
            insert(backtest_results).values(
                job_id=job_id,
                user_id=user_id,
                stock_ticker=stock_ticker,
                strategy_type=strategy_type,
                results_json=results_json,
            )
        )
        await session.execute(
            update(backtest_jobs)
            .where(backtest_jobs.c.id == job_id)
            .values(status="completed", completed_at=datetime.utcnow())
        )
        await session.commit()

    await create_notification(
        user_id=user_id,
        type="backtesting_complete",
        title="백테스팅 완료",
        message="백테스팅이 완료되었습니다. 결과를 확인해주세요.",
        data={"job_id": job_id, "stock_ticker": stock_ticker, "strategy_type": strategy_type},
    )


async def finalize_backtest_job_failed(
    *,
    job_id: int,
    user_id: int,
    error_message: str,
    stock_ticker: Optional[str] = None,
    strategy_type: Optional[str] = None,
) -> None:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        await session.execute(
            update(backtest_jobs)
            .where(backtest_jobs.c.id == job_id)
            .values(
                status="failed",
                error_message=error_message,
                completed_at=datetime.utcnow(),
            )
        )
        await session.commit()

    await create_notification(
        user_id=user_id,
        type="backtesting_complete",
        title="백테스팅 실패",
        message="백테스팅 실행 중 오류가 발생했습니다. 다시 시도해주세요.",
        data={"job_id": job_id, "stock_ticker": stock_ticker, "strategy_type": strategy_type, "error": error_message},
    )


