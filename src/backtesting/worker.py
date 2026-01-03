from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

# When this file is executed as a script (e.g. `python src/backtesting/worker.py`),
# Python sets sys.path[0] to `.../src/backtesting`, which breaks absolute imports like
# `from backtesting.job_queue import ...`. Ensure `.../src` is on sys.path.
_SRC_DIR = Path(__file__).resolve().parents[1]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from backtesting.job_queue import finalize_backtest_job_failed, finalize_backtest_job_success
from persistence.db import get_async_engine, init_app_schema
from persistence.schema import backtest_jobs


async def _simulate_backtest(*, job: dict[str, Any]) -> dict[str, Any]:
    """MVP backtest simulation.

    Real backtest logic will live in Epic 3 (simulator/strategies).
    For now, we simulate a long-running job and return a structured JSON result.
    """
    seconds = int(os.getenv("BACKTEST_SIMULATE_SECONDS", "300"))  # 5 minutes default
    await asyncio.sleep(max(1, seconds))

    return {
        "job_id": job["id"],
        "status": "completed",
        "stock_ticker": job.get("stock_ticker"),
        "strategy_type": job.get("strategy_type"),
        "generated_at": datetime.utcnow().isoformat(),
        "summary": "MVP: 백테스팅 결과(시뮬레이션)입니다.",
        "metrics": {
            "cumulative_return": 0.0,
            "mdd": 0.0,
            "sharpe_ratio": 0.0,
        },
    }


async def _reserve_one_pending_job() -> Optional[dict[str, Any]]:
    engine = get_async_engine()
    await init_app_schema(engine)

    async with AsyncSession(engine) as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(backtest_jobs)
                    .where(backtest_jobs.c.status == "pending")
                    .order_by(backtest_jobs.c.created_at.asc())
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).mappings().one_or_none()
            if not row:
                return None

            job = dict(row)
            await session.execute(
                update(backtest_jobs)
                .where(backtest_jobs.c.id == job["id"])
                .values(status="in_progress", started_at=datetime.utcnow())
            )
            return job


async def worker_loop(poll_interval_seconds: int = 5) -> None:
    while True:
        job = await _reserve_one_pending_job()
        if not job:
            await asyncio.sleep(poll_interval_seconds)
            continue

        try:
            results = await _simulate_backtest(job=job)
            await finalize_backtest_job_success(
                job_id=int(job["id"]),
                user_id=int(job["user_id"]),
                stock_ticker=job.get("stock_ticker"),
                strategy_type=job.get("strategy_type"),
                results_json=results,
            )
        except Exception as e:
            await finalize_backtest_job_failed(
                job_id=int(job["id"]),
                user_id=int(job["user_id"]),
                stock_ticker=job.get("stock_ticker"),
                strategy_type=job.get("strategy_type"),
                error_message=str(e),
            )


async def main() -> None:
    poll = int(os.getenv("BACKTEST_WORKER_POLL_SECONDS", "5"))
    await worker_loop(poll_interval_seconds=poll)


if __name__ == "__main__":
    asyncio.run(main())


