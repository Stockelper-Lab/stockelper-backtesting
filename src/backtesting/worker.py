from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict

from backtesting.agents.orchestrator import process_job
from backtesting.web_db import claim_next_pending_job

logger = logging.getLogger(__name__)


def _require_env(key: str) -> str:
    value = (os.getenv(key) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


async def _process_one(job: Dict[str, Any]) -> None:
    """claim된 job(dict)을 OpenAI Agents SDK 파이프라인으로 처리합니다(Agents-only)."""

    await process_job(job)


async def main() -> None:
    # Agents-only: 워커 기동 전에 필수 키를 검증하여, 잡을 잘못 실패 처리하지 않도록 합니다.
    _require_env("OPENAI_API_KEY")

    poll_seconds = float(os.getenv("BACKTEST_WORKER_POLL_SECONDS", "5") or 5)

    while True:
        job = await claim_next_pending_job()
        if not job:
            await asyncio.sleep(poll_seconds)
            continue

        try:
            await _process_one(job)
        except Exception:
            # process_job 내부에서 persist_failed로 마킹하려고 시도합니다.
            # 여기서는 워커 루프가 죽지 않도록만 보장합니다.
            logger.exception("worker failed while processing job: %s", job)


if __name__ == "__main__":
    asyncio.run(main())

