from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from backtesting.job_queue import create_backtest_job, get_backtest_job, get_backtest_result

router = APIRouter(prefix="/backtesting", tags=["backtesting"])
api_router = APIRouter(prefix="/api/backtesting", tags=["backtesting"])


class BacktestCreateRequest(BaseModel):
    user_id: int = Field(description="User ID")
    stock_ticker: str | None = Field(default=None, description="Stock ticker/code (optional)")
    strategy_type: str | None = Field(default=None, description="Strategy identifier (optional)")
    query: str | None = Field(default=None, description="Original user request (optional)")


@router.post("/jobs", status_code=status.HTTP_201_CREATED)
async def create_job(body: BacktestCreateRequest):
    job_id = await create_backtest_job(
        user_id=body.user_id,
        stock_ticker=body.stock_ticker,
        strategy_type=body.strategy_type,
        input_json={"query": body.query} if body.query else None,
    )
    return {"job_id": job_id, "status": "pending"}


@router.get("/jobs/{job_id}", status_code=status.HTTP_200_OK)
async def get_job(job_id: int, user_id: int):
    job = await get_backtest_job(job_id=job_id, user_id=user_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="백테스트 작업을 찾을 수 없습니다.")

    result = None
    if job.get("status") == "completed":
        result = await get_backtest_result(job_id=job_id, user_id=user_id)

    return {"job": job, "result": result}


class BacktestExecuteRequest(BaseModel):
    """아키텍처 문서(`/api/backtesting/execute`)와 맞춘 실행 요청 스키마."""

    user_id: int = Field(description="User ID")
    stock_symbol: str | None = Field(default=None, description="Stock symbol/code (optional)")
    strategy_type: str | None = Field(default=None, description="Strategy identifier (optional)")
    query: str | None = Field(default=None, description="Original user request (optional)")


def _map_job_status(status_str: str | None) -> tuple[str, int]:
    """DB 상태값을 API 표준 상태 + progress_pct로 매핑."""
    if status_str == "pending":
        return "queued", 0
    if status_str == "in_progress":
        return "running", 50
    if status_str == "completed":
        return "completed", 100
    if status_str == "failed":
        return "failed", 100
    return "queued", 0


@api_router.post("/execute", status_code=status.HTTP_201_CREATED)
async def execute_backtest(body: BacktestExecuteRequest):
    job_id = await create_backtest_job(
        user_id=body.user_id,
        stock_ticker=body.stock_symbol,
        strategy_type=body.strategy_type,
        input_json={"query": body.query} if body.query else None,
    )
    return {"job_id": str(job_id), "status": "queued"}


@api_router.get("/{job_id}/status", status_code=status.HTTP_200_OK)
async def get_backtest_status(job_id: int, user_id: int):
    job = await get_backtest_job(job_id=job_id, user_id=user_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="백테스트 작업을 찾을 수 없습니다.")

    status_mapped, progress = _map_job_status(job.get("status"))
    payload: dict = {
        "job_id": str(job_id),
        "status": status_mapped,
        "progress_pct": progress,
    }

    if status_mapped == "completed":
        result_row = await get_backtest_result(job_id=job_id, user_id=user_id)
        payload["result"] = result_row.get("results_json") if result_row else None
    if status_mapped == "failed":
        payload["error_message"] = job.get("error_message")

    return payload


@api_router.get("/{job_id}/result", status_code=status.HTTP_200_OK)
async def get_backtest_result_api(job_id: int, user_id: int):
    job = await get_backtest_job(job_id=job_id, user_id=user_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="백테스트 작업을 찾을 수 없습니다.")

    if job.get("status") != "completed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="백테스팅이 아직 완료되지 않았습니다.",
        )

    result_row = await get_backtest_result(job_id=job_id, user_id=user_id)
    if not result_row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="백테스트 결과를 찾을 수 없습니다.")

    return {"job_id": str(job_id), "result": result_row.get("results_json")}


