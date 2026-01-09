from __future__ import annotations

import os
import re
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from backtesting.web_db import get_backtesting_job, insert_backtesting_job

logger = logging.getLogger(__name__)

# ============================================================
# Routers
# - router: Legacy endpoints (/backtesting/*)
# - api_router: Architecture-compatible endpoints (/api/backtesting/*)
# ============================================================

router = APIRouter(prefix="/backtesting", tags=["backtesting"])
api_router = APIRouter(prefix="/api/backtesting", tags=["backtesting"])


_STOCK_CODE_RE = re.compile(r"\b\d{6}\b")


def _resolve_results_dir() -> Path:
    # worker와 동일한 기본값을 사용
    return Path(os.getenv("BACKTEST_RESULTS_DIR", "outputs/backtesting_results")).resolve()


def _is_under_base(path: Path, base: Path) -> bool:
    try:
        path.resolve().relative_to(base.resolve())
        return True
    except Exception:
        return False


class BacktestingExecuteRequest(BaseModel):
    """LLM/외부 서비스에서 백테스팅을 트리거할 때 사용하는 입력.

    NOTE: stockelper-llm이 현재 아래 형태로 호출합니다.
      { "user_id": <int>, "stock_symbol": null, "strategy_type": null, "query": "<text>" }

    설계상, `parameters`(구조화 파라미터)를 함께 넘기면 worker가 더 정확히 BacktestInput으로 매핑할 수 있습니다.
    """

    user_id: int = Field(description="stockelper_web.users.id")

    # Legacy/compat fields
    stock_symbol: Optional[str] = Field(
        default=None, description="6자리 종목코드. 제공 시 단일 종목 백테스트로 해석"
    )
    stock_ticker: Optional[str] = Field(
        default=None,
        description="레거시 호환: stock_symbol과 동일 의미",
    )
    strategy_type: Optional[str] = Field(
        default=None,
        description="레거시 호환: sort_by에 매핑(momentum/market_cap/disclosure 등)",
    )
    query: Optional[str] = Field(
        default=None,
        description="자연어 요청(예: '삼성전자 2023~2024 백테스트'). worker가 종목/기간 추정에 활용",
    )

    # New structured payload (recommended)
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "BacktestInput에 대응되는 구조화 파라미터(권장). "
            "예: start_date/end_date/target_symbols/max_positions/event_indicator_conditions 등"
        ),
    )


class BacktestingExecuteResponse(BaseModel):
    id: str
    job_id: str
    status: str


class BacktestingJobView(BaseModel):
    id: str
    job_id: str
    user_id: int
    status: str
    request_source: Optional[str] = None
    input_json: Dict[str, Any] = Field(default_factory=dict)
    output_json: Dict[str, Any] = Field(default_factory=dict)
    result_file_path: Optional[str] = None
    report_file_path: Optional[str] = None
    error_message: Optional[str] = None
    # LLM 해석(후처리)
    analysis_status: Optional[str] = None
    analysis_md: Optional[str] = None
    analysis_json: Dict[str, Any] = Field(default_factory=dict)
    analysis_model: Optional[str] = None
    analysis_prompt_version: Optional[str] = None
    analysis_error_message: Optional[str] = None
    analysis_started_at: Optional[str] = None
    analysis_completed_at: Optional[str] = None
    analysis_elapsed_seconds: Optional[float] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    elapsed_seconds: Optional[float] = None


def _extract_candidate_symbols(body: BacktestingExecuteRequest) -> list[str]:
    """최소 안전장치: query에서 6자리 종목코드를 찾아 미리 candidate로 저장.

    실제 최종 매핑/결정은 worker가 수행하지만,
    placeholder row에 기록해두면 디버깅/운영이 쉬워집니다.
    """

    symbols: list[str] = []
    if body.stock_symbol and isinstance(body.stock_symbol, str):
        symbols.append(body.stock_symbol.strip())
    if body.stock_ticker and isinstance(body.stock_ticker, str):
        symbols.append(body.stock_ticker.strip())
    q = (body.query or "").strip()
    if q:
        symbols.extend(_STOCK_CODE_RE.findall(q))

    # normalize unique
    symbols = [s for s in symbols if s and len(s) == 6 and s.isdigit()]
    return sorted(set(symbols))


# ============================================================
# New endpoints (Architecture-compatible)
# ============================================================


@api_router.post("/execute", response_model=BacktestingExecuteResponse, status_code=status.HTTP_200_OK)
async def execute_backtesting(body: BacktestingExecuteRequest):
    """백테스트를 '요청'만 등록합니다.

    - 요청 즉시 `public.backtesting`에 placeholder row를 생성합니다.
    - 실제 백테스트 실행은 별도 워커가 DB 폴링으로 처리합니다.
    """

    # 기록용: 최소 candidate 추정
    candidates = _extract_candidate_symbols(body)

    input_json: Dict[str, Any] = body.model_dump()
    input_json["candidate_symbols"] = candidates

    try:
        job = await insert_backtesting_job(
            user_id=body.user_id,
            input_json=input_json,
            request_source="llm",
        )
    except Exception as e:
        # 운영에서 502만 찍히는 문제를 줄이기 위해 상세 로그를 남깁니다.
        logger.exception("backtesting job insert failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"backtesting job insert failed: {e}",
        ) from e

    return {"id": job["id"], "job_id": job["job_id"], "status": job["status"]}


@api_router.get("/{job_id}/status", response_model=BacktestingJobView)
async def get_status(job_id: str, user_id: int = Query(..., description="stockelper_web.users.id")):
    row = await get_backtesting_job(job_id=job_id, user_id=user_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return row


@api_router.get("/{job_id}/result", response_model=BacktestingJobView)
async def get_result(job_id: str, user_id: int = Query(..., description="stockelper_web.users.id")):
    # 현재는 status와 동일 payload를 반환합니다.
    # (output_json/result_file_path/report_file_path를 포함)
    row = await get_backtesting_job(job_id=job_id, user_id=user_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return row


@api_router.get("/{job_id}/artifact")
async def get_artifact(
    job_id: str,
    user_id: int = Query(..., description="stockelper_web.users.id"),
    kind: str = Query("json", pattern="^(json|md)$", description="json 또는 md"),
):
    """결과 파일 자체를 내려주는 엔드포인트.

    LLM 서버가 `public.backtesting`의 file_path를 직접 읽을 수 없으므로(컨테이너/호스트 분리),
    backtesting 서비스가 파일을 스트리밍해주는 형태가 필요합니다.
    """

    row = await get_backtesting_job(job_id=job_id, user_id=user_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")

    # 파일 경로 선택
    file_path_str = row.get("result_file_path") if kind == "json" else row.get("report_file_path")
    if not file_path_str:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="artifact not ready")

    p = Path(str(file_path_str)).resolve()
    base = _resolve_results_dir()
    if not _is_under_base(p, base):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid artifact path")
    if not p.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="artifact file missing")

    media_type = "application/json" if kind == "json" else "text/markdown; charset=utf-8"
    return FileResponse(path=str(p), media_type=media_type, filename=p.name)


# ============================================================
# Legacy endpoints (minimal compatibility)
# ============================================================


@router.post("/jobs", response_model=BacktestingExecuteResponse, status_code=status.HTTP_200_OK)
async def create_job_legacy(body: BacktestingExecuteRequest):
    """레거시 호환: `/backtesting/jobs`는 `/api/backtesting/execute`와 동일하게 동작."""
    return await execute_backtesting(body)


@router.get("/jobs/{job_id}", response_model=BacktestingJobView)
async def get_job_legacy(job_id: str, user_id: int = Query(..., description="stockelper_web.users.id")):
    """레거시 호환: `/backtesting/jobs/{job_id}` 조회."""
    return await get_status(job_id=job_id, user_id=user_id)
