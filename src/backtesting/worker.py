from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import text

from backtesting.portfolio_backtest import BacktestInput, DataLoader, run_backtest
from backtesting.web_db import claim_next_pending_job, mark_job_completed, mark_job_failed

logger = logging.getLogger(__name__)

_STOCK_CODE_RE = re.compile(r"\b\d{6}\b")
_HANGUL_TOKEN_RE = re.compile(r"[가-힣A-Za-z]{2,}")

_QUERY_STOPWORDS = {
    "백테스트",
    "백테스팅",
    "backtest",
    "backtesting",
    "기간",
    "수익률",
    "전략",
    "리밸런싱",
    "포트폴리오",
}


async def _trigger_llm_interpretation(*, job_id: str, user_id: int) -> None:
    """백테스트 완료 직후 LLM 서버에 해석을 트리거(비동기 후처리).

    - 실패해도 백테스트 결과(completed)는 유지되어야 하므로 best-effort로 동작합니다.
    - 실제 해석 수행/DB 업데이트는 stockelper-llm이 담당합니다.
    """

    llm_base = os.getenv("STOCKELPER_LLM_URL", "").strip().rstrip("/")
    if not llm_base:
        return

    timeout_s = float(os.getenv("BACKTEST_LLM_TRIGGER_TIMEOUT", "10") or 10)
    try:
        import httpx

        async with httpx.AsyncClient(timeout=timeout_s) as client:
            await client.post(
                f"{llm_base}/internal/backtesting/interpret",
                json={"user_id": int(user_id), "job_id": str(job_id), "force": False},
            )
    except Exception:
        # 로그 시스템 확정 전까지는 조용히 무시 (운영 시 logger로 교체 권장)
        pass


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _resolve_backtest_input(payload: Dict[str, Any]) -> BacktestInput:
    """DB에 저장된 input_json(payload)을 BacktestInput으로 변환.

    지원 입력 형태:
    - LLM 기본 포맷: {user_id, stock_symbol, strategy_type, query, parameters?}
    - 권장 포맷: { ..., parameters: {BacktestInput에 해당하는 키들...} }
    """
    # asyncpg가 jsonb를 문자열로 반환할 수 있으므로 파싱
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}

    params: Dict[str, Any] = {}

    raw_params = payload.get("parameters")
    # parameters도 문자열일 수 있음
    if isinstance(raw_params, str):
        try:
            raw_params = json.loads(raw_params)
        except Exception:
            raw_params = {}
    if isinstance(raw_params, dict):
        params.update(raw_params)

    # Legacy mapping
    stock_symbol = payload.get("stock_symbol") or payload.get("stock_ticker")
    if isinstance(stock_symbol, str) and stock_symbol.strip():
        params.setdefault("target_symbols", [stock_symbol.strip()])

    strategy_type = payload.get("strategy_type")
    if isinstance(strategy_type, str) and strategy_type.strip():
        params.setdefault("sort_by", strategy_type.strip())

    query = payload.get("query")
    if isinstance(query, str) and query.strip():
        # 1) query 내 6자리 종목코드 추출
        found = _STOCK_CODE_RE.findall(query)
        if found and not params.get("target_symbols") and not params.get("target_corp_names"):
            params["target_symbols"] = sorted(set(found))

        # 2) 종목코드가 없으면, 회사명 토큰을 DB(score_table_dart_idc)에서 매칭 시도
        if not params.get("target_symbols") and not params.get("target_corp_names"):
            try:
                tokens = [
                    t
                    for t in _HANGUL_TOKEN_RE.findall(query)
                    if t and t.lower() not in _QUERY_STOPWORDS
                ]
                tokens = sorted(set(tokens), key=len, reverse=True)[:5]
                if tokens:
                    base_input = BacktestInput()  # DB_* 환경변수 기반
                    loader = DataLoader(base_input)
                    with loader.pg_engine.connect() as conn:
                        for tok in tokens:
                            res = conn.execute(
                                text(
                                    """
                                    SELECT DISTINCT stock_code
                                    FROM score_table_dart_idc
                                    WHERE corp_name ILIKE :pattern
                                    ORDER BY stock_code
                                    LIMIT 5
                                    """
                                ),
                                {"pattern": f"%{tok}%"},
                            )
                            rows = [r[0] for r in res.fetchall() if r and r[0]]
                            if rows:
                                params["target_symbols"] = sorted(set(rows))
                                break
            except Exception:
                # 매핑 실패 시 무시하고 아래 안전장치에서 실패 처리
                pass

    # target_corp_names가 있으면 target_symbols로 변환 시도
    target_corp_names = params.get("target_corp_names")
    if target_corp_names and isinstance(target_corp_names, list) and not params.get("target_symbols"):
        try:
            base_input = BacktestInput()
            loader = DataLoader(base_input)
            resolved_symbols = []
            with loader.pg_engine.connect() as conn:
                for corp_name in target_corp_names:
                    if not isinstance(corp_name, str) or not corp_name.strip():
                        continue
                    res = conn.execute(
                        text(
                            """
                            SELECT DISTINCT stock_code
                            FROM score_table_dart_idc
                            WHERE corp_name = :corp_name
                            LIMIT 1
                            """
                        ),
                        {"corp_name": corp_name.strip()},
                    )
                    row = res.fetchone()
                    if row and row[0]:
                        resolved_symbols.append(row[0])
                        logger.info("Resolved corp_name=%s -> stock_code=%s", corp_name, row[0])
                    else:
                        logger.warning("Could not resolve corp_name=%s to stock_code", corp_name)
            if resolved_symbols:
                params["target_symbols"] = sorted(set(resolved_symbols))
        except Exception as e:
            logger.warning("Failed to resolve target_corp_names to target_symbols: %s", e)

    # 안전장치: 대상 종목이 없으면 전체 유니버스 백테스트가 되어 DB 부하가 큼
    allow_full = os.getenv("ALLOW_FULL_UNIVERSE", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    has_target_symbols = params.get("target_symbols") and len(params.get("target_symbols", [])) > 0
    has_target_corp_names = params.get("target_corp_names") and len(params.get("target_corp_names", [])) > 0
    
    if not allow_full and not has_target_symbols and not has_target_corp_names:
        raise ValueError(
            "백테스트 대상 종목을 찾을 수 없습니다. "
            "parameters.target_symbols(6자리 코드 리스트), parameters.target_corp_names(회사명 리스트), "
            "또는 stock_symbol을 제공해주세요."
        )
    
    # target_corp_names만 있고 target_symbols로 변환 실패한 경우
    if not has_target_symbols and has_target_corp_names:
        raise ValueError(
            f"회사명 {params.get('target_corp_names')}을(를) 종목코드로 변환할 수 없습니다. "
            "score_table_dart_idc 테이블에 해당 회사의 데이터가 없거나, 회사명이 정확하지 않을 수 있습니다."
        )

    # 기본값 보정(대상 종목이 있으면, 그 크기에 맞게 과도한 스크리닝을 피함)
    target_symbols = params.get("target_symbols")
    if isinstance(target_symbols, list) and target_symbols:
        # max_portfolio_size가 없으면 대상 종목 수로 제한
        params.setdefault("max_portfolio_size", max(1, len(target_symbols)))
        params.setdefault("max_positions", max(1, min(10, len(target_symbols))))

    # 결과는 파일로 저장하므로 trades/report도 생성되도록 기본 DART 모드 유지
    return BacktestInput(**params)


def _build_output_files(job_id: str, output: Any) -> Dict[str, str]:
    """BacktestOutput을 JSON/MD 파일로 저장하고 경로를 반환."""

    out_dir = Path(
        os.getenv("BACKTEST_RESULTS_DIR", "outputs/backtesting_results")
    ).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{job_id}.json"
    md_path = out_dir / f"{job_id}.md"

    # BacktestOutput(dataclass) -> dict
    out_dict: Dict[str, Any] = {
        "cumulative_return": getattr(output, "cumulative_return", 0.0),
        "total_return": getattr(output, "total_return", 0.0),
        "annualized_return": getattr(output, "annualized_return", 0.0),
        "mdd": getattr(output, "mdd", 0.0),
        "sharpe_ratio": getattr(output, "sharpe_ratio", 0.0),
        "win_rate": getattr(output, "win_rate", 0.0),
        "total_trades": getattr(output, "total_trades", 0),
        "total_profit": getattr(output, "total_profit", 0.0),
        "total_loss": getattr(output, "total_loss", 0.0),
        "trades": getattr(output, "trades", []) or [],
        "event_performance": getattr(output, "event_performance", {}) or {},
        "report": getattr(output, "report", "") or "",
    }

    json_path.write_text(json.dumps(out_dict, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(out_dict["report"], encoding="utf-8")

    return {"result_file_path": str(json_path), "report_file_path": str(md_path)}


def _build_output_summary(output: Any) -> Dict[str, Any]:
    """DB row에 저장할 가벼운 summary (컬럼 설계 기준)."""

    return {
        "total_return": _safe_float(getattr(output, "total_return", None)),
        "annualized_return": _safe_float(getattr(output, "annualized_return", None)),
        "mdd": _safe_float(getattr(output, "mdd", None)),
        "sharpe_ratio": _safe_float(getattr(output, "sharpe_ratio", None)),
        "win_rate": _safe_float(getattr(output, "win_rate", None)),
        "total_trades": int(getattr(output, "total_trades", 0) or 0),
        "total_profit": _safe_float(getattr(output, "total_profit", None)),
        "total_loss": _safe_float(getattr(output, "total_loss", None)),
    }


async def _process_one(job: Dict[str, Any]) -> None:
    job_id = str(job.get("job_id") or "")
    payload = job.get("input_json") or {}
    
    # asyncpg가 jsonb를 문자열로 반환할 수 있으므로 파싱
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception as e:
            logger.warning("Failed to parse input_json as JSON: %s", e)
            payload = {}
    if not isinstance(payload, dict):
        payload = {}
    
    logger.debug("Processing job %s with payload keys: %s", job_id, list(payload.keys()) if payload else [])

    t0 = time.time()
    try:
        input_params = _resolve_backtest_input(payload)
        output = await run_backtest(input_params)
        elapsed = time.time() - t0

        files = _build_output_files(job_id, output)
        summary = _build_output_summary(output)
        await mark_job_completed(
            job_id=job_id,
            output_json=summary,
            result_file_path=files.get("result_file_path"),
            report_file_path=files.get("report_file_path"),
            elapsed_seconds=elapsed,
        )

        # 완료 직후 LLM 해석 트리거(별도 서비스)
        asyncio.create_task(_trigger_llm_interpretation(job_id=job_id, user_id=int(job.get("user_id") or 0)))
    except Exception as e:
        elapsed = time.time() - t0
        await mark_job_failed(job_id=job_id, error_message=str(e), elapsed_seconds=elapsed)


async def main() -> None:
    poll_seconds = float(os.getenv("BACKTEST_WORKER_POLL_SECONDS", "5") or 5)

    while True:
        job = await claim_next_pending_job()
        if not job:
            await asyncio.sleep(poll_seconds)
            continue

        await _process_one(job)


if __name__ == "__main__":
    asyncio.run(main())

