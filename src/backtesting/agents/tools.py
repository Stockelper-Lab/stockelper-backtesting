from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents import (
    RunContextWrapper,
    ToolGuardrailFunctionOutput,
    function_tool,
    tool_input_guardrail,
    tool_output_guardrail,
)
from sqlalchemy import text

from backtesting.agents.schemas import (
    BacktestAgentContext,
    DartCoverage,
    PreflightReport,
    PriceCoverage,
)
from backtesting.portfolio_backtest import BacktestInput, DataLoader, run_backtest
from backtesting.web_db import mark_job_completed_with_analysis, mark_job_failed

_SECRET_RE = re.compile(r"\bsk-[A-Za-z0-9]{10,}\b")
_PG_DSN_RE = re.compile(r"\bpostgresql(\+[\w]+)?://[^\s]+\b", re.IGNORECASE)


def _safe_date_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (datetime,)):
        return v.date().isoformat()
    try:
        return str(v)
    except Exception:
        return None


def _normalize_symbol(x: Any) -> Optional[str]:
    if not isinstance(x, str):
        return None
    s = x.strip()
    if len(s) == 6 and s.isdigit():
        return s
    return None


@tool_input_guardrail
def _block_secrets_in_tool_args(data) -> ToolGuardrailFunctionOutput:
    """도구 호출 인자에 민감정보가 섞이면 거부합니다."""
    args = {}
    try:
        args = json.loads(data.context.tool_arguments or "{}")
    except Exception:
        args = {"raw": data.context.tool_arguments}

    blob = json.dumps(args, ensure_ascii=False, default=str)
    if _SECRET_RE.search(blob) or _PG_DSN_RE.search(blob):
        return ToolGuardrailFunctionOutput.reject_content(
            "도구 인자에 민감정보가 포함되어 호출을 차단했습니다."
        )
    return ToolGuardrailFunctionOutput.allow()


@tool_output_guardrail
def _block_secrets_in_tool_output(data) -> ToolGuardrailFunctionOutput:
    """도구 출력에 민감정보가 섞이면 거부합니다."""
    text_out = str(data.output or "")
    if _SECRET_RE.search(text_out) or _PG_DSN_RE.search(text_out):
        return ToolGuardrailFunctionOutput.reject_content(
            "도구 출력에 민감정보가 포함되어 차단했습니다."
        )
    return ToolGuardrailFunctionOutput.allow()


def _output_to_dict(output: Any) -> Dict[str, Any]:
    return {
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


def _build_output_summary(output_dict: Dict[str, Any]) -> Dict[str, Any]:
    def _f(x: Any) -> Optional[float]:
        try:
            return None if x is None else float(x)
        except Exception:
            return None

    return {
        "total_return": _f(output_dict.get("total_return")),
        "annualized_return": _f(output_dict.get("annualized_return")),
        "mdd": _f(output_dict.get("mdd")),
        "sharpe_ratio": _f(output_dict.get("sharpe_ratio")),
        "win_rate": _f(output_dict.get("win_rate")),
        "total_trades": int(output_dict.get("total_trades") or 0),
        "total_profit": _f(output_dict.get("total_profit")),
        "total_loss": _f(output_dict.get("total_loss")),
    }


def _write_artifacts(*, job_id: str, output_dict: Dict[str, Any]) -> Dict[str, str]:
    out_dir = Path(os.getenv("BACKTEST_RESULTS_DIR", "outputs/backtesting_results")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{job_id}.json"
    md_path = out_dir / f"{job_id}.md"

    json_path.write_text(
        json.dumps(output_dict, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    md_path.write_text(str(output_dict.get("report") or ""), encoding="utf-8")

    return {"result_file_path": str(json_path), "report_file_path": str(md_path)}


def resolve_symbols_impl(
    *,
    corp_names: Optional[List[str]] = None,
    query: Optional[str] = None,
) -> Dict[str, Any]:
    """회사명/자연어에서 종목코드를 조회해 반환합니다(score_table_dart_idc 기반)."""

    base_input = BacktestInput()  # DB_* env 기반
    loader = DataLoader(base_input)

    resolved: List[str] = []
    mapping_log: Dict[str, Any] = {"by_corp_name": {}, "by_query_tokens": {}}

    # 1) corp_names exact match 우선
    if corp_names:
        for name in corp_names:
            if not isinstance(name, str) or not name.strip():
                continue
            corp_name = name.strip()
            with loader.pg_engine.connect() as conn:
                res = conn.execute(
                    text(
                        """
                        SELECT DISTINCT stock_code
                        FROM score_table_dart_idc
                        WHERE corp_name = :corp_name
                        ORDER BY stock_code
                        LIMIT 5
                        """
                    ),
                    {"corp_name": corp_name},
                )
                rows = [r[0] for r in res.fetchall() if r and r[0]]
                mapping_log["by_corp_name"][corp_name] = rows
                resolved.extend([s for s in rows if _normalize_symbol(s)])

    # 2) query에서 6자리 종목코드 직접 추출
    if query and isinstance(query, str):
        found = re.findall(r"\b\d{6}\b", query)
        resolved.extend([s for s in found if _normalize_symbol(s)])

        # 3) query 토큰으로 ILIKE 검색(최대 5개 토큰)
        tokens = re.findall(r"[가-힣A-Za-z]{2,}", query)
        stop = {
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
        tokens = [t for t in tokens if t and t.lower() not in stop]
        tokens = sorted(set(tokens), key=len, reverse=True)[:5]
        if tokens:
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
                    mapping_log["by_query_tokens"][tok] = rows
                    resolved.extend([s for s in rows if _normalize_symbol(s)])

    unique = sorted(set([s for s in resolved if _normalize_symbol(s)]))
    return {"symbols": unique, "mapping_log": mapping_log}


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
def resolve_symbols(
    ctx: RunContextWrapper[BacktestAgentContext],
    corp_names: Optional[List[str]] = None,
    query: Optional[str] = None,
) -> Dict[str, Any]:
    return resolve_symbols_impl(corp_names=corp_names, query=query)


def preflight_data_check_impl(*, backtest_params: Dict[str, Any]) -> Dict[str, Any]:
    """백테스트 실행 전, 가격/공시 데이터 커버리지를 빠르게 점검합니다."""

    # BacktestInput 검증(여기서 터지면 상위에서 처리)
    bt = BacktestInput(**backtest_params)

    symbols = bt.target_symbols or []
    if not symbols:
        report = PreflightReport(
            ok=False,
            warnings=["대상 종목이 없어 사전 점검을 진행할 수 없습니다."],
        )
        return report.model_dump()

    loader = DataLoader(bt)
    start_date = bt.start_date
    end_date = bt.end_date

    price: Dict[str, PriceCoverage] = {}
    dart: Dict[str, DartCoverage] = {}
    missing_price: List[str] = []
    warnings: List[str] = []

    min_dates: List[str] = []
    max_dates: List[str] = []

    with loader.pg_engine.connect() as conn:
        for sym in symbols:
            sym_n = _normalize_symbol(sym) or str(sym)

            q_price = text(
                """
                SELECT COUNT(*) AS cnt, MIN(date) AS min_date, MAX(date) AS max_date
                FROM daily_stock_price
                WHERE symbol = :symbol
                  AND date >= :start_date
                  AND date <= :end_date
                """
            )
            r = conn.execute(
                q_price, {"symbol": sym_n, "start_date": start_date, "end_date": end_date}
            ).fetchone()
            cnt = int(r[0] or 0) if r else 0
            mn = _safe_date_str(r[1]) if r else None
            mx = _safe_date_str(r[2]) if r else None

            price[sym_n] = PriceCoverage(symbol=sym_n, rows=cnt, min_date=mn, max_date=mx)
            if cnt <= 0:
                missing_price.append(sym_n)
            else:
                if mn:
                    min_dates.append(mn)
                if mx:
                    max_dates.append(mx)

            if bt.use_dart_disclosure:
                q_dart = text(
                    """
                    SELECT COUNT(*) AS cnt, MIN(rcept_dt) AS min_date, MAX(rcept_dt) AS max_date
                    FROM score_table_dart_idc
                    WHERE stock_code = :symbol
                      AND rcept_dt >= :start_date
                      AND rcept_dt <= :end_date
                    """
                )
                rr = conn.execute(
                    q_dart,
                    {
                        "symbol": sym_n,
                        "start_date": datetime.strptime(start_date, "%Y-%m-%d").date(),
                        "end_date": datetime.strptime(end_date, "%Y-%m-%d").date(),
                    },
                ).fetchone()
                dcnt = int(rr[0] or 0) if rr else 0
                dmn = _safe_date_str(rr[1]) if rr else None
                dmx = _safe_date_str(rr[2]) if rr else None
                dart[sym_n] = DartCoverage(symbol=sym_n, rows=dcnt, min_date=dmn, max_date=dmx)

    suggested_start: Optional[str] = None
    suggested_end: Optional[str] = None

    # 가격 데이터가 있는 종목들에 대해 공통 구간(intersection) 제안
    if min_dates and max_dates:
        try:
            suggested_start = max(min_dates)
            suggested_end = min(max_dates)
            if suggested_start and suggested_end and suggested_start > suggested_end:
                warnings.append("종목 간 공통 데이터 구간이 없어 기간 보정이 필요할 수 있습니다.")
        except Exception:
            pass

    if missing_price:
        warnings.append(f"가격 데이터가 없는 종목이 있습니다: {missing_price}")

    report = PreflightReport(
        ok=(len(missing_price) < len(symbols)),
        warnings=warnings,
        suggested_start_date=suggested_start,
        suggested_end_date=suggested_end,
        missing_price_symbols=missing_price,
        price=price,
        dart=dart,
    )
    return report.model_dump()


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
def preflight_data_check(
    ctx: RunContextWrapper[BacktestAgentContext],
    backtest_params: Dict[str, Any],
) -> Dict[str, Any]:
    return preflight_data_check_impl(backtest_params=backtest_params)


async def run_backtest_impl(*, backtest_params: Dict[str, Any]) -> Dict[str, Any]:
    bt = BacktestInput(**backtest_params)
    output = await run_backtest(bt)
    return _output_to_dict(output)


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
async def run_backtest_tool(
    ctx: RunContextWrapper[BacktestAgentContext],
    backtest_params: Dict[str, Any],
) -> Dict[str, Any]:
    return await run_backtest_impl(backtest_params=backtest_params)


def build_artifacts_impl(*, job_id: str, output_dict: Dict[str, Any]) -> Dict[str, Any]:
    files = _write_artifacts(job_id=job_id, output_dict=output_dict)
    summary = _build_output_summary(output_dict)
    return {"files": files, "summary": summary}


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
def build_artifacts_tool(
    ctx: RunContextWrapper[BacktestAgentContext],
    job_id: str,
    output_dict: Dict[str, Any],
) -> Dict[str, Any]:
    return build_artifacts_impl(job_id=job_id, output_dict=output_dict)


async def persist_completed_impl(
    *,
    job_id: str,
    output_json: Dict[str, Any],
    result_file_path: Optional[str],
    report_file_path: Optional[str],
    elapsed_seconds: Optional[float],
    analysis_md: Optional[str],
    analysis_json: Optional[Dict[str, Any]],
    analysis_model: Optional[str],
    analysis_prompt_version: Optional[str],
    analysis_elapsed_seconds: Optional[float],
) -> bool:
    return await mark_job_completed_with_analysis(
        job_id=job_id,
        output_json=output_json,
        result_file_path=result_file_path,
        report_file_path=report_file_path,
        elapsed_seconds=elapsed_seconds,
        analysis_md=analysis_md,
        analysis_json=analysis_json,
        analysis_model=analysis_model,
        analysis_prompt_version=analysis_prompt_version,
        analysis_elapsed_seconds=analysis_elapsed_seconds,
    )


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
async def persist_completed_tool(
    ctx: RunContextWrapper[BacktestAgentContext],
    *,
    job_id: str,
    output_json: Dict[str, Any],
    result_file_path: Optional[str],
    report_file_path: Optional[str],
    elapsed_seconds: Optional[float],
    analysis_md: Optional[str],
    analysis_json: Optional[Dict[str, Any]],
    analysis_model: Optional[str],
    analysis_prompt_version: Optional[str],
    analysis_elapsed_seconds: Optional[float],
) -> bool:
    return await persist_completed_impl(
        job_id=job_id,
        output_json=output_json,
        result_file_path=result_file_path,
        report_file_path=report_file_path,
        elapsed_seconds=elapsed_seconds,
        analysis_md=analysis_md,
        analysis_json=analysis_json,
        analysis_model=analysis_model,
        analysis_prompt_version=analysis_prompt_version,
        analysis_elapsed_seconds=analysis_elapsed_seconds,
    )


async def persist_failed_impl(
    *, job_id: str, error_message: str, elapsed_seconds: Optional[float]
) -> bool:
    return await mark_job_failed(
        job_id=job_id, error_message=str(error_message), elapsed_seconds=elapsed_seconds
    )


@function_tool(
    tool_input_guardrails=[_block_secrets_in_tool_args],
    tool_output_guardrails=[_block_secrets_in_tool_output],
)
async def persist_failed_tool(
    ctx: RunContextWrapper[BacktestAgentContext],
    *,
    job_id: str,
    error_message: str,
    elapsed_seconds: Optional[float],
) -> bool:
    return await persist_failed_impl(
        job_id=job_id, error_message=error_message, elapsed_seconds=elapsed_seconds
    )

