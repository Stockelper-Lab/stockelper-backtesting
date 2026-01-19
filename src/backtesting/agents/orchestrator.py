from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime
from typing import Any, Dict, Optional

from agents import Runner, trace

from backtesting.agents.agents import (
    adjust_plan_agent,
    audit_result_agent,
    parse_request_agent,
    report_agent,
)
from backtesting.agents.schemas import (
    AdjustmentPlan,
    AuditResult,
    BacktestAgentContext,
    FinalNarrative,
    ParsedRequest,
    PreflightReport,
)
from backtesting.agents.tools import (
    persist_completed_impl,
    persist_failed_impl,
    preflight_data_check_impl,
    resolve_symbols_impl,
    run_backtest_impl,
)


def _to_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str, indent=2)


def _extract_parameters(payload: Dict[str, Any]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    raw_params = payload.get("parameters")
    if isinstance(raw_params, str):
        try:
            raw_params = json.loads(raw_params)
        except Exception:
            raw_params = {}
    if isinstance(raw_params, dict):
        params.update(raw_params)
    return params


def _merge_backtest_params(
    *,
    payload: Dict[str, Any],
    parsed: ParsedRequest | None,
) -> Dict[str, Any]:
    """payload(parameters/legacy) + parsed 결과를 병합하여 BacktestInput dict를 구성."""

    params = _extract_parameters(payload)

    # Legacy mapping
    stock_symbol = payload.get("stock_symbol") or payload.get("stock_ticker")
    if isinstance(stock_symbol, str) and stock_symbol.strip():
        params.setdefault("target_symbols", [stock_symbol.strip()])

    strategy_type = payload.get("strategy_type")
    if isinstance(strategy_type, str) and strategy_type.strip():
        params.setdefault("sort_by", strategy_type.strip())

    # ParsedRequest 반영(단, parameters에 이미 있으면 덮지 않음)
    if parsed is not None:
        if parsed.target_symbols:
            params.setdefault("target_symbols", parsed.target_symbols)
        if parsed.target_corp_names:
            params.setdefault("target_corp_names", parsed.target_corp_names)
        if parsed.start_date:
            params.setdefault("start_date", parsed.start_date)
        if parsed.end_date:
            params.setdefault("end_date", parsed.end_date)
        if parsed.sort_by:
            params.setdefault("sort_by", parsed.sort_by)
        if parsed.rebalancing_period:
            params.setdefault("rebalancing_period", parsed.rebalancing_period)
        if parsed.max_positions is not None:
            params.setdefault("max_positions", parsed.max_positions)
        if parsed.max_portfolio_size is not None:
            params.setdefault("max_portfolio_size", parsed.max_portfolio_size)
        if parsed.initial_cash is not None:
            params.setdefault("initial_cash", parsed.initial_cash)
        if parsed.use_dart_disclosure is not None:
            params.setdefault("use_dart_disclosure", parsed.use_dart_disclosure)

    return params


def _apply_param_guardrails(params: Dict[str, Any]) -> Dict[str, Any]:
    """운영 안전장치(결정적) 적용."""

    allow_full = _to_bool(os.getenv("ALLOW_FULL_UNIVERSE"), default=False)

    target_symbols = params.get("target_symbols")
    if isinstance(target_symbols, str):
        target_symbols = [target_symbols]
    if target_symbols is None:
        target_symbols = []
    if not isinstance(target_symbols, list):
        target_symbols = []

    target_symbols = [s.strip() for s in target_symbols if isinstance(s, str) and s.strip()]
    target_symbols = sorted(set([s for s in target_symbols if len(s) == 6 and s.isdigit()]))
    if target_symbols:
        params["target_symbols"] = target_symbols

    if not allow_full and not target_symbols:
        # target_corp_names는 resolve_symbols 단계에서 변환을 시도하므로 여기서는 즉시 fail하지 않고,
        # 최종적으로도 비어 있으면 실패 처리.
        pass

    # 날짜 포맷 검증(LLM 출력이 비정형이면 제거 후, query 기반 보정에 맡김)
    def _valid_date(s: Any) -> bool:
        if not isinstance(s, str) or not s.strip():
            return False
        try:
            datetime.strptime(s.strip(), "%Y-%m-%d")
            return True
        except Exception:
            return False

    sd = params.get("start_date")
    ed = params.get("end_date")
    if sd is not None and not _valid_date(sd):
        params.pop("start_date", None)
    if ed is not None and not _valid_date(ed):
        params.pop("end_date", None)
    sd2 = params.get("start_date")
    ed2 = params.get("end_date")
    if _valid_date(sd2) and _valid_date(ed2):
        try:
            if str(sd2) > str(ed2):
                # YYYY-MM-DD 문자열은 사전식 비교가 날짜 비교와 동일
                params["start_date"], params["end_date"] = str(ed2), str(sd2)
        except Exception:
            pass

    # 상한/기본값
    max_positions = params.get("max_positions")
    try:
        if max_positions is not None:
            mp = int(max_positions)
            params["max_positions"] = max(1, min(20, mp))
    except Exception:
        params.pop("max_positions", None)

    max_portfolio_size = params.get("max_portfolio_size")
    try:
        if max_portfolio_size is not None:
            mps = int(max_portfolio_size)
            params["max_portfolio_size"] = max(1, min(50, mps))
    except Exception:
        params.pop("max_portfolio_size", None)

    # use_dart_disclosure 기본 true 유지(엔진 기본값에 위임)
    return params


def _extract_corp_names_from_query(query: Optional[str]) -> list[str]:
    """자연어 요청에서 회사명 후보를 결정적으로 추출(LLM 보조 실패 대비)."""
    if not query or not isinstance(query, str):
        return []

    parts = [p.strip() for p in query.split(",") if p and p.strip()]
    if not parts:
        return []

    # 마지막 조각에 붙는 기간/요청 문구 제거
    last = parts[-1]
    last = re.split(r"\b\d+\s*년", last, maxsplit=1)[0].strip()
    last = re.split(r"\b백테", last, maxsplit=1)[0].strip()
    if last:
        parts[-1] = last

    cleaned: list[str] = []
    for p in parts:
        s = p.strip()
        if not s:
            continue
        # (주), ㈜ 변형 제거 (DB corp_name 매칭률 향상)
        s = s.replace("㈜", "").replace("(주)", "").replace("주)", "").replace("(주", "")
        s = s.strip()
        if len(s) >= 2:
            cleaned.append(s)
    # unique preserve order
    seen = set()
    out: list[str] = []
    for s in cleaned:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out[:20]


def _maybe_apply_years_range_from_query(params: Dict[str, Any], query: Optional[str]) -> Dict[str, Any]:
    """query에 'N년치'가 있고 start/end가 비어있으면 결정적으로 채웁니다."""
    if not query or not isinstance(query, str):
        return params
    if params.get("start_date") and params.get("end_date"):
        return params

    m = re.search(r"(\d+)\s*년", query)
    if not m:
        return params

    try:
        years = int(m.group(1))
    except Exception:
        return params
    if years <= 0 or years > 30:
        return params

    today = date.today()
    end_s = today.isoformat()
    start_year = today.year - years
    # 윤년 등 안전 처리
    try:
        start_s = today.replace(year=start_year).isoformat()
    except Exception:
        start_s = date(start_year, today.month, 1).isoformat()

    params.setdefault("end_date", end_s)
    params.setdefault("start_date", start_s)
    return params


def _apply_adjustment(params: Dict[str, Any], adj: AdjustmentPlan) -> Dict[str, Any]:
    if adj.new_start_date:
        params["start_date"] = adj.new_start_date
    if adj.new_end_date:
        params["end_date"] = adj.new_end_date

    # 종목 제외
    if adj.drop_symbols:
        cur = params.get("target_symbols") or []
        if isinstance(cur, str):
            cur = [cur]
        if not isinstance(cur, list):
            cur = []
        drop = {s for s in adj.drop_symbols if isinstance(s, str)}
        cur2 = [s for s in cur if isinstance(s, str) and s not in drop]
        params["target_symbols"] = sorted(set(cur2))

    if adj.disable_event_indicator_conditions is True:
        params["event_indicator_conditions"] = []

    if adj.set_use_dart_disclosure is not None:
        params["use_dart_disclosure"] = bool(adj.set_use_dart_disclosure)

    return params


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


async def process_job(job: Dict[str, Any]) -> None:
    """claim된 job(dict)을 Agents SDK 기반 파이프라인으로 처리합니다."""

    job_id = str(job.get("job_id") or "")
    user_id = int(job.get("user_id") or 0)
    payload = job.get("input_json") or {}

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}

    max_retries = int(os.getenv("BACKTEST_AGENT_MAX_RETRIES", "2") or 2)
    prompt_version = os.getenv("BACKTEST_AGENT_PROMPT_VERSION", "v1") or "v1"

    ctx = BacktestAgentContext(job_id=job_id, user_id=user_id, prompt_version=prompt_version)

    t0 = time.time()

    try:
        if not (os.getenv("OPENAI_API_KEY") or "").strip():
            raise RuntimeError("OPENAI_API_KEY가 설정되어 있지 않습니다. (Agents-only worker)")

        with trace(
            "Stockelper Backtesting Agent",
            group_id=job_id,
            metadata={"job_id": job_id, "user_id": user_id},
        ):
            # 1) Parse(필요 시)
            parsed: ParsedRequest | None = None
            query = payload.get("query") if isinstance(payload.get("query"), str) else None

            need_parse = bool(query and query.strip())
            if need_parse:
                parse_input = {"payload": payload}
                parse_result = await Runner.run(
                    parse_request_agent, _json_dumps(parse_input), context=ctx
                )
                parsed = parse_result.final_output_as(ParsedRequest)

            # 2) BacktestInput dict 구성
            backtest_params = _merge_backtest_params(payload=payload, parsed=parsed)
            backtest_params = _apply_param_guardrails(backtest_params)
            backtest_params = _maybe_apply_years_range_from_query(backtest_params, query)

            # 3) 종목 해석/보강(회사명/쿼리 -> 종목코드)
            allow_full = _to_bool(os.getenv("ALLOW_FULL_UNIVERSE"), default=False)

            target_symbols = backtest_params.get("target_symbols") or []
            target_corp_names = backtest_params.get("target_corp_names") or []
            if isinstance(target_corp_names, str):
                target_corp_names = [target_corp_names]
            if not isinstance(target_corp_names, list):
                target_corp_names = []

            # LLM 파서가 corp_names를 못 뽑아도, query 기반으로 후보를 보강
            if not target_corp_names and query:
                target_corp_names = _extract_corp_names_from_query(query)

            if (not target_symbols) and (target_corp_names or query):
                resolved = resolve_symbols_impl(corp_names=target_corp_names, query=query)
                resolved_symbols = resolved.get("symbols") or []
                if resolved_symbols:
                    backtest_params["target_symbols"] = resolved_symbols
                    target_symbols = resolved_symbols

            if not allow_full and not target_symbols:
                raise ValueError(
                    "백테스트 대상 종목을 찾을 수 없습니다. "
                    "parameters.target_symbols(6자리 코드 리스트), parameters.target_corp_names(회사명 리스트), "
                    "또는 query/stock_symbol을 제공해주세요."
                )

            # 4) 실행 루프(Preflight -> Run -> Audit)
            retry_count = 0
            last_preflight: PreflightReport | None = None
            last_audit: AuditResult | None = None
            output_dict: Dict[str, Any] | None = None
            output_summary: Dict[str, Any] | None = None

            while True:
                # 4-1) Preflight
                preflight_dict = preflight_data_check_impl(backtest_params=backtest_params)
                last_preflight = PreflightReport.model_validate(preflight_dict)

                if not last_preflight.ok:
                    if retry_count >= max_retries:
                        raise ValueError(
                            f"사전 점검 실패: {last_preflight.warnings} (retries={retry_count})"
                        )
                    adjust_input = {
                        "stage": "preflight",
                        "backtest_params": backtest_params,
                        "preflight_report": last_preflight.model_dump(),
                        "audit_feedback": None,
                    }
                    adj_run = await Runner.run(
                        adjust_plan_agent, _json_dumps(adjust_input), context=ctx
                    )
                    adj = adj_run.final_output_as(AdjustmentPlan)
                    backtest_params = _apply_adjustment(backtest_params, adj)
                    backtest_params = _apply_param_guardrails(backtest_params)
                    retry_count += 1
                    continue

                # 4-2) Run backtest
                output_dict = await run_backtest_impl(backtest_params=backtest_params)
                output_summary = _build_output_summary(output_dict)

                # 4-4) Audit
                audit_input = {
                    "backtest_params": backtest_params,
                    "preflight_report": last_preflight.model_dump(),
                    "output_summary": output_summary,
                    "trades_head": (output_dict.get("trades") or [])[:50],
                }
                audit_run = await Runner.run(
                    audit_result_agent, _json_dumps(audit_input), context=ctx
                )
                last_audit = audit_run.final_output_as(AuditResult)

                if last_audit.score == "fail":
                    raise ValueError(f"Audit fail: {last_audit.feedback}")

                if last_audit.needs_retry:
                    if retry_count >= max_retries:
                        # 재시도 여력이 없으면 실패 처리(운영 비용/지연 통제)
                        raise ValueError(
                            f"Audit requested retry but retry budget exhausted: {last_audit.feedback}"
                        )
                    adjust_input = {
                        "stage": "audit",
                        "backtest_params": backtest_params,
                        "preflight_report": last_preflight.model_dump(),
                        "audit_feedback": last_audit.model_dump(),
                    }
                    adj_run = await Runner.run(
                        adjust_plan_agent, _json_dumps(adjust_input), context=ctx
                    )
                    adj = adj_run.final_output_as(AdjustmentPlan)
                    backtest_params = _apply_adjustment(backtest_params, adj)
                    backtest_params = _apply_param_guardrails(backtest_params)
                    retry_count += 1
                    continue

                break

            assert output_dict is not None
            assert output_summary is not None
            assert last_preflight is not None
            assert last_audit is not None

            # 5) 최종 artifact 작성(통과 후에만 저장하여 실패 시 찌꺼기 최소화)
            from backtesting.agents.tools import build_artifacts_impl

            artifacts = build_artifacts_impl(job_id=job_id, output_dict=output_dict)
            files = artifacts.get("files") or {}

            # 5) Report(해석)
            report_t0 = time.time()
            report_input = {
                "backtest_params": backtest_params,
                "preflight_report": last_preflight.model_dump(),
                "audit_result": last_audit.model_dump(),
                "output_summary": output_summary,
                "trades_head": (output_dict.get("trades") or [])[:200],
            }
            analysis_md: Optional[str] = None
            analysis_json_dict: Optional[Dict[str, Any]] = None
            report_elapsed: Optional[float] = None
            try:
                report_run = await Runner.run(report_agent, _json_dumps(report_input), context=ctx)
                narrative = report_run.final_output_as(FinalNarrative)
                report_elapsed = time.time() - report_t0

                analysis_md = narrative.analysis_md
                analysis_json_dict2: Dict[str, Any] = {}
                for item in getattr(narrative, "analysis_json", []) or []:
                    k = getattr(item, "key", None)
                    v = getattr(item, "value", None)
                    if isinstance(k, str) and k.strip():
                        analysis_json_dict2[k.strip()] = v
                analysis_json_dict = analysis_json_dict2
            except Exception:
                # 리포트 생성은 best-effort: 실패해도 백테스트 결과는 completed로 적재합니다.
                analysis_md = None
                analysis_json_dict = None
                report_elapsed = None

            # 6) Persist
            elapsed = time.time() - t0
            await persist_completed_impl(
                job_id=job_id,
                output_json=output_summary,
                result_file_path=files.get("result_file_path"),
                report_file_path=files.get("report_file_path"),
                elapsed_seconds=elapsed,
                analysis_md=analysis_md,
                analysis_json=analysis_json_dict,
                analysis_model=os.getenv("BACKTEST_AGENT_MODEL_REPORT") or None,
                analysis_prompt_version=prompt_version,
                analysis_elapsed_seconds=report_elapsed,
            )

    except Exception as e:
        elapsed = time.time() - t0
        await persist_failed_impl(job_id=job_id, error_message=str(e), elapsed_seconds=elapsed)

