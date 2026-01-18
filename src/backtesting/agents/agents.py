from __future__ import annotations

import os

from agents import Agent

from backtesting.agents.guardrails import secrets_input_guardrail, secrets_output_guardrail
from backtesting.agents.schemas import AuditResult, FinalNarrative, ParsedRequest, AdjustmentPlan


def _env(key: str, default: str) -> str:
    v = (os.getenv(key) or "").strip()
    return v or default


PARSE_MODEL = _env("BACKTEST_AGENT_MODEL_PARSE", "")
ADJUST_MODEL = _env("BACKTEST_AGENT_MODEL_ADJUST", "")
AUDIT_MODEL = _env("BACKTEST_AGENT_MODEL_AUDIT", "")
REPORT_MODEL = _env("BACKTEST_AGENT_MODEL_REPORT", "")


parse_request_agent = Agent(
    name="backtest_parse_request_agent",
    # model 지정이 비어있으면 SDK 기본 모델 사용(gpt-4.1)
    model=PARSE_MODEL or None,
    instructions=(
        "너는 주식 포트폴리오 백테스팅 요청을 구조화하는 파서다.\n"
        "입력으로 JSON payload가 주어진다. payload에는 user_id, stock_symbol/stock_ticker, "
        "strategy_type, query, parameters 등이 올 수 있다.\n\n"
        "목표: payload에서 백테스트 실행에 필요한 정보를 추출해 `ParsedRequest` 스키마로 반환한다.\n\n"
        "규칙:\n"
        "- target_symbols는 6자리 숫자 문자열 배열로만 반환한다(확실할 때만).\n"
        "- 날짜는 YYYY-MM-DD 형식으로만 반환한다.\n"
        "- sort_by는 (momentum|market_cap|event_type|disclosure|sentiment_score|fundamental) 중 하나.\n"
        "- rebalancing_period는 (daily|weekly|monthly|quarterly) 중 하나.\n"
        "- 불확실하면 null(또는 생략)로 둔다.\n"
        "- parameters가 이미 있다면, 그 값을 존중하되, 명백히 충돌/오류가 있으면 notes에 언급한다.\n"
    ),
    output_type=ParsedRequest,
    input_guardrails=[secrets_input_guardrail],
)


adjust_plan_agent = Agent(
    name="backtest_adjust_plan_agent",
    model=ADJUST_MODEL or None,
    instructions=(
        "너는 백테스트 실행 전/후 문제가 발견됐을 때, 재시도를 위한 수정안을 제안하는 에이전트다.\n"
        "입력에는 현재 backtest_params(BacktestInput dict), preflight_report, audit_feedback가 포함된다.\n\n"
        "목표: 수정안을 `AdjustmentPlan` 스키마로 반환한다.\n\n"
        "가능한 조치 예:\n"
        "- 가격 데이터가 없는 종목은 drop_symbols에 추가\n"
        "- 종목 간 공통 데이터 구간이 있다면 new_start_date/new_end_date로 기간 축소\n"
        "- 지표 조건이 너무 엄격해 거래 0건이면 disable_event_indicator_conditions=true 고려\n"
        "- 단, 전체 유니버스 실행을 유도하지 말고, 안전한 범위 내에서만 조정한다.\n"
    ),
    output_type=AdjustmentPlan,
    input_guardrails=[secrets_input_guardrail],
)


audit_result_agent = Agent(
    name="backtest_audit_agent",
    model=AUDIT_MODEL or None,
    instructions=(
        "너는 백테스트 결과를 감사(audit)하는 에이전트다.\n"
        "입력에는 output_summary(수익률/거래수 등), trades(일부), preflight_report, backtest_params가 포함된다.\n\n"
        "목표: 결과가 합리적인지 판단하고 `AuditResult`로 반환한다.\n\n"
        "권장 판정 기준(예시):\n"
        "- total_trades==0인데 abs(total_return)가 매우 크면 needs_retry=true\n"
        "- mdd가 0~100 범위를 벗어나면 fail 또는 needs_retry\n"
        "- 데이터 부족 경고가 많으면 needs_improvement로 피드백 제공\n"
        "- 개선 여지가 있으면 needs_improvement/needs_retry, 치명적이면 fail\n"
    ),
    output_type=AuditResult,
    input_guardrails=[secrets_input_guardrail],
)


report_agent = Agent(
    name="backtest_report_agent",
    model=REPORT_MODEL or None,
    instructions=(
        "너는 백테스트 결과를 사용자에게 설명하는 리포트 작성 에이전트다.\n"
        "입력에는 backtest_params, output_summary, trades, preflight_report, audit_result가 포함된다.\n\n"
        "목표:\n"
        "- analysis_md: 사용자에게 전달 가능한 마크다운 리포트\n"
        "- analysis_json: 핵심 포인트를 구조화한 요약(dict)\n\n"
        "주의:\n"
        "- 민감정보(키/DSN/내부 IP 등)를 절대 포함하지 말 것\n"
        "- 데이터 부족/전략 한계/재현 방법을 명확히 포함\n"
    ),
    output_type=FinalNarrative,
    input_guardrails=[secrets_input_guardrail],
    output_guardrails=[secrets_output_guardrail],
)

