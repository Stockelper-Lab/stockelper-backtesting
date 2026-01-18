from __future__ import annotations

import json
import re
from typing import Any, List

from agents import (
    Agent,
    GuardrailFunctionOutput,
    RunContextWrapper,
    TResponseInputItem,
    input_guardrail,
    output_guardrail,
)

from backtesting.agents.schemas import BacktestAgentContext, FinalNarrative

_SECRET_RE = re.compile(r"\bsk-[A-Za-z0-9]{10,}\b")
_PG_DSN_RE = re.compile(r"\bpostgresql(\+[\w]+)?://[^\s]+\b", re.IGNORECASE)


def _stringify_input(input_data: str | List[TResponseInputItem]) -> str:
    if isinstance(input_data, str):
        return input_data
    try:
        return json.dumps(input_data, ensure_ascii=False, default=str)
    except Exception:
        return str(input_data)


@input_guardrail
async def secrets_input_guardrail(
    ctx: RunContextWrapper[BacktestAgentContext],
    agent: Agent,
    input: str | List[TResponseInputItem],
) -> GuardrailFunctionOutput:
    """민감정보(sk- 토큰/DB DSN 등)가 LLM 입력으로 넘어가는 것을 차단."""

    text = _stringify_input(input)
    has_secret = bool(_SECRET_RE.search(text))
    has_dsn = bool(_PG_DSN_RE.search(text))

    return GuardrailFunctionOutput(
        output_info={"has_secret": has_secret, "has_dsn": has_dsn},
        tripwire_triggered=has_secret or has_dsn,
    )


@output_guardrail
async def secrets_output_guardrail(
    ctx: RunContextWrapper[BacktestAgentContext],
    agent: Agent,
    output: FinalNarrative,
) -> GuardrailFunctionOutput:
    """민감정보가 리포트에 포함되는 것을 차단."""

    md = output.analysis_md or ""
    has_secret = bool(_SECRET_RE.search(md))
    has_dsn = bool(_PG_DSN_RE.search(md))

    return GuardrailFunctionOutput(
        output_info={"has_secret": has_secret, "has_dsn": has_dsn},
        tripwire_triggered=has_secret or has_dsn,
    )

