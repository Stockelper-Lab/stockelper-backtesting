from agents.strict_schema import ensure_strict_json_schema


def test_agents_output_types_are_strict_schema_compatible():
    """
    OpenAI Agents SDK는 structured output에서 strict schema를 사용합니다.
    output_type에 Dict[str, Any] 같은 자유형 object가 들어가면 런타임에서 즉시 실패하므로,
    CI 단계에서 미리 감지합니다.
    """

    from backtesting.agents.schemas import (
        AdjustmentPlan,
        AuditResult,
        FinalNarrative,
        ParsedRequest,
    )

    for cls in (ParsedRequest, AdjustmentPlan, AuditResult, FinalNarrative):
        ensure_strict_json_schema(cls.model_json_schema())

