from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class BacktestAgentContext(BaseModel):
    """Agents SDK 실행 컨텍스트(런타임 메타데이터)."""

    job_id: str = Field(description="public.backtesting.job_id")
    user_id: int = Field(description="stockelper_web.users.id")
    request_source: str = Field(default="llm", description="요청 출처")
    prompt_version: str = Field(default="v1", description="에이전트 프롬프트 버전")


class ParsedRequest(BaseModel):
    """자연어/레거시 입력을 해석한 결과(LLM structured output)."""

    target_symbols: Optional[List[str]] = Field(
        default=None, description="6자리 종목코드 리스트(문자열)"
    )
    target_corp_names: Optional[List[str]] = Field(
        default=None, description="회사명 리스트(종목코드로 매핑 전)"
    )

    start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")

    sort_by: Optional[
        Literal[
            "momentum",
            "market_cap",
            "event_type",
            "disclosure",
            "sentiment_score",
            "fundamental",
        ]
    ] = Field(default=None, description="스크리닝/정렬 기준")
    rebalancing_period: Optional[Literal["daily", "weekly", "monthly", "quarterly"]] = (
        Field(default=None, description="리밸런싱 주기")
    )

    max_positions: Optional[int] = Field(default=None, description="최대 보유 종목 수")
    max_portfolio_size: Optional[int] = Field(default=None, description="대상 종목 수 상한")
    initial_cash: Optional[float] = Field(default=None, description="초기 자본(원)")

    use_dart_disclosure: Optional[bool] = Field(default=None, description="DART 공시 사용 여부")

    notes: Optional[str] = Field(default=None, description="해석 메모/불확실성")


class PriceCoverage(BaseModel):
    symbol: str
    rows: int = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None


class DartCoverage(BaseModel):
    symbol: str
    rows: int = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None


class PreflightReport(BaseModel):
    ok: bool = Field(description="실행 가능 여부(가격 데이터 기준)")
    warnings: List[str] = Field(default_factory=list)

    # 기간 보정 제안(필요 시)
    suggested_start_date: Optional[str] = None
    suggested_end_date: Optional[str] = None

    missing_price_symbols: List[str] = Field(default_factory=list)
    price: Dict[str, PriceCoverage] = Field(default_factory=dict)

    dart: Dict[str, DartCoverage] = Field(default_factory=dict)


class AdjustmentPlan(BaseModel):
    """preflight/audit 실패 시 수정안(LLM structured output)."""

    reason: str = Field(description="왜 이 수정이 필요한지")

    new_start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    new_end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")

    drop_symbols: List[str] = Field(default_factory=list, description="제외할 종목코드")

    disable_event_indicator_conditions: Optional[bool] = Field(
        default=None, description="지표 조건 기반 매매 비활성화"
    )
    set_use_dart_disclosure: Optional[bool] = Field(
        default=None, description="use_dart_disclosure 강제 설정"
    )


class AuditResult(BaseModel):
    """결과 정합성 감사(LLM structured output)."""

    score: Literal["pass", "needs_improvement", "fail"]
    needs_retry: bool = Field(description="재시도 필요 여부")
    feedback: str = Field(description="재시도/개선 피드백(또는 통과 사유)")


class FinalNarrative(BaseModel):
    """사용자에게 저장/제공할 최종 해석/요약."""

    analysis_md: str = Field(description="마크다운 리포트(해석)")

    class AnalysisItem(BaseModel):
        """Strict schema 호환을 위해 dict 대신 key-value 리스트로 저장."""

        key: str = Field(description="요약 키")
        value: Union[str, int, float, bool, None] = Field(
            default=None, description="요약 값"
        )

    analysis_json: List[AnalysisItem] = Field(
        default_factory=list, description="구조화 요약(선택, key-value 리스트)"
    )

