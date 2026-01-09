"""
포트폴리오 백테스팅 시스템

이 모듈은 주가 데이터와 DART 공시 정보를 활용한 포트폴리오 백테스팅을 수행합니다.

주요 기능:
1. PostgreSQL에서 주가 데이터 조회 (daily_stock_price 테이블)
2. DART 공시 정보 조회 (score_table_dart_idc 테이블)
3. 공시 이벤트 및 지표 기반 종목 선택 및 매매 결정
4. 리밸런싱 기반 포트폴리오 전략 실행
5. 성과 지표 및 거래 내역 분석
6. 이벤트별 수익률 분석 및 리포트 생성

백테스팅 전략:
- 공시 이벤트 기반 종목 선택: 특정 공시 유형이 발생한 종목만 매수
- 지표 조건 기반 매매: 공시 지표(희석률, 감자비율 등)가 특정 조건을 만족할 때 매수/매도
- 카테고리별 신호: 공시 카테고리(증자감자, 자기주식 등)에 따른 매수/매도 신호
- 리밸런싱: 정기적으로 포트폴리오를 재구성하여 최적의 종목 조합 유지
- ATR 기반 리스크 관리: ATR을 활용한 스탑로스 및 이익실현 설정

데이터 소스:
- 주가 데이터: PostgreSQL daily_stock_price 테이블
- 공시 데이터: PostgreSQL score_table_dart_idc 테이블 (통합 버전)
- 지표 데이터: PostgreSQL score_table_dart_idc 테이블 (idc_nm, idc_score 컬럼)

사용 방법:
    from backtesting.portfolio_backtest import BacktestInput, run_backtest
    
    input_params = BacktestInput(
        start_date="2024-01-01",
        end_date="2024-12-31",
        use_dart_disclosure=True,
        sort_by="disclosure",  # 또는 "momentum", "market_cap", "event_type"
        event_indicator_conditions=[
            {
                "report_type": "유상증자 결정",
                "idc_nm": "희석률",
                "action": "BUY",
                "condition": {"min": 0.0, "max": 0.3, "operator": "between"},
                "delay_days": 0
            }
        ]
    )
    
    output = await run_backtest(input_params)
"""
import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from sqlalchemy import create_engine, text
import time
import os
from dotenv import load_dotenv
import asyncio
import json

load_dotenv()

# ============================================================
# 유틸: 날짜/필터링
# ============================================================
def _to_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def _apply_metric_filter_and_sort(
    symbol_to_score: Dict[str, float],
    *,
    filter_type: Optional[str],
    filter_percent: Optional[float],
    filter_value: Optional[float],
    sort_ascending: bool,
) -> List[str]:
    """
    공통 필터링/정렬 유틸.
    - filter_type:
      - "value": filter_value 기준으로 컷
      - "top"/"bottom": filter_percent(%) 기준으로 상/하위 컷
    """
    items = [(sym, float(score)) for sym, score in symbol_to_score.items()]

    if filter_type == "value" and filter_value is not None:
        if sort_ascending:
            items = [(s, v) for s, v in items if v <= float(filter_value)]
        else:
            items = [(s, v) for s, v in items if v >= float(filter_value)]

    # 먼저 정렬(상/하위 컷을 위해)
    items.sort(key=lambda x: x[1], reverse=not sort_ascending)

    if filter_type in ("top", "bottom") and filter_percent is not None:
        p = max(0.0, min(100.0, float(filter_percent)))
        if p == 0.0:
            return []
        k = max(1, int(round(len(items) * (p / 100.0))))
        if filter_type == "top":
            items = items[:k]
        else:  # bottom
            items = items[-k:]

    # 최종은 정렬 상태 유지
    return [s for s, _ in items]


# ============================================================
# Input 파라미터 정의
# ============================================================
@dataclass
class BacktestInput:
    """
    백테스팅 입력 파라미터 클래스
    
    이 클래스는 백테스팅에 필요한 모든 입력 파라미터를 정의합니다.
    유니버스, 필터, 정렬 기준, 투자 파라미터 등을 포함합니다.
    
    주요 파라미터:
    
    1. 유니버스 및 종목 필터링:
       - universe: 투자 대상 시장 구분 (코스피 중대형, 코스닥 대형 등)
       - target_symbols: 특정 종목 코드 리스트 (예: ["005930", "000660"])
       - target_corp_names: 특정 회사명 리스트 (예: ["삼성전자", "SK하이닉스"])
       - filter_type: 필터 타입 ("top", "bottom", "value")
       - filter_percent: 상위/하위 % (예: 20 = 상위 20%)
       - filter_value: 값 기준 필터링
    
    2. 정렬 기준:
       - sort_by: 정렬 기준 ("momentum", "market_cap", "event_type", "disclosure", "fundamental")
         * "momentum": 최근 20일 주가 급등률
         * "market_cap": 시가총액 (종가 × 추정 발행주식수)
         * "event_type": 최근 공시 이벤트의 disclosure 코드 합계
         * "disclosure": disclosure 코드 합계
         * "fundamental": 펀더멘털 지표 (현재 미구현)
       - sort_ascending: 정렬 방향 (False: 내림차순, True: 오름차순)
    
    3. 투자 파라미터:
       - initial_cash: 초기 투자금액 (기본값: 1억원)
       - rebalancing_period: 리밸런싱 주기 ("daily", "weekly", "monthly", "quarterly")
       - max_positions: 최대 보유 종목 수
       - max_portfolio_size: 포트폴리오 최대 종목 수 (선택 대상)
       - commission_rate: 거래 수수료율 (기본값: 0.05%)
       - slippage_rate: 슬리피지율 (기본값: 0.1%)
    
    4. DART 공시 설정:
       - use_dart_disclosure: DART 공시 사용 여부 (기본값: True)
       - category_signals: 카테고리별 매매 신호 설정
         예: {"증자감자": {"action": "BUY", "delay_days": 0}}
       - event_signals: 세부 이벤트별 매매 신호 설정
         예: {"감자": {"action": "SELL", "delay_days": 0}}
       - event_indicator_conditions: 지표 조건 기반 매매 설정
         예: [{"report_type": "유상증자 결정", "idc_nm": "희석률", "action": "BUY",
               "condition": {"min": 0.0, "max": 0.3, "operator": "between"}, "delay_days": 0}]
    
    5. DB 연결 설정 (.env 파일에서 불러옴):
       - db_user: DB 사용자명 (환경변수: DB_USER)
       - db_password: DB 비밀번호 (환경변수: DB_PASSWORD)
       - db_host: DB 호스트 (환경변수: DB_HOST)
       - db_port: DB 포트 (환경변수: DB_PORT)
       - db_name: DB 이름 (환경변수: DB_NAME)
    """
    # 주식 유니버스: 투자 대상이 되는 시장 구분
    # 예: 코스피 중대형, 코스닥 대형 등
    universe: List[str] = field(default_factory=lambda: [
        "코스피 중대형", "코스피 중소형", "코스닥 대형", 
        "코스닥 중형", "코스닥 소형", "코스닥 초소형"
    ])
    
    # 업종 필터: 특정 업종만 선택 (None이면 모든 업종)
    sectors: Optional[List[str]] = None
    
    # 종목 필터 설정
    filter_type: Optional[str] = None  # "top": 상위, "bottom": 하위, "value": 값 기준
    filter_percent: Optional[float] = None  # 상위/하위 % (예: 20 = 상위 20%)
    filter_value: Optional[float] = None  # 값 기준 필터링
    
    # 종목 정렬 기준
    # "momentum": 모멘텀(최근 주가 급등률), "market_cap": 시가총액, "composite_score": 종합점수,
    # "fundamental": 펀더멘탈, "event_type": 이벤트 타입, "disclosure": disclosure 코드
    sort_by: str = "disclosure"  # 기본값: disclosure 코드
    sort_ascending: bool = False  # False: 내림차순(높은 순), True: 오름차순
    
    # 투자 파라미터
    initial_cash: float = 100_000_000  # 초기 투자금액 (1억원)
    rebalancing_period: str = "monthly"  # 리밸런싱 주기: "daily", "weekly", "monthly", "quarterly"
    max_positions: int = 10  # 최대 보유 종목 수
    max_portfolio_size: int = 20  # 포트폴리오 최대 종목 수 (선택 대상)
    
    # 백테스팅 기간
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    
    # 거래 비용
    commission_rate: float = 0.0005  # 거래 수수료율 (0.05%)
    slippage_rate: float = 0.001  # 슬리피지율 (0.1%)
    
    # DB 연결 설정 (.env 파일에서 불러옴)
    db_user: str = os.getenv("DB_USER", "stockelper")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5432")
    db_name: str = os.getenv("DB_NAME", "postgres")
    
    # DART 공시 데이터 사용 여부
    use_dart_disclosure: bool = True  # True: DART 공시 사용, False: 뉴스 감성 사용 (레거시)
    
    # DART 공시 필터: 특정 테이블만 사용 (None이면 전체)
    dart_table_names: Optional[List[str]] = None  # 예: ['dart_piic_decsn', 'dart_tsstk_aq_decsn']
    
    # 특정 종목 리스트 (None이면 유니버스 전체, 지정하면 해당 종목들만 백테스팅)
    target_symbols: Optional[List[str]] = None  # 예: ['005930', '000660', '035420'] (종목코드)
    target_corp_names: Optional[List[str]] = None  # 예: ['삼성전자', 'SK하이닉스', 'NAVER'] (회사명)
    
    # 각 카테고리에 대해 {"action": "BUY"/"SELL"/"NEUTRAL", "delay_days": N} 형태
    category_signals: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        # 긍정적 이벤트 (매수 신호)
        "증자감자": {"action": "BUY", "delay_days": 0},  # 유상증자, 무상증자, 유무상증자 (감자는 별도 처리)
        "자기주식": {"action": "BUY", "delay_days": 0},  # 자기주식 취득/처분, 신탁계약
        "사채발행": {"action": "BUY", "delay_days": 0},  # 전환사채권, 신주인수권부사채권 등
        "영업양수도": {"action": "BUY", "delay_days": 0},  # 영업양수 (영업양도는 별도 처리)
        "자산양수도": {"action": "NEUTRAL", "delay_days": 0},  # 유형자산 양수/양도 (중립)
        "타법인주식": {"action": "NEUTRAL", "delay_days": 0},  # 타법인 주식 양수/양도 (중립)
        "사채권양수도": {"action": "NEUTRAL", "delay_days": 0},  # 주권관련 사채권 양수/양도 (중립)
        "합병분할": {"action": "NEUTRAL", "delay_days": 0},  # 회사합병, 회사분할, 분할합병, 주식교환이전 (중립)
        "해외상장": {"action": "NEUTRAL", "delay_days": 0},  # 해외 증권시장 상장/상장폐지 (중립)
        
        # 부정적 이벤트 (매도 신호)
        "기업상태": {"action": "SELL", "delay_days": 0},  # 부도발생, 영업정지, 회생절차, 해산사유
        "채권은행": {"action": "SELL", "delay_days": 0},  # 채권은행 관리절차
        "소송": {"action": "SELL", "delay_days": 0},  # 소송 제기
    })
    
    # 세부 이벤트별 매매 신호 설정 (카테고리 내 세부 이벤트)
    # 예: "증자감자" 카테고리 내에서 "감자"만 매도 신호
    event_signals: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        "감자": {"action": "SELL", "delay_days": 0},  # 감자는 매도 신호
        "영업양도": {"action": "SELL", "delay_days": 0},  # 영업양도는 매도 신호
        "자기주식 처분": {"action": "SELL", "delay_days": 0},  # 자기주식 처분은 매도 신호
    })
    
    # 이벤트별 지표 조건 설정 (score_table_dart_idc 기반)
    # 각 이벤트에 대해 지표 조건을 설정하여 필터링
    # 사용자가 직접 어떤 공시정보의 지표가 얼마를 넘으면 매수/매도할지 설정 가능
    event_indicator_conditions: List[Dict[str, Any]] = field(default_factory=list)
    # 예시:
    # [
    #     {
    #         "report_type": "유상증자 결정",  # 공시 유형
    #         "idc_nm": "희석률",  # 지표명
    #         "action": "BUY",  # 조건 만족 시 매수/매도 ("BUY" 또는 "SELL")
    #         "condition": {
    #             "min": 0.0,  # 최소값 (None이면 제한 없음)
    #             "max": 0.3,  # 최대값 (None이면 제한 없음)
    #             "operator": "between"  # "between", ">=", "<=", ">", "<", "=="
    #         },
    #         "delay_days": 0  # 공시일로부터 N일 후 매매
    #     },
    #     {
    #         "report_type": "감자 결정",
    #         "idc_nm": "감자비율",
    #         "action": "SELL",
    #         "condition": {
    #             "min": 0.1,  # 0.1 이상일 때 매도
    #             "max": None,
    #             "operator": ">="
    #         },
    #         "delay_days": 0
    #     },
    #     {
    #         "report_type": "회사합병 결정",
    #         "idc_nm": "합병비율",
    #         "action": "BUY",
    #         "condition": {
    #             "min": 0.5,
    #             "max": 1.5,
    #             "operator": "between"  # 0.5 ~ 1.5 사이일 때 매수
    #         },
    #         "delay_days": 1  # 공시일 다음날 매수
    #     }
    # ]
    
    # 여러 조건 조합 설정
    # 조건들을 AND/OR로 조합하여 매매 결정
    use_condition_combination: bool = False  # 조건 조합 사용 여부
    condition_logic: str = "AND"  # "AND" 또는 "OR"
    min_positive_signals: int = 1  # 최소 긍정 신호 개수 (AND 조건일 때)

    # ------------------------------------------------------------
    # 유니버스 스크리닝(필터링) 설정
    #   - 목적: "백테스트 실행 전 유니버스 축소" (성능/탐색 비용 절감)
    #   - 실제 매매 의사결정은 PortfolioStrategy에서 다시 수행됨
    # ------------------------------------------------------------
    sentiment_screening_days: int = 30  # 공시 이벤트 집계 구간(최근 N일)
    screening_concurrency: int = 20  # 유니버스 스크리닝 병렬도(과도하면 DB 부하)
    
    # DART 공시 스크리닝 설정
    dart_screening_days: int = 30  # 공시 이벤트 집계 구간(최근 N일)
    dart_event_score_weight: float = 1.0  # 공시 이벤트 점수 가중치


# ============================================================
# Output 구조 정의
# ============================================================
@dataclass
class BacktestOutput:
    """
    백테스팅 출력 결과 클래스
    
    백테스팅 실행 후 생성되는 모든 결과를 담는 클래스입니다.
    성과 지표, 거래 내역, 리포트 등을 포함합니다.
    """
    # 성과 지표
    cumulative_return: float = 0.0  # 누적 수익률 (소수)
    total_return: float = 0.0  # 총 수익률 (%)
    annualized_return: float = 0.0  # 연환산 수익률 (%)
    mdd: float = 0.0  # Maximum Drawdown (최대 낙폭, %)
    sharpe_ratio: float = 0.0  # 샤프 지수
    win_rate: float = 0.0  # 승률 (%)
    total_trades: int = 0  # 총 거래 횟수
    total_profit: float = 0.0  # 총 수익 (원)
    total_loss: float = 0.0  # 총 손실 (원)
    
    # 거래 내역: 일자별 매매 종목 정보
    # 각 거래는 다음 정보를 포함:
    # - date: 거래 일자
    # - symbol: 종목 코드
    # - action: 매매 행위 (BUY/SELL)
    # - size: 거래 수량
    # - price: 거래 가격
    # - amount: 거래 금액
    # - reason: 매매 사유
    trades: List[Dict] = field(default_factory=list)
    
    # 리포트: LLM 기반 생성 리포트
    report: str = ""
    
    # 이벤트별 수익률 분석: 각 이벤트 타입별 성과 통계
    event_performance: Dict[str, Dict] = field(default_factory=dict)


# ============================================================
# DART 테이블명 및 이벤트 타입 매핑
# ============================================================

# 사용 가능한 DART 공시 테이블 목록 (이미지에서 확인한 테이블명들)
AVAILABLE_DART_TABLES = [
    "dart_bdwt_is_decsn",  # 신주인수권부사채권 발행결정
    "dart_bsn_inh_decsn",  # 영업양수 결정
    "dart_bsn_trf_decsn",  # 영업양도 결정
    "dart_cmp_dv_decsn",  # 회사분할 결정
    "dart_cmp_dvmg_decsn",  # 회사분할합병 결정
    "dart_cmp_mg_decsn",  # 회사합병 결정
    "dart_cr_decsn",  # 감자 결정
    "dart_cvbd_is_decsn",  # 전환사채권 발행결정
    "dart_fric_decsn",  # 무상증자 결정
    "dart_otcpr_stk_invscr_inh_decsn",  # 타법인주식 양수결정
    "dart_otcpr_stk_invscr_trf_decsn",  # 타법인주식 양도결정
    "dart_pifric_decsn",  # 유무상증자 결정
    "dart_piic_decsn",  # 유상증자 결정
    "dart_stk_extr_decsn",  # 주식교환이전 결정
    "dart_tgast_inh_decsn",  # 유형자산 양수 결정
    "dart_tgast_trf_decsn",  # 유형자산 양도 결정
    "dart_tsstk_aq_decsn",  # 자기주식 취득 결정
    "dart_tsstk_aq_trctr_cc_decsn",  # 자기주식취득 신탁계약 해지 결정
    "dart_tsstk_aq_trctr_cns_decsn",  # 자기주식취득 신탁계약 체결 결정
    "dart_tsstk_dp_decsn",  # 자기주식 처분 결정
]


def map_dart_table_to_disclosure_code(table_name: str) -> int:
    """
    DART 테이블명을 disclosure 코드로 변환
    
    백테스팅 전략에서 사용하는 disclosure 코드:
    - 0: 이벤트 없음 (general)
    - 1: 긍정적 이벤트 (증자, 자기주식 매입 등)
    - 2: 부정적 이벤트 (감자, 영업정지 등)
    - 3: 중립적 이벤트 (합병, 분할 등)
    
    Args:
        table_name: PostgreSQL 테이블명 (예: 'dart_piic_decsn')
    
    Returns:
        disclosure 코드 (0, 1, 2, 3)
    """
    # 긍정적 이벤트 테이블
    positive_tables = [
        "dart_piic_decsn",  # 유상증자
        "dart_fric_decsn",  # 무상증자
        "dart_pifric_decsn",  # 유무상증자
        "dart_tsstk_aq_decsn",  # 자기주식 취득
        "dart_tsstk_aq_trctr_cns_decsn",  # 자기주식취득 신탁계약 체결
        "dart_bdwt_is_decsn",  # 신주인수권부사채권 발행
        "dart_cvbd_is_decsn",  # 전환사채권 발행
    ]
    
    # 부정적 이벤트 테이블
    negative_tables = [
        "dart_cr_decsn",  # 감자
        "dart_tsstk_dp_decsn",  # 자기주식 처분
    ]
    
    if table_name in positive_tables:
        return 1
    elif table_name in negative_tables:
        return 2
    else:
        return 3  # 중립적 이벤트 (합병, 분할, 양수도 등)


def get_table_display_name(table_name: str) -> str:
    """
    테이블명을 한글 표시명으로 변환
    
    Args:
        table_name: PostgreSQL 테이블명

    Returns:
        한글 표시명
    """
    name_mapping = {
        "dart_bdwt_is_decsn": "신주인수권부사채권 발행결정",
        "dart_bsn_inh_decsn": "영업양수 결정",
        "dart_bsn_trf_decsn": "영업양도 결정",
        "dart_cmp_dv_decsn": "회사분할 결정",
        "dart_cmp_dvmg_decsn": "회사분할합병 결정",
        "dart_cmp_mg_decsn": "회사합병 결정",
        "dart_cr_decsn": "감자 결정",
        "dart_cvbd_is_decsn": "전환사채권 발행결정",
        "dart_fric_decsn": "무상증자 결정",
        "dart_otcpr_stk_invscr_inh_decsn": "타법인주식 양수결정",
        "dart_otcpr_stk_invscr_trf_decsn": "타법인주식 양도결정",
        "dart_pifric_decsn": "유무상증자 결정",
        "dart_piic_decsn": "유상증자 결정",
        "dart_stk_extr_decsn": "주식교환이전 결정",
        "dart_tgast_inh_decsn": "유형자산 양수 결정",
        "dart_tgast_trf_decsn": "유형자산 양도 결정",
        "dart_tsstk_aq_decsn": "자기주식 취득 결정",
        "dart_tsstk_aq_trctr_cc_decsn": "자기주식취득 신탁계약 해지 결정",
        "dart_tsstk_aq_trctr_cns_decsn": "자기주식취득 신탁계약 체결 결정",
        "dart_tsstk_dp_decsn": "자기주식 처분 결정",
    }
    return name_mapping.get(table_name, table_name)


def map_report_type_to_disclosure_code(report_type: str) -> int:
    """
    report_type을 disclosure 코드로 변환
    
    Args:
        report_type: 공시 유형 (예: "유상증자 결정")
        
    Returns:
        disclosure 코드 (0, 1, 2, 3)
    """
    # 긍정적 이벤트
    positive_keywords = ["유상증자", "무상증자", "유무상증자", "자기주식 취득", "신탁계약 체결", 
                        "전환사채권 발행", "신주인수권부사채권 발행"]
    # 부정적 이벤트
    negative_keywords = ["감자", "자기주식 처분"]
    
    for keyword in positive_keywords:
        if keyword in report_type:
            return 1
    
    for keyword in negative_keywords:
        if keyword in report_type:
            return 2
    
    return 3  # 중립적 이벤트



# ============================================================
# DB 연결 및 데이터 조회
# ============================================================
class DataLoader:
    """
    DB에서 주가 데이터 및 DART 공시 데이터를 조회하는 클래스
    
    주요 기능:
    1. PostgreSQL에서 주가 데이터 조회 (daily_stock_price 테이블)
    2. PostgreSQL에서 DART 공시 데이터 조회 (score_table_dart_idc 테이블)
    3. PostgreSQL에서 지표 데이터 조회 (score_table_dart_idc 테이블)
    4. 주가 기반 지표 계산 (모멘텀, 시가총액 등)
    5. 공시 이벤트 기반 점수 계산
    
    데이터 소스:
    - 주가: daily_stock_price 테이블
    - 공시: score_table_dart_idc 테이블 (통합 버전)
    - 지표: score_table_dart_idc 테이블 (idc_nm, idc_score 컬럼)
    """
    
    def __init__(self, input_params: BacktestInput):
        """
        DataLoader 초기화
        
        Args:
            input_params: 백테스팅 입력 파라미터
        """
        self.input = input_params
        
        # PostgreSQL 연결 설정
        # NOTE:
        # - 이 프로젝트 의존성은 psycopg3(`psycopg[binary]`)를 포함합니다.
        # - SQLAlchemy 기본 스킴 `postgresql://` 은 psycopg2를 기본 드라이버로 사용하므로,
        #   운영 환경에서 psycopg2가 없을 경우 연결 실패가 날 수 있습니다.
        # - 따라서 psycopg3 드라이버를 명시합니다.
        self.pg_engine = create_engine(
            f'postgresql+psycopg://{input_params.db_user}:{input_params.db_password}@'
            f'{input_params.db_host}:{input_params.db_port}/{input_params.db_name}'
        )
        
        # DART 공시 사용 여부
        self.use_dart = input_params.use_dart_disclosure
    
    def get_stock_price_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        PostgreSQL에서 주가 데이터 조회
        
        Args:
            symbol: 종목 코드 (예: "005930")
            start_date: 시작 날짜 (YYYY-MM-DD)
            end_date: 종료 날짜 (YYYY-MM-DD)
        
        Returns:
            주가 데이터프레임 (datetime 인덱스)
            컬럼: open, high, low, close, volume
        
        Raises:
            Exception: DB 연결 실패 시
        """
        query = text("""
            SELECT date, open, high, low, close, volume, adj_close
            FROM daily_stock_price
            WHERE symbol = :symbol
            AND date >= :start_date
            AND date <= :end_date
            ORDER BY date
        """)
        
        try:
            with self.pg_engine.connect() as conn:
                df = pd.read_sql(
                    query,
                    conn,
                    params={"symbol": symbol, "start_date": start_date, "end_date": end_date}
                )
            
            if df.empty:
                print(f"⚠️  경고: {symbol}의 주가 데이터가 없습니다.")
                return pd.DataFrame()
            
            # 날짜를 인덱스로 설정
            df['datetime'] = pd.to_datetime(df['date'])
            df = df.set_index('datetime')
            df = df[['open', 'high', 'low', 'close', 'volume']]
            df.columns = [col.lower() for col in df.columns]
            
            return df
            
        except Exception as e:
            print(f"❌ 오류: {symbol} 주가 데이터 조회 실패 - {e}")
            return pd.DataFrame()
    
    def get_dart_disclosure_data(
        self, 
        symbol: Optional[str] = None,
        corp_name: Optional[str] = None,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        score_table_dart_idc 테이블에서 공시 데이터 조회 (통합 버전)
        
        Args:
            symbol: 종목 코드 (6자리) - symbol 또는 corp_name 중 하나 필수
            corp_name: 회사명 - symbol 또는 corp_name 중 하나 필수
            start_date: 시작 날짜 (YYYY-MM-DD)
            end_date: 종료 날짜 (YYYY-MM-DD)
        
        Returns:
            공시 데이터프레임 (datetime 인덱스)
            컬럼: report_type, event_type, disclosure, category 등
        """
        if not self.use_dart:
            return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type'])
        
        if not symbol and not corp_name:
            return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type'])
        
        # WHERE 조건 구성
        where_conditions = []
        params = {}
        
        if symbol:
            where_conditions.append("stock_code = :symbol")
            params["symbol"] = symbol
        
        if corp_name:
            where_conditions.append("corp_name = :corp_name")
            params["corp_name"] = corp_name
        
        if start_date:
            # YYYY-MM-DD 형식을 date 객체로 변환
            where_conditions.append("rcept_dt >= :start_date")
            params["start_date"] = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        if end_date:
            # YYYY-MM-DD 형식을 date 객체로 변환
            where_conditions.append("rcept_dt <= :end_date")
            params["end_date"] = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = text(f"""
            SELECT DISTINCT
                rcept_dt, 
                stock_code, 
                corp_name, 
                report_type,
                category,
                rcept_no
            FROM score_table_dart_idc
            WHERE {where_clause}
            ORDER BY rcept_dt
        """)
        
        try:
            with self.pg_engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            
            if df.empty:
                # 디버깅: 쿼리와 파라미터 출력
                print(f"   🔍 디버깅: {symbol or corp_name} 공시 데이터 조회 결과 없음")
                print(f"      WHERE 조건: {where_clause}")
                print(f"      파라미터: {params}")
                print(f"      쿼리: SELECT DISTINCT rcept_dt, stock_code, corp_name, report_type, category, rcept_no FROM score_table_dart_idc WHERE {where_clause}")
                # 실제로 해당 종목의 데이터가 있는지 확인 (날짜 조건 없이)
                test_query = text("""
                    SELECT COUNT(*) as cnt, MIN(rcept_dt) as min_date, MAX(rcept_dt) as max_date
                    FROM score_table_dart_idc
                    WHERE stock_code = :symbol
                """)
                test_params = {"symbol": symbol} if symbol else {}
                if not symbol and corp_name:
                    test_query = text("""
                        SELECT COUNT(*) as cnt, MIN(rcept_dt) as min_date, MAX(rcept_dt) as max_date
                        FROM score_table_dart_idc
                        WHERE corp_name = :corp_name
                    """)
                    test_params = {"corp_name": corp_name}
                
                try:
                    with self.pg_engine.connect() as conn:
                        test_df = pd.read_sql(test_query, conn, params=test_params)
                        if not test_df.empty:
                            cnt = test_df.iloc[0]['cnt']
                            min_date = test_df.iloc[0]['min_date']
                            max_date = test_df.iloc[0]['max_date']
                            if cnt > 0:
                                print(f"      ⚠️  테이블 내 총 데이터: {cnt}건 (기간: {min_date} ~ {max_date})")
                                print(f"      💡 백테스팅 기간({params.get('start_date')} ~ {params.get('end_date')})에 데이터가 없을 수 있습니다.")
                            else:
                                print(f"      ⚠️  테이블에 해당 종목 데이터가 없습니다.")
                                # stock_code 형식 확인 (앞의 0 제거한 버전도 시도)
                                if symbol:
                                    symbol_no_zero = symbol.lstrip('0') or '0'
                                    alt_query = text("""
                                        SELECT DISTINCT stock_code, COUNT(*) as cnt
                                        FROM score_table_dart_idc
                                        WHERE stock_code LIKE :pattern
                                        GROUP BY stock_code
                                        LIMIT 5
                                    """)
                                    try:
                                        with self.pg_engine.connect() as conn:
                                            alt_df = pd.read_sql(alt_query, conn, params={"pattern": f"%{symbol_no_zero}%"})
                                            if not alt_df.empty:
                                                print(f"      💡 유사한 stock_code: {alt_df['stock_code'].tolist()}")
                                    except:
                                        pass
                except Exception as e:
                    print(f"      ⚠️  디버깅 쿼리 실패: {e}")
                return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type', 'category'])
            
            # 날짜를 인덱스로 설정
            # rcept_dt가 이미 date 타입이므로 직접 사용
            if 'rcept_dt' in df.columns:
                # date 타입을 datetime으로 변환
                df['date'] = pd.to_datetime(df['rcept_dt'], errors='coerce')
                df = df.dropna(subset=['date'])
            else:
                return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type', 'category'])
            
            # report_type에서 event_type 추출, category는 테이블에서 직접 가져옴
            all_data = []
            for _, row in df.iterrows():
                report_type = row.get('report_type', '')
                # report_type을 기반으로 event_type 추출
                event_type = report_type.replace(' 결정', '').replace(' 발행결정', '').lower()
                category = row.get('category', '기타')  # 테이블에서 직접 가져옴
                disclosure = map_report_type_to_disclosure_code(report_type)
                
                all_data.append({
                    'date': row['date'],
                    'report_type': report_type,
                    'event_type': event_type,
                    'disclosure': disclosure,
                    'category': category,
                    'rcept_no': row.get('rcept_no', ''),
                    'stock_code': row.get('stock_code', ''),
                    'corp_name': row.get('corp_name', ''),
                })
            
            if not all_data:
                return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type', 'category'])
            
            result_df = pd.DataFrame(all_data)
            result_df = result_df.set_index('date')
            result_df = result_df.sort_index()
            
            return result_df
                    
        except Exception as e:
            print(f"⚠️  경고: 공시 데이터 조회 실패 ({symbol or corp_name}): {e}")
            return pd.DataFrame(columns=['event_type', 'disclosure', 'report_type', 'category'])
    
    async def get_news_sentiment_data(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        뉴스 감성 데이터 조회 (레거시 호환용)
        
        DART 공시를 사용하는 경우 DART 데이터를 반환하고,
        그렇지 않으면 빈 DataFrame을 반환합니다.
        
        Args:
            symbol: 종목 코드
            start_date: 시작 날짜
            end_date: 종료 날짜
        
        Returns:
            공시 데이터프레임 (datetime 인덱스)
            컬럼: event_type, disclosure, report_type, category
        """
        if self.use_dart:
            # DART 공시 데이터 사용
            return self.get_dart_disclosure_data(symbol, start_date, end_date)
        
        # 레거시: 빈 DataFrame 반환
        return pd.DataFrame(columns=['event_type', 'disclosure'])

    async def get_sentiment_screening_score(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        lookback_days: int = 30,
    ) -> float:
        """
        유니버스 축소용 "공시 이벤트 점수"를 계산합니다.

        DART 공시를 사용하는 경우:
        1) 최근 lookback_days 동안의 공시 이벤트를 기반으로 점수 계산
        2) 긍정적 이벤트(disclosure=1)는 +1, 부정적 이벤트(disclosure=2)는 -1, 중립(disclosure=3)은 0으로 가중치 적용
        3) 최근 이벤트에 더 높은 가중치 부여 (선형 감쇠)
        
        레거시 모드:
        1) disclosure 코드 기반으로 점수 계산
        2) 데이터가 없으면 0.0(중립) 반환
        """
        try:
            start_dt = _to_datetime(start_date)
            end_dt = _to_datetime(end_date)
            if end_dt < start_dt:
                start_dt, end_dt = end_dt, start_dt
            if lookback_days and lookback_days > 0:
                window_start = max(start_dt, end_dt - timedelta(days=int(lookback_days)))
            else:
                window_start = start_dt
            window_start_str = window_start.strftime("%Y-%m-%d")

            if self.use_dart:
                # DART 공시 기반 점수 계산
                df_dart = self.get_dart_disclosure_data(symbol=symbol, start_date=window_start_str, end_date=end_date)
                if not df_dart.empty and "disclosure" in df_dart.columns:
                    # disclosure 코드를 기반으로 점수 계산
                    # 1(긍정) -> +1, 2(부정) -> -1, 3(중립) -> 0
                    disclosure_scores = df_dart["disclosure"].map({
                        1: 1.0 * self.input.dart_event_score_weight,
                        2: -1.0 * self.input.dart_event_score_weight,
                        3: 0.0
                    }).fillna(0.0)
                    
                    # 최근 이벤트에 더 높은 가중치 부여 (선형 감쇠)
                    if len(disclosure_scores) > 0:
                        weights = np.linspace(0.5, 1.0, len(disclosure_scores))
                        weighted_score = (disclosure_scores * weights).sum() / weights.sum()
                        return float(np.clip(weighted_score, -1.0, 1.0))
                    else:
                        return 0.0
                else:
                    return 0.0
            else:
                # 레거시: 뉴스 감성 데이터 사용 (현재는 사용하지 않음)
                # DART 공시를 사용하지 않는 경우에도 disclosure 코드 기반으로 점수 계산
                df_sent = await self.get_news_sentiment_data(symbol, window_start_str, end_date)
                if not df_sent.empty and "disclosure" in df_sent.columns:
                    # disclosure 코드를 기반으로 점수 계산
                    disclosure_scores = df_sent["disclosure"].map({
                        1: 1.0,
                        2: -1.0,
                        3: 0.0
                    }).fillna(0.0)
                    
                    if len(disclosure_scores) > 0:
                        return float(disclosure_scores.mean())

        except Exception:
            # 스크리닝 단계에서는 조용히 중립 처리(백테스트 전체를 멈추지 않기 위함)
            pass

        return 0.0

    async def get_fundamental_screening_score(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        metric: str = "per",
        lookback_days: int = 30,
    ) -> float:
        """
        유니버스 축소용 펀더멘털 요약 점수(예: PER).

        현재는 구현되지 않았습니다.
        추후:
        - DB 테이블(예: daily_fundamental, quarterly_fundamental 등)에서 metric을 조회하도록 구현 필요
        """
        # 현재는 미구현
        return 0.0
    
    def _event_type_to_disclosure_code(self, event_type: str) -> int:
        """
        이벤트 타입을 disclosure 코드로 변환
        
        백테스팅 전략에서 사용하는 disclosure 코드:
        - 0: 이벤트 없음 (general)
        - 1: 실적 발표 (earnings)
        - 2: 자사주 매입 (share_buyback)
        
        Args:
            event_type: 이벤트 타입 문자열
        
        Returns:
            disclosure 코드 (0, 1, 2)
        """
        mapping = {
            'earnings': 1,
            'share_buyback': 2,
        }
        return mapping.get(event_type, 0)
    
    def get_universe_symbols(self) -> List[str]:
        """
        유니버스에 해당하는 종목 코드 리스트 반환
        
        target_symbols 또는 target_corp_names가 지정되어 있으면 해당 종목만 반환합니다.
        
        Returns:
            종목 코드 리스트
        """
        # 특정 종목이 지정되어 있으면 해당 종목만 반환
        if self.input.target_symbols:
            return self.input.target_symbols
        
        if self.input.target_corp_names:
            # 종목명으로 종목코드 조회 (score_table_dart_idc에서 조회)
            symbols = []
            for corp_name in self.input.target_corp_names:
                try:
                    query = text("""
                        SELECT DISTINCT stock_code 
                        FROM score_table_dart_idc 
                        WHERE corp_name = :corp_name
                        LIMIT 1
                    """)
                    with self.pg_engine.connect() as conn:
                        result = conn.execute(query, {"corp_name": corp_name})
                        row = result.fetchone()
                        if row and row[0]:
                            symbols.append(row[0])
                        else:
                            print(f"⚠️  경고: {corp_name}의 종목코드를 찾을 수 없습니다.")
                except Exception as e:
                    print(f"⚠️  경고: {corp_name} 종목코드 조회 실패 - {e}")
            
            return symbols if symbols else self._get_all_symbols()
        
        return self._get_all_symbols()
    
    def _get_all_symbols(self) -> List[str]:
        """DB에 있는 모든 종목 코드 반환"""
        query = text("SELECT DISTINCT symbol FROM daily_stock_price ORDER BY symbol")
        
        try:
            with self.pg_engine.connect() as conn:
                symbols = [row[0] for row in conn.execute(query)]
            return symbols
        except Exception as e:
            print(f"❌ 오류: 유니버스 종목 조회 실패 - {e}")
            return []
    
    def get_corp_name_from_symbol(self, symbol: str) -> Optional[str]:
        """종목코드로 회사명 조회"""
        # score_table_dart_idc에서 회사명 조회
        try:
            query = text("""
                SELECT DISTINCT corp_name 
                FROM score_table_dart_idc 
                WHERE stock_code = :symbol 
                LIMIT 1
            """)
            with self.pg_engine.connect() as conn:
                result = conn.execute(query, {"symbol": symbol})
                row = result.fetchone()
                if row:
                    return row[0]
        except:
            pass
        return None
    
    def get_stock_momentum_score(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        lookback_days: int = 20
    ) -> float:
        """
        주가 급등률(모멘텀) 점수 계산
        
        Args:
            symbol: 종목 코드
            start_date: 시작 날짜
            end_date: 종료 날짜
            lookback_days: 모멘텀 계산 기간 (일)
        
        Returns:
            모멘텀 점수 (수익률 %)
        """
        try:
            df_price = self.get_stock_price_data(symbol, start_date, end_date)
            if df_price.empty or len(df_price) < 2:
                return 0.0
            
            # 최근 lookback_days일의 수익률 계산
            recent_prices = df_price['close'].tail(lookback_days)
            if len(recent_prices) < 2:
                return 0.0
            
            start_price = recent_prices.iloc[0]
            end_price = recent_prices.iloc[-1]
            
            if start_price > 0:
                return ((end_price - start_price) / start_price) * 100.0
        except:
            pass
        
        return 0.0
    
    def get_stock_market_cap_score(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> float:
        """
        시가총액 점수 계산 (최근 종가 * 발행주식수 추정)
        
        Args:
            symbol: 종목 코드
            start_date: 시작 날짜
            end_date: 종료 날짜
        
        Returns:
            시가총액 점수 (원)
        """
        try:
            df_price = self.get_stock_price_data(symbol, start_date, end_date)
            if df_price.empty:
                return 0.0
            
            # 최근 종가
            recent_close = df_price['close'].iloc[-1]
            
            # 발행주식수는 DB에서 조회하거나 추정값 사용
            # 여기서는 간단히 종가 * 1000만주로 추정 (실제로는 DB에서 조회 필요)
            estimated_shares = 10000000  # 1000만주 추정
            market_cap = recent_close * estimated_shares
            
            return market_cap
        except:
            pass
        
        return 0.0
    
    def get_event_type_score(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> float:
        """
        event_type 기반 점수 계산 (최근 공시 이벤트의 disclosure 코드 합계)
        
        Args:
            symbol: 종목 코드
            start_date: 시작 날짜
            end_date: 종료 날짜
        
        Returns:
            event_type 점수 (disclosure 코드 합계)
        """
        try:
            df_disclosure = self.get_dart_disclosure_data(symbol=symbol, start_date=start_date, end_date=end_date)
            if df_disclosure.empty or 'disclosure' not in df_disclosure.columns:
                return 0.0
            
            # 최근 30일 이내의 disclosure 코드 합계
            recent_disclosures = df_disclosure.tail(30)
            return float(recent_disclosures['disclosure'].sum())
        except:
            pass
        
        return 0.0
    
    def get_dart_indicator_data(
        self,
        symbol: Optional[str] = None,
        corp_name: Optional[str] = None,
        start_date: str = None,
        end_date: str = None,
        report_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        score_table_dart_idc 테이블에서 지표 데이터 조회
        
        Args:
            symbol: 종목 코드 (6자리) - symbol 또는 corp_name 중 하나 필수
            corp_name: 회사명 - symbol 또는 corp_name 중 하나 필수
            start_date: 시작 날짜 (YYYY-MM-DD)
            end_date: 종료 날짜 (YYYY-MM-DD)
            report_types: 조회할 공시 유형 목록 (None이면 전체)
        
        Returns:
            지표 데이터프레임 (datetime 인덱스)
            컬럼: report_type, idc_nm, idc_score, stock_code, corp_name, rcept_dt 등
        """
        if not symbol and not corp_name:
            return pd.DataFrame()
        
        # WHERE 조건 구성
        where_conditions = []
        params = {}
        
        if symbol:
            where_conditions.append("stock_code = :symbol")
            params["symbol"] = symbol
        
        if corp_name:
            where_conditions.append("corp_name = :corp_name")
            params["corp_name"] = corp_name
        
        if start_date:
            # YYYY-MM-DD 형식을 date 객체로 변환
            where_conditions.append("rcept_dt >= :start_date")
            params["start_date"] = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        if end_date:
            # YYYY-MM-DD 형식을 date 객체로 변환
            where_conditions.append("rcept_dt <= :end_date")
            params["end_date"] = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        if report_types:
            where_conditions.append("report_type = ANY(:report_types)")
            params["report_types"] = report_types
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = text(f"""
            SELECT 
                rcept_dt, 
                stock_code, 
                corp_name, 
                report_type,
                idc_nm,
                idc_score,
                rcept_no
            FROM score_table_dart_idc
            WHERE {where_clause}
            ORDER BY rcept_dt, report_type, idc_nm
        """)
        
        try:
            with self.pg_engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            
            if df.empty:
                return pd.DataFrame()
            
            # 날짜를 인덱스로 설정
            # rcept_dt가 이미 date 타입이므로 직접 사용
            if 'rcept_dt' in df.columns:
                # date 타입을 datetime으로 변환
                df['date'] = pd.to_datetime(df['rcept_dt'], errors='coerce')
                df = df.dropna(subset=['date'])
                df = df.set_index('date')
                df = df.sort_index()
            else:
                return pd.DataFrame()
            
            return df
            
        except Exception as e:
            print(f"⚠️  경고: 지표 데이터 조회 실패 ({symbol or corp_name}): {e}")
            return pd.DataFrame()


# ============================================================
# 지표 조건 확인 함수
# ============================================================
def check_indicator_conditions(
    indicator_df: pd.DataFrame,
    event_indicator_conditions: List[Dict[str, Any]],
    current_date: datetime
) -> Dict[str, Any]:
    """
    이벤트별 지표 조건을 확인하여 매매 신호 생성
    
    Args:
        indicator_df: 지표 데이터프레임 (report_type, idc_nm, idc_score 컬럼 포함)
        event_indicator_conditions: 이벤트별 지표 조건 설정
        current_date: 현재 날짜
    
    Returns:
        {"action": "BUY"/"SELL"/"NEUTRAL", "report_type": ..., "idc_nm": ..., "idc_score": ...}
    """
    if indicator_df.empty or not event_indicator_conditions:
        return {"action": "NEUTRAL", "report_type": "", "idc_nm": "", "idc_score": None}
    
    # 현재 날짜 이전의 지표 데이터만 확인
    recent_indicators = indicator_df[indicator_df.index <= current_date]
    
    if recent_indicators.empty:
        return {"action": "NEUTRAL", "report_type": "", "idc_nm": "", "idc_score": None}
    
    # 각 조건 확인
    for condition in event_indicator_conditions:
        report_type = condition.get("report_type")
        idc_nm = condition.get("idc_nm")
        action = condition.get("action", "NEUTRAL")
        delay_days = condition.get("delay_days", 0)
        cond = condition.get("condition", {})
        
        # 해당 report_type과 idc_nm의 지표 확인
        matching_indicators = recent_indicators[
            (recent_indicators['report_type'] == report_type) &
            (recent_indicators['idc_nm'] == idc_nm)
        ]
        
        if not matching_indicators.empty:
            # 가장 최근 지표 확인
            latest_indicator = matching_indicators.iloc[-1]
            idc_score = latest_indicator.get('idc_score')
            
            if idc_score is None or pd.isna(idc_score):
                continue
            
            # 지표 조건 확인
            min_val = cond.get("min")
            max_val = cond.get("max")
            operator = cond.get("operator", "between")  # 기본값: between
            
            condition_met = False
            
            if operator == "between":
                # min_val <= idc_score <= max_val
                condition_met = True
                if min_val is not None and idc_score < min_val:
                    condition_met = False
                if max_val is not None and idc_score > max_val:
                    condition_met = False
            elif operator == ">=":
                # idc_score >= min_val
                if min_val is not None:
                    condition_met = idc_score >= min_val
            elif operator == "<=":
                # idc_score <= max_val
                if max_val is not None:
                    condition_met = idc_score <= max_val
            elif operator == ">":
                # idc_score > min_val
                if min_val is not None:
                    condition_met = idc_score > min_val
            elif operator == "<":
                # idc_score < max_val
                if max_val is not None:
                    condition_met = idc_score < max_val
            elif operator == "==":
                # idc_score == min_val (또는 max_val)
                target_val = min_val if min_val is not None else max_val
                if target_val is not None:
                    condition_met = abs(idc_score - target_val) < 1e-6  # 부동소수점 오차 고려
            
            if condition_met:
                # delay_days 고려
                indicator_date = matching_indicators.index[-1]
                signal_date = indicator_date + timedelta(days=delay_days)
                
                if signal_date <= current_date:
                    return {
                        "action": action,
                        "report_type": report_type,
                        "idc_nm": idc_nm,
                        "idc_score": float(idc_score),
                        "signal_date": signal_date,
                        "indicator_date": indicator_date
                    }
    
    return {"action": "NEUTRAL", "report_type": "", "idc_nm": "", "idc_score": None}


# ============================================================
# 카테고리별 매매 신호 생성 함수
# ============================================================
def generate_category_signals(
    disclosure_df: pd.DataFrame,
    category_signals: Dict[str, Dict[str, Any]],
    event_signals: Dict[str, Dict[str, Any]],
    current_date: datetime
) -> Dict[str, Any]:
    """
    카테고리별 공시 정보를 기반으로 매매 신호 생성 (REPORT_CATEGORIES 기반)
    
    Args:
        disclosure_df: 공시 데이터프레임 (category, event_type 컬럼 포함)
        category_signals: 카테고리별 신호 설정
        event_signals: 세부 이벤트별 신호 설정
        current_date: 현재 날짜
    
    Returns:
        {"action": "BUY"/"SELL"/"NEUTRAL", "categories": [...], "signal_date": ..., "event_type": ...}
    """
    if disclosure_df.empty:
        return {"action": "NEUTRAL", "categories": [], "signal_date": current_date, "event_type": ""}
    
    # 현재 날짜 기준으로 공시 발생 여부 확인
    buy_signals = []
    sell_signals = []
    neutral_signals = []
    
    # 최근 공시 확인 (현재 날짜 이전의 공시만)
    recent_disclosures = disclosure_df[disclosure_df.index <= current_date]
    
    if recent_disclosures.empty:
        return {"action": "NEUTRAL", "categories": [], "signal_date": current_date, "event_type": ""}
    
    # 가장 최근 공시 확인
    latest_disclosure = recent_disclosures.iloc[-1]
    latest_date = recent_disclosures.index[-1]
    
    # 1단계: 세부 이벤트별 신호 확인 (우선순위 높음)
    event_type = latest_disclosure.get('event_type', '')
    report_type = latest_disclosure.get('report_type', '')
    
    # event_type이나 report_type에서 세부 이벤트명 추출
    for event_name, signal_config in event_signals.items():
        if event_name in event_type or event_name in report_type:
            action = signal_config.get("action", "NEUTRAL")
            delay_days = signal_config.get("delay_days", 0)
            signal_date = latest_date + timedelta(days=delay_days)
            
            if signal_date <= current_date:
                if action == "BUY":
                    buy_signals.append({
                        "category": latest_disclosure.get('category', ''),
                        "event_type": event_name,
                        "disclosure_date": latest_date,
                        "signal_date": signal_date
                    })
                elif action == "SELL":
                    sell_signals.append({
                        "category": latest_disclosure.get('category', ''),
                        "event_type": event_name,
                        "disclosure_date": latest_date,
                        "signal_date": signal_date
                    })
                break  # 세부 이벤트 신호가 있으면 카테고리 신호보다 우선
    
    # 2단계: 세부 이벤트 신호가 없으면 카테고리별 신호 확인
    if not buy_signals and not sell_signals and 'category' in latest_disclosure:
        category = latest_disclosure.get('category', '')
        if category in category_signals:
            signal_config = category_signals[category]
            action = signal_config.get("action", "NEUTRAL")
            delay_days = signal_config.get("delay_days", 0)
            signal_date = latest_date + timedelta(days=delay_days)
            
            if signal_date <= current_date:
                if action == "BUY":
                    buy_signals.append({
                        "category": category,
                        "event_type": event_type,
                        "disclosure_date": latest_date,
                        "signal_date": signal_date
                    })
                elif action == "SELL":
                    sell_signals.append({
                        "category": category,
                        "event_type": event_type,
                        "disclosure_date": latest_date,
                        "signal_date": signal_date
                    })
                else:
                    neutral_signals.append({
                        "category": category,
                        "event_type": event_type,
                        "disclosure_date": latest_date,
                        "signal_date": signal_date
                    })
    
    # 3단계: 신호 우선순위 결정 (SELL > BUY > NEUTRAL)
    if sell_signals:
        return {
            "action": "SELL",
            "categories": [s["category"] for s in sell_signals],
            "signal_date": max(s["signal_date"] for s in sell_signals),
            "event_type": sell_signals[0]["event_type"],
            "details": sell_signals
        }
    elif buy_signals:
        return {
            "action": "BUY",
            "categories": [s["category"] for s in buy_signals],
            "signal_date": max(s["signal_date"] for s in buy_signals),
            "event_type": buy_signals[0]["event_type"],
            "details": buy_signals
        }
    else:
        return {
            "action": "NEUTRAL",
            "categories": [s["category"] for s in neutral_signals] if neutral_signals else [],
            "signal_date": current_date,
            "event_type": event_type,
            "details": neutral_signals
        }


# ============================================================
# 포트폴리오 백테스팅 전략
# ============================================================
class PortfolioStrategy(bt.Strategy):
    """
    포트폴리오 리밸런싱 전략
    
    이 전략은 다음과 같은 로직으로 동작합니다:
    
    1. 리밸런싱:
       - 정기적으로(매월/매주 등) 포트폴리오를 재구성
       - 현재 보유 종목을 모두 청산하고 새로운 종목 선택
       - 리밸런싱 주기: rebalancing_period 파라미터로 설정
    
    2. 종목 선택 (매수 결정):
       우선순위 순서:
       a) 지표 조건 (event_indicator_conditions): 공시 지표가 특정 조건을 만족할 때
       b) 카테고리 신호 (category_signals, event_signals): 공시 카테고리/이벤트에 따른 신호
       c) disclosure 코드: disclosure=1 (긍정)이면 매수
       - 최대 보유 종목 수(max_positions) 제한
    
    3. 포지션 관리 (매도 결정):
       우선순위 순서:
       a) 지표 조건: 공시 지표가 특정 조건을 만족할 때
       b) 카테고리 신호: 공시 카테고리/이벤트에 따른 신호
       c) disclosure 코드: disclosure=2 (부정)이면 매도
       - ATR 기반 스탑로스 및 이익실현 설정
    
    4. 리스크 관리:
       - ATR(Average True Range)을 활용한 동적 스탑로스
       - 스탑로스: 현재가 - 2*ATR
       - 이익실현: 현재가 + 3*ATR
       - 각 종목별로 독립적인 리스크 관리
    
    매매 신호 우선순위:
    - 지표 조건 > 카테고리 신호 > disclosure 코드
    - SELL 신호 > BUY 신호 > NEUTRAL
    """
    
    params = dict(
        rebalancing_period=30,  # 리밸런싱 주기 (일)
        max_positions=10,  # 최대 보유 종목 수
    )
    
    def __init__(
        self, 
        selected_symbols: List[str], 
        sentiment_data: Dict[str, pd.DataFrame], 
        use_dart: bool = True,
        category_signals: Optional[Dict[str, Dict[str, Any]]] = None,
        event_signals: Optional[Dict[str, Dict[str, Any]]] = None,
        indicator_data: Optional[Dict[str, pd.DataFrame]] = None,
        event_indicator_conditions: Optional[List[Dict[str, Any]]] = None
    ):
        """
        전략 초기화
        
        Args:
            selected_symbols: 선택된 종목 코드 리스트
            sentiment_data: 종목별 감성/공시 데이터 딕셔너리
                - sentiment_score: 감성 점수
                - disclosure: 이벤트 타입 (0: 없음, 1: 긍정, 2: 부정, 3: 중립)
                - event_type: 이벤트 타입 문자열
                - category: 카테고리
            use_dart: DART 공시 사용 여부
            category_signals: 카테고리별 매매 신호 설정
            event_signals: 세부 이벤트별 매매 신호 설정
            indicator_data: 종목별 지표 데이터 딕셔너리 (score_table_dart_idc 기반)
                - report_type: 공시 유형
                - idc_nm: 지표명
                - idc_score: 지표 점수
            event_indicator_conditions: 이벤트별 지표 조건 설정
        """
        self.selected_symbols = selected_symbols
        self.sentiment_data = sentiment_data
        self.use_dart = use_dart
        self.category_signals = category_signals or {}
        self.event_signals = event_signals or {}
        self.indicator_data = indicator_data or {}
        self.event_indicator_conditions = event_indicator_conditions or []
        self.rebalance_date = None  # 마지막 리밸런싱 날짜
        self.portfolio_positions = {}  # 현재 보유 포지션 {symbol: size}
        self.trades_log = []  # 거래 내역 로그
        
        # 각 종목에 대한 기술적 지표 계산
        # SMA(이동평균선)와 ATR(평균 진폭)을 계산하여 추세와 변동성 파악
        self.indicators = {}
        for i, data in enumerate(self.datas):
            symbol = self.selected_symbols[i]
            self.indicators[symbol] = {
                'sma_fast': bt.indicators.SMA(data.close, period=10),  # 단기 이동평균
                'sma_slow': bt.indicators.SMA(data.close, period=30),  # 장기 이동평균
                'atr': bt.indicators.ATR(data, period=14),  # 평균 진폭 (변동성 지표)
            }
    
    def next(self):
        """
        매 거래일마다 호출되는 메서드
        
        리밸런싱 주기가 되면 포트폴리오를 재구성하고,
        각 종목의 포지션을 관리합니다.
        """
        current_date = self.data.datetime.date(0)
        
        # 리밸런싱 체크
        # 마지막 리밸런싱 이후 지정된 기간이 지났는지 확인
        if (self.rebalance_date is None or 
            (current_date - self.rebalance_date).days >= self.params.rebalancing_period):
            self.rebalance(current_date)
            self.rebalance_date = current_date
        
        # 개별 종목 포지션 관리
        # 각 종목에 대해 스탑로스, 이익실현 등을 체크
        for i, data in enumerate(self.datas):
            symbol = self.selected_symbols[i]
            self.manage_position(symbol, data, current_date)
    
    def rebalance(self, date):
        """
        포트폴리오 리밸런싱 수행
        
        리밸런싱 과정:
        1. 현재 보유 종목 모두 청산
        2. 감성 점수 기반으로 새로운 종목 선택
        3. 선택된 종목에 균등 분할 투자
        
        Args:
            date: 리밸런싱 날짜
        """
        # 1단계: 현재 보유 종목 청산
        for symbol in list(self.portfolio_positions.keys()):
            if symbol in self.selected_symbols:
                idx = self.selected_symbols.index(symbol)
                pos = self.getposition(self.datas[idx])
                if pos.size > 0:
                    self.close(data=self.datas[idx])
                    self.log_trade(symbol, date, 'SELL', pos.size, 
                                 self.datas[idx].close[0], '리밸런싱')
        
        self.portfolio_positions.clear()
        
        # 2단계: 새로운 종목 선택 및 매수
        # 사용 가능한 현금을 최대 보유 종목 수로 나눔
        num_positions = min(len(self.selected_symbols), self.params.max_positions)
        cash_per_position = self.broker.getcash() / num_positions if num_positions > 0 else 0
        
        for i, symbol in enumerate(self.selected_symbols[:self.params.max_positions]):
            data = self.datas[i]
            sentiment_df = self.sentiment_data.get(symbol, pd.DataFrame())
            
            # 해당 날짜의 공시 정보 조회
            date_str = date.strftime('%Y-%m-%d')
            
            # 지표 조건 > 카테고리 신호 > disclosure 코드 순으로 매수 결정
            should_buy = False
            reason = ""
            
            # 1단계: 지표 조건 확인 (최우선순위)
            if self.use_dart and self.event_indicator_conditions:
                indicator_df = self.indicator_data.get(symbol, pd.DataFrame())
                if not indicator_df.empty:
                    try:
                        date_dt = datetime.combine(date, datetime.min.time())
                        indicator_signal = check_indicator_conditions(
                            indicator_df,
                            self.event_indicator_conditions,
                            date_dt
                        )
                        
                        if indicator_signal["action"] == "BUY":
                            should_buy = True
                            report_type = indicator_signal.get("report_type", "")
                            idc_nm = indicator_signal.get("idc_nm", "")
                            idc_score = indicator_signal.get("idc_score", 0)
                            reason = f'지표 조건: {report_type} ({idc_nm}={idc_score:.4f})'
                    except Exception as e:
                        pass  # 지표 조건 확인 실패 시 다음 단계로
            
            # 2단계: 지표 조건이 없으면 카테고리별 신호 확인
            if not should_buy and self.use_dart and not sentiment_df.empty:
                if self.category_signals or self.event_signals:
                    try:
                        date_dt = datetime.combine(date, datetime.min.time())
                        signal = generate_category_signals(
                            sentiment_df, 
                            self.category_signals, 
                            self.event_signals,
                            date_dt
                        )
                        
                        if signal["action"] == "BUY":
                            should_buy = True
                            categories_str = ", ".join(signal["categories"]) if signal["categories"] else signal.get("event_type", "")
                            reason = f'카테고리 신호: {categories_str}'
                    except Exception as e:
                        pass
            
            # 3단계: 카테고리 신호가 없으면 disclosure 코드만 확인
            if not should_buy and self.use_dart and not sentiment_df.empty:
                try:
                    before_dates = sentiment_df.loc[:date_str]
                    if len(before_dates) > 0:
                        latest_event = before_dates.iloc[-1]
                        disclosure = latest_event.get('disclosure', 0)
                        event_type = latest_event.get('event_type', 'general')
                        
                    if disclosure == 1:  # 긍정적 이벤트
                        should_buy = True
                        reason = f'긍정 이벤트: {event_type}'
                except:
                    pass
            
            if should_buy:
                size = int(cash_per_position / data.close[0])
                if size > 0:
                    self.buy(data=data, size=size)
                    self.portfolio_positions[symbol] = size
                    self.log_trade(symbol, date, 'BUY', size, data.close[0], reason)
    
    def manage_position(self, symbol: str, data, date):
        """
        개별 포지션 관리
        
        보유 중인 종목에 대해 다음을 수행:
        1. 부정적 감성 체크 → 즉시 청산
        2. ATR 기반 스탑로스 및 이익실현 설정
        
        Args:
            symbol: 종목 코드
            data: backtrader 데이터 객체
            date: 현재 날짜
        """
        pos = self.getposition(data)
        if pos.size == 0:
            return
        
        sentiment_df = self.sentiment_data.get(symbol, pd.DataFrame())
        if sentiment_df.empty:
            return
        
        # 공시 정보 조회
        date_str = date.strftime('%Y-%m-%d')
        
        # 1단계: 지표 조건 > 카테고리 신호 > disclosure 코드 순으로 매도 결정
        should_sell = False
        sell_reason = ""
        
        # 1-1단계: 지표 조건 확인 (최우선순위)
        if self.use_dart and self.event_indicator_conditions:
            indicator_df = self.indicator_data.get(symbol, pd.DataFrame())
            if not indicator_df.empty:
                try:
                    date_dt = datetime.combine(date, datetime.min.time())
                    indicator_signal = check_indicator_conditions(
                        indicator_df,
                        self.event_indicator_conditions,
                        date_dt
                    )
                    
                    if indicator_signal["action"] == "SELL":
                        should_sell = True
                        report_type = indicator_signal.get("report_type", "")
                        idc_nm = indicator_signal.get("idc_nm", "")
                        idc_score = indicator_signal.get("idc_score", 0)
                        sell_reason = f'지표 조건: {report_type} ({idc_nm}={idc_score:.4f})'
                except Exception as e:
                    pass  # 지표 조건 확인 실패 시 다음 단계로
        
        # 1-2단계: 지표 조건이 없으면 카테고리별 신호 확인
        if not should_sell and self.use_dart and not sentiment_df.empty:
            if self.category_signals or self.event_signals:
                try:
                    date_dt = datetime.combine(date, datetime.min.time())
                    signal = generate_category_signals(
                        sentiment_df, 
                        self.category_signals, 
                        self.event_signals,
                        date_dt
                    )
                    
                    if signal["action"] == "SELL":
                        should_sell = True
                        categories_str = ", ".join(signal["categories"]) if signal["categories"] else signal.get("event_type", "")
                        sell_reason = f'카테고리 신호: {categories_str}'
                except Exception as e:
                    pass
        
        # 1-3단계: 카테고리 신호가 없으면 disclosure 코드만 확인
        if not should_sell and self.use_dart and not sentiment_df.empty:
            try:
                before_dates = sentiment_df.loc[:date_str]
                if len(before_dates) > 0:
                    latest_event = before_dates.iloc[-1]
                    disclosure = latest_event.get('disclosure', 0)
                    event_type = latest_event.get('event_type', 'general')
                    
                    if disclosure == 2:  # 부정적 이벤트
                        should_sell = True
                        sell_reason = f'부정 이벤트: {event_type}'
            except:
                pass
        
        if should_sell:
            self.close(data=data)
            self.log_trade(symbol, date, 'SELL', pos.size, data.close[0], sell_reason)
            if symbol in self.portfolio_positions:
                del self.portfolio_positions[symbol]
            return
        
        # 2단계: ATR 기반 스탑로스 및 이익실현 설정
        # ATR은 변동성을 나타내는 지표로, 이를 활용하여 동적 리스크 관리
        indicators = self.indicators[symbol]
        atr = max(indicators['atr'][0], 0.1)  # 최소값 0.1로 제한
        
        # 스탑로스: 현재가 - 2*ATR
        # 이익실현: 현재가 + 3*ATR
        stop_price = data.close[0] - 2 * atr
        take_profit = data.close[0] + 3 * atr
        
        # 기존 주문 취소 (새로운 가격으로 갱신하기 위해)
        for order in self.broker.orders:
            if order.data == data and order.status in [order.Submitted, order.Accepted]:
                self.broker.cancel(order)
        
        # 새로운 스탑로스 및 이익실현 주문 설정
        if pos.size > 0:
            self.sell(data=data, exectype=bt.Order.Stop, price=stop_price, size=pos.size)
            self.sell(data=data, exectype=bt.Order.Limit, price=take_profit, size=pos.size)
    
    def log_trade(self, symbol: str, date, action: str, size: int, price: float, reason: str):
        """
        거래 내역 기록
        
        Args:
            symbol: 종목 코드
            date: 거래 날짜
            action: 매매 행위 (BUY/SELL)
            size: 거래 수량
            price: 거래 가격
            reason: 매매 사유
        """
        self.trades_log.append({
            'date': date.strftime('%Y-%m-%d') if isinstance(date, datetime) else str(date),
            'symbol': symbol,
            'action': action,
            'size': size,
            'price': price,
            'amount': size * price,
            'reason': reason
        })
    
    def notify_order(self, order):
        """
        주문 상태 알림 콜백
        
        주문이 체결되거나 취소될 때 호출됩니다.
        """
        if order.status in [order.Completed]:
            pass  # log_trade에서 처리


# ============================================================
# 백테스팅 실행 함수
# ============================================================
async def run_backtest(input_params: BacktestInput) -> BacktestOutput:
    """
    백테스팅 실행 메인 함수
    
    전체 백테스팅 프로세스:
    1. 유니버스 종목 조회: target_symbols 또는 target_corp_names 지정 시 해당 종목만, 아니면 전체
    2. 종목 스크리닝: sort_by 기준으로 정렬 및 필터링
       - "momentum": 최근 20일 주가 급등률
       - "market_cap": 시가총액
       - "event_type": 최근 공시 이벤트의 disclosure 코드 합계
       - "disclosure": disclosure 코드 합계
    3. 데이터 로딩: 주가 데이터, 공시 데이터, 지표 데이터
    4. Backtrader 엔진 설정: 초기 자본, 수수료, 슬리피지 등
    5. 전략 실행: PortfolioStrategy 실행
       - 리밸런싱: 정기적으로 포트폴리오 재구성
       - 매수 결정: 지표 조건 > 카테고리 신호 > disclosure 코드 순
       - 매도 결정: 지표 조건 > 카테고리 신호 > disclosure 코드 순
       - 리스크 관리: ATR 기반 스탑로스 및 이익실현
    6. 결과 분석: 수익률, MDD, 샤프 지수, 승률 등
    7. 리포트 생성: 성과 지표 및 거래 내역 리포트
    
    Args:
        input_params: 백테스팅 입력 파라미터 (BacktestInput)
    
    Returns:
        백테스팅 결과 (BacktestOutput)
        - cumulative_return: 누적 수익률
        - total_return: 총 수익률 (%)
        - annualized_return: 연환산 수익률 (%)
        - mdd: 최대 낙폭 (%)
        - sharpe_ratio: 샤프 지수
        - win_rate: 승률 (%)
        - total_trades: 총 거래 횟수
        - trades: 거래 내역 리스트
        - report: 리포트 문자열
        - event_performance: 이벤트별 수익률 분석
    """
    start_time = time.time()
    
    print("="*60)
    print("🚀 포트폴리오 백테스팅 시작")
    print("="*60)
    
    # 1단계: 데이터 로더 초기화
    loader = DataLoader(input_params)
    
    # 2단계: 유니버스 종목 조회
    print("\n📊 [1/5] 유니버스 종목 조회 중...")
    universe_symbols = loader.get_universe_symbols()
    print(f"   ✅ 총 {len(universe_symbols)}개 종목 발견")
    
    if not universe_symbols:
        print("❌ 오류: 조회된 종목이 없습니다.")
        return BacktestOutput()
    
    # 3단계: 종목 필터링 및 선택
    print("\n🔍 [2/5] 종목 필터링 중...")
    selected_symbols: List[str] = universe_symbols

    # (A) 정렬 기준에 따른 스크리닝
    if input_params.sort_by in ["momentum", "market_cap", "event_type", "disclosure"]:
        symbol_to_score = {}
        
        for sym in universe_symbols:
            try:
                if input_params.sort_by == "momentum":
                    score = loader.get_stock_momentum_score(
                        sym,
                        input_params.start_date,
                        input_params.end_date,
                        lookback_days=20
                    )
                elif input_params.sort_by == "market_cap":
                    score = loader.get_stock_market_cap_score(
                        sym,
                        input_params.start_date,
                        input_params.end_date
                    )
                elif input_params.sort_by == "event_type":
                    score = loader.get_event_type_score(
                        sym,
                        input_params.start_date,
                        input_params.end_date
                    )
                elif input_params.sort_by == "disclosure":
                    # disclosure 코드 기반 점수 (disclosure 코드 합계)
                    df_disclosure = loader.get_dart_disclosure_data(
                        symbol=sym,
                        start_date=input_params.start_date,
                        end_date=input_params.end_date
                    )
                    if not df_disclosure.empty and 'disclosure' in df_disclosure.columns:
                        score = float(df_disclosure['disclosure'].sum())
                    else:
                        score = 0.0
                else:
                    score = 0.0
                
                symbol_to_score[sym] = score
            except Exception as e:
                symbol_to_score[sym] = 0.0
        
        # filter_type / filter_percent / filter_value 적용 + 정렬
        screened = _apply_metric_filter_and_sort(
            symbol_to_score,
            filter_type=input_params.filter_type,
            filter_percent=input_params.filter_percent,
            filter_value=input_params.filter_value,
            sort_ascending=input_params.sort_ascending,
        )
        selected_symbols = screened if screened else list(symbol_to_score.keys())

        # 정렬 기준에 따라 정렬
        selected_symbols.sort(
            key=lambda s: symbol_to_score.get(s, 0.0),
            reverse=not input_params.sort_ascending,
        )

        print(
            f"   ✅ {input_params.sort_by} 스크리닝 완료 | "
            f"filter_type={input_params.filter_type}, sort={input_params.sort_by}"
        )
    elif input_params.sort_by == "sentiment_score":
        # 레거시: 감성점수 기반 (disclosure 코드 기반으로 계산)
        sem = asyncio.Semaphore(max(1, int(input_params.screening_concurrency)))

        async def _score_one(sym: str) -> Tuple[str, float]:
            async with sem:
                score = await loader.get_sentiment_screening_score(
                    sym,
                    input_params.start_date,
                    input_params.end_date,
                    lookback_days=int(input_params.sentiment_screening_days),
                )
                return sym, score

        scored = await asyncio.gather(*[_score_one(s) for s in universe_symbols])
        symbol_to_score = {s: v for s, v in scored}

        # filter_type / filter_percent / filter_value 적용 + 정렬
        screened = _apply_metric_filter_and_sort(
            symbol_to_score,
            filter_type=input_params.filter_type,
            filter_percent=input_params.filter_percent,
            filter_value=input_params.filter_value,
            sort_ascending=input_params.sort_ascending,
        )
        selected_symbols = screened if screened else list(symbol_to_score.keys())

        # 항상 감성점수 기준 정렬은 유지(필터가 없더라도)
        selected_symbols.sort(
            key=lambda s: symbol_to_score.get(s, 0.0),
            reverse=not input_params.sort_ascending,
        )

        print(
            f"   ✅ 감성 스크리닝 완료 | lookback={input_params.sentiment_screening_days}d, "
            f"filter_type={input_params.filter_type}, sort={input_params.sort_by}"
        )

    # (B) TODO(추후): 펀더멘탈/기타 팩터 스크리닝을 여기 동일한 방식으로 추가
    elif input_params.sort_by == "fundamental":
        sem = asyncio.Semaphore(max(1, int(input_params.screening_concurrency)))

        async def _score_one(sym: str) -> Tuple[str, float]:
            async with sem:
                score = await loader.get_fundamental_screening_score(
                    sym,
                    input_params.start_date,
                    input_params.end_date,
                    metric="per",
                    lookback_days=int(getattr(input_params, "sentiment_screening_days", 30)),
                )
                return sym, score

        scored = await asyncio.gather(*[_score_one(s) for s in universe_symbols])
        symbol_to_score = {s: v for s, v in scored}

        screened = _apply_metric_filter_and_sort(
            symbol_to_score,
            filter_type=input_params.filter_type,
            filter_percent=input_params.filter_percent,
            filter_value=input_params.filter_value,
            sort_ascending=input_params.sort_ascending,
        )
        selected_symbols = screened if screened else list(symbol_to_score.keys())
        selected_symbols.sort(
            key=lambda s: symbol_to_score.get(s, 0.0),
            reverse=not input_params.sort_ascending,
        )

        print(
            f"   ✅ 펀더멘털 스크리닝 완료 | metric=PER(synth), "
            f"filter_type={input_params.filter_type}, sort={input_params.sort_by}"
        )

    # (C) 최종 유니버스 컷(백테스트에서 실제로 로딩/대상으로 삼을 종목 수)
    selected_symbols = selected_symbols[: input_params.max_portfolio_size]
    print(f"   ✅ 최종 {len(selected_symbols)}개 종목 선택됨 (max_portfolio_size={input_params.max_portfolio_size})")
    
    # 4단계: 주가 데이터, 공시 데이터, 지표 데이터 로드
    print("\n📥 [3/5] 데이터 로딩 중...")
    price_data = {}
    sentiment_data = {}
    indicator_data = {}
    
    for symbol in selected_symbols:
        # 주가 데이터 조회
        df_price = loader.get_stock_price_data(
            symbol, 
            input_params.start_date, 
            input_params.end_date
        )
        
        if not df_price.empty:
            price_data[symbol] = df_price
            
            # DART 공시 또는 뉴스 감성 데이터 조회
            if input_params.use_dart_disclosure:
                # score_table_dart_idc에서 공시 데이터 조회
                df_sentiment = loader.get_dart_disclosure_data(
                    symbol=symbol,
                    start_date=input_params.start_date,
                    end_date=input_params.end_date
                )
                
                if df_sentiment.empty:
                    print(f"   ⚠️  {symbol}: DART 공시 데이터 없음")
                
                # 지표 데이터 조회 (event_indicator_conditions가 설정된 경우)
                if input_params.event_indicator_conditions:
                    # 조회할 report_type 목록 추출
                    report_types = list(set([
                        cond.get("report_type") 
                        for cond in input_params.event_indicator_conditions 
                        if cond.get("report_type")
                    ]))
                    
                    # 종목명 조회 (선택사항)
                    corp_name = loader.get_corp_name_from_symbol(symbol)
                    
                    df_indicator = loader.get_dart_indicator_data(
                        symbol=symbol,
                        corp_name=corp_name,
                        start_date=input_params.start_date,
                        end_date=input_params.end_date,
                        report_types=report_types if report_types else None
                    )
                    
                    if not df_indicator.empty:
                        indicator_data[symbol] = df_indicator
                        print(f"   ✅ {symbol} {corp_name}: 지표 데이터 {len(df_indicator)}건 로드")
            else:
                # 레거시: 뉴스 감성 데이터 조회
                df_sentiment = await loader.get_news_sentiment_data(
                    symbol, 
                    input_params.start_date, 
                    input_params.end_date
                )
                            
            sentiment_data[symbol] = df_sentiment
    
    print(f"   ✅ {len(price_data)}개 종목 데이터 로드 완료")
    if indicator_data:
        print(f"   ✅ {len(indicator_data)}개 종목 지표 데이터 로드 완료")
    
    if not price_data:
        print("❌ 오류: 로드된 주가 데이터가 없습니다.")
        return BacktestOutput()
    
    # 5단계: Backtrader 설정
    print("\n⚙️  [4/5] Backtrader 엔진 설정 중...")
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(input_params.initial_cash)
    cerebro.broker.setcommission(commission=input_params.commission_rate)
    cerebro.broker.set_slippage_perc(perc=input_params.slippage_rate)
    
    # EventDataFeed 정의 (감성 점수와 disclosure 코드를 포함하는 커스텀 피드)
    class EventDataFeed(bt.feeds.PandasData):
        """
        이벤트 데이터 피드
        
        주가 데이터에 감성 점수와 disclosure 코드를 추가한 커스텀 피드입니다.
        backtrader_test.py의 EventDataFeed와 동일한 구조입니다.
        """
        lines = ('disclosure', 'news_sentiment',)
        params = (
            ('datetime', None),
            ('open', 'open'),
            ('high', 'high'),
            ('low', 'low'),
            ('close', 'close'),
            ('volume', 'volume'),
            ('openinterest', None),
            ('disclosure', 'disclosure'),
            ('news_sentiment', 'news_sentiment'),
        )
    
    # 데이터 피드 추가
    datafeeds = []
    valid_symbols = []
    
    for symbol in selected_symbols:
        if symbol in price_data:
            df = price_data[symbol].copy()
            if not df.empty:
                # DART 공시 데이터 병합
                if symbol in sentiment_data and not sentiment_data[symbol].empty:
                    df_sent = sentiment_data[symbol].copy()
                    # 주가 데이터와 공시 데이터를 날짜 기준으로 병합
                    # category 컬럼도 포함
                    cols_to_join = ['disclosure']
                    if 'category' in df_sent.columns:
                        cols_to_join.append('category')
                    df = df.join(df_sent[cols_to_join], how='left')
                    df['disclosure'] = df['disclosure'].fillna(0).astype(int)
                else:
                    # 공시 데이터가 없으면 기본값 설정
                    df['disclosure'] = 0
                
                # backtrader 호환을 위해 news_sentiment 컬럼 추가 (사용하지 않음)
                df['news_sentiment'] = 0.0
                
                # EventDataFeed 생성 및 추가
                datafeed = EventDataFeed(dataname=df)
                cerebro.adddata(datafeed)
                datafeeds.append(datafeed)
                valid_symbols.append(symbol)
    
    print(f"   ✅ {len(valid_symbols)}개 데이터 피드 추가 완료")
    
    # 6단계: 전략 추가
    rebalancing_days = {
        'daily': 1,
        'weekly': 7,
        'monthly': 30,
        'quarterly': 90
    }
    period = rebalancing_days.get(input_params.rebalancing_period, 30)
    
    cerebro.addstrategy(
        PortfolioStrategy,
        selected_symbols=valid_symbols,
        sentiment_data=sentiment_data,
        use_dart=input_params.use_dart_disclosure,
        category_signals=input_params.category_signals,
        event_signals=input_params.event_signals,
        indicator_data=indicator_data,
        event_indicator_conditions=input_params.event_indicator_conditions,
        rebalancing_period=period,
        max_positions=input_params.max_positions
    )
    
    # 7단계: Analyzer 추가 (성과 분석용)
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
    
    # 8단계: 백테스팅 실행
    print("\n🚀 [5/5] 백테스팅 실행 중...")
    print(f"   초기 자본: {input_params.initial_cash:,.0f}원")
    
    results = cerebro.run()
    
    # 9단계: 결과 추출
    print("\n📊 결과 분석 중...")
    strat = results[0]
    
    # Analyzer 결과 추출
    returns_analyzer = strat.analyzers.returns.get_analysis()
    sharpe_analyzer = strat.analyzers.sharpe.get_analysis()
    trade_analyzer = strat.analyzers.trade.get_analysis()
    drawdown_analyzer = strat.analyzers.drawdown.get_analysis()
    
    # Output 생성
    output = BacktestOutput()
    output.cumulative_return = returns_analyzer.get('rtot', 0.0)
    output.total_return = returns_analyzer.get('rtot', 0.0) * 100
    output.annualized_return = returns_analyzer.get('rnorm100', 0.0)
    output.mdd = abs(drawdown_analyzer.get('max', {}).get('drawdown', 0.0)) * 100
    output.sharpe_ratio = sharpe_analyzer.get('sharperatio', 0.0) or 0.0
    
    if trade_analyzer.total.closed:
        output.total_trades = trade_analyzer.total.closed
        output.win_rate = (trade_analyzer.won.total / trade_analyzer.total.closed) * 100
        output.total_profit = trade_analyzer.won.pnl.total
        output.total_loss = abs(trade_analyzer.lost.pnl.total)
    
    output.trades = strat.trades_log
    
    # 이벤트별 수익률 분석
    output.event_performance = analyze_event_performance(
        strat.trades_log, 
        sentiment_data, 
        price_data
    )
    
    # 실행 시간 측정
    elapsed_time = time.time() - start_time
    
    # 리포트 생성
    output.report = generate_report(output, input_params, elapsed_time)
    
    print(f"\n⏱️  실행 시간: {elapsed_time:.2f}초")
    print("="*60)
    
    return output


def analyze_event_performance(
    trades: List[Dict], 
    sentiment_data: Dict[str, pd.DataFrame], 
    price_data: Dict[str, pd.DataFrame]
) -> Dict[str, Dict]:
    """
    이벤트 타입별 수익률 분석
    
    각 이벤트 타입(earnings, share_buyback, general 등)별로
    거래 성과를 분석하여 어떤 이벤트가 가장 수익성이 좋은지 파악합니다.
    
    Args:
        trades: 거래 내역 리스트
        sentiment_data: 종목별 감성 데이터
        price_data: 종목별 주가 데이터
    
    Returns:
        이벤트 타입별 통계 딕셔너리
    """
    event_stats = {}
    
    # 거래별 이벤트 타입 추적
    for trade in trades:
        symbol = trade['symbol']
        date = trade['date']
        
        if symbol not in sentiment_data or sentiment_data[symbol].empty:
            continue
        
        # 해당 날짜의 이벤트 타입 찾기
        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
        sentiment_df = sentiment_data[symbol]
        
        if date_str in sentiment_df.index.strftime('%Y-%m-%d').values:
            event_type = sentiment_df.loc[date_str, 'event_type']
        else:
            # 가장 가까운 이전 날짜
            try:
                before_dates = sentiment_df.loc[:date_str]
                event_type = before_dates['event_type'].iloc[-1] if len(before_dates) > 0 else 'general'
            except:
                event_type = 'general'
        
        if event_type not in event_stats:
            event_stats[event_type] = {
                'count': 0,
                'total_profit': 0.0,
                'total_loss': 0.0,
                'win_count': 0,
                'loss_count': 0
            }
        
        event_stats[event_type]['count'] += 1
    
    return event_stats


def generate_report(
    output: BacktestOutput, 
    input_params: BacktestInput, 
    elapsed_time: float
) -> str:
    """
    백테스팅 결과 리포트 생성
    
    성과 지표, 거래 내역, 이벤트별 분석 등을 포함한 리포트를 생성합니다.
    
    Args:
        output: 백테스팅 결과
        input_params: 입력 파라미터
        elapsed_time: 실행 시간
    
    Returns:
        리포트 문자열
    """
    report = f"""
# 백테스팅 결과 리포트

## 실행 정보
- 실행 시간: {elapsed_time:.2f}초
- 투자 기간: {input_params.start_date} ~ {input_params.end_date}
- 초기 자본: {input_params.initial_cash:,.0f}원
- 리밸런싱 주기: {input_params.rebalancing_period}
- 최대 보유 종목 수: {input_params.max_positions}

## 성과 지표
- 누적 수익률: {output.total_return:.2f}%
- 연환산 수익률: {output.annualized_return:.2f}%
- 최대 낙폭 (MDD): {output.mdd:.2f}%
- 샤프 지수: {output.sharpe_ratio:.2f}
- 승률: {output.win_rate:.2f}%
- 총 거래 횟수: {output.total_trades}

## 손익 분석
- 총 수익: {output.total_profit:,.0f}원
- 총 손실: {output.total_loss:,.0f}원
- 순 손익: {output.total_profit + output.total_loss:,.0f}원

## 이벤트별 성과
"""
    
    for event_type, stats in output.event_performance.items():
        if stats['count'] > 0:
            win_rate = (stats['win_count'] / stats['count']) * 100 if stats['count'] > 0 else 0
            report += f"""
### {event_type}
- 거래 횟수: {stats['count']}
- 승률: {win_rate:.2f}%
- 총 수익: {stats['total_profit']:,.0f}원
- 총 손실: {stats['total_loss']:,.0f}원
"""
    
    report += f"""
## 주요 거래 내역
총 {len(output.trades)}건의 거래가 발생했습니다.

### 최근 20건 거래
"""
    
    for trade in output.trades[-20:]:
        report += f"- {trade['date']} | {trade['symbol']} | {trade['action']} | {trade['size']}주 | {trade['price']:,.0f}원 | {trade['reason']}\n"
    
    return report


# ============================================================
# 메인 실행
# ============================================================
if __name__ == "__main__":
    """
    메인 실행 함수
    
    백테스팅을 실행하고 결과를 출력합니다.
    """
    # Input 설정 - 카테고리별 매매 신호 예시
    input_params = BacktestInput(
        # 특정 종목 리스트 지정 (종목코드 또는 회사명)
        # target_symbols=["347860", "035720", "005930", "000660", "035420"],  # 카카오, 삼성전자, SK하이닉스, NAVER
        target_corp_names=["카카오", "이스트소프트", "KR모터스"],  # 또는 회사명으로 지정
        
        start_date="2023-01-01",
        end_date="2025-12-31",
        initial_cash=100_000_000,
        rebalancing_period="monthly",
        max_positions=5,
        max_portfolio_size=10,
        sort_by="disclosure",  # disclosure 코드 기반 정렬
        use_dart_disclosure=True,  # DART 공시 사용
        
        # 이벤트별 지표 조건 설정 (score_table_dart_idc 기반)
        # 사용자가 직접 어떤 공시정보의 지표가 얼마를 넘으면 매수/매도할지 설정
        event_indicator_conditions=[
            {
                "report_type": "유상증자 결정",
                "idc_nm": "희석률",
                "action": "BUY",  # 조건 만족 시 매수
                "condition": {
                    "min": 0.0,
                    "max": 0.3,
                    "operator": "between"  # 0.0~0.3 사이일 때만 매수
                },
                "delay_days": 0
            },
            {
                "report_type": "감자 결정",
                "idc_nm": "감자비율",
                "action": "SELL",  # 조건 만족 시 매도
                "condition": {
                    "min": 0.1,
                    "max": None,
                    "operator": ">="  # 0.1 이상일 때 매도
                },
                "delay_days": 0
            },
            {
                "report_type": "회사합병 결정",
                "idc_nm": "합병비율",
                "action": "BUY",
                "condition": {
                    "min": 0.5,
                    "max": 1.5,
                    "operator": "between"  # 합병비율이 0.5~1.5 사이일 때 매수
                },
                "delay_days": 1  # 공시일 다음날 매수
            },
        ],
    )
    
    # 백테스팅 실행
    output = asyncio.run(run_backtest(input_params))
    
    # 결과 출력
    print("\n" + "="*60)
    print("📈 백테스팅 결과 요약")
    print("="*60)
    print(f"누적 수익률: {output.total_return:.2f}%")
    print(f"연환산 수익률: {output.annualized_return:.2f}%")
    print(f"최대 낙폭 (MDD): {output.mdd:.2f}%")
    print(f"샤프 지수: {output.sharpe_ratio:.2f}")
    print(f"승률: {output.win_rate:.2f}%")
    print(f"총 거래 횟수: {output.total_trades}")
    print(f"총 손익: {output.total_profit + output.total_loss:,.0f}원")
    
    print(f"\n거래 내역 (최근 10건):")
    for trade in output.trades[-10:]:
        print(f"  {trade['date']} | {trade['symbol']} | {trade['action']} | "
              f"{trade['size']}주 | {trade['price']:,.0f}원 | {trade['reason']}")
    
    # 리포트 저장 (선택사항)
    with open('backtest_report.md', 'w', encoding='utf-8') as f:
        f.write(output.report)
