## Stockelper Backtesting - OpenAI Agents SDK 기반 설계 문서

- **문서 목적**: 기존 `stockelper-backtesting` 서비스(요청 API + DB 큐 + 워커 + Backtrader 엔진)를 유지하면서, “입력 해석/가드레일/사전검증/재시도/결과 감사/리포트 생성”을 **OpenAI Agents Python SDK(`openai-agents`)** 로 개발하기 위한 설계안을 제시합니다.
- **적용 범위**: `src/backtesting/worker.py`의 처리 흐름을 Agents SDK 기반 파이프라인으로 교체(또는 기능 플래그로 병행 운영).
- **핵심 원칙**: **백테스트 실행/계산은 코드(툴)로 고정**하고, LLM은 **의사결정(해석/계획/검증/서술)**만 수행합니다.

---

## 현재 시스템 요약(현행 구현 기반)

- **요청 등록(API)**: `POST /api/backtesting/execute`
  - `public.backtesting` 테이블에 placeholder row 생성(status=`pending`, input_json 저장)
- **워커(큐 처리)**: `src/backtesting/worker.py`
  - `claim_next_pending_job()`로 pending 1건을 `FOR UPDATE SKIP LOCKED`로 점유 → status=`in_progress`
  - `_resolve_backtest_input(input_json)`로 `BacktestInput` 구성
  - `run_backtest(BacktestInput)` 실행
  - 결과 파일(json/md) 저장 + `mark_job_completed()` 또는 `mark_job_failed()`로 DB 업데이트
- **백테스트 엔진**: `src/backtesting/portfolio_backtest.py`
  - 가격/공시/지표 로딩 → Backtrader 실행 → 결과 산출/리포트 생성

---

## 목표/비목표

### 목표
- **자연어 요청을 안정적으로 구조화(`BacktestInput`)**: 종목/기간/전략/조건을 추출하고 안전한 범위로 보정
- **사전 데이터 점검(Preflight)**: “실행했는데 거래 0건” 같은 낭비를 줄이고, 데이터 부족 시 자동 수정/재시도
- **결과 정합성 감사(Audit)**: 산출물 모순(거래 0인데 수익률 큰 값 등)을 감지하고 자동 재시도/실패 분류
- **운영 가시성(Tracing)**: job 단위로 에이전트 판단과 도구 호출을 트레이스로 남겨 디버깅/품질 개선

### 비목표
- LLM이 직접 수익률/체결/포지션을 계산하는 것(금지)
- “전략을 임의 코드로 생성”하여 실행하는 것(운영 리스크 큼)

---

## 전체 오케스트레이션 구조(권장)

### 설계 선택
- **코드 기반 오케스트레이션(결정적) + LLM은 structured outputs** 형태로만 사용
  - 이유: 백테스팅은 **재현성/안전성/예측가능성**이 중요하며, 비용/지연을 통제해야 함

### Mermaid: End-to-End(워커 내부 파이프라인)

```mermaid
flowchart TD
    A[Claim Job\n(DB: pending -> in_progress)] --> B[Parse Request Agent\n(Structured Output)]
    B --> C[Resolve Targets Tool\n(DB: corp_name/query -> symbols)]
    C --> D[Build BacktestInput\n(Deterministic Merge)]
    D --> E[Guardrails\n(Input/Tool/Output)]
    E -->|pass| F[Preflight Data Check Tool\n(DB: coverage/availability)]
    E -->|fail| X[Fail & Persist\n(DB: failed)]
    F -->|ok| G[Run Backtest Tool\n(run_backtest)]
    F -->|missing| H[Adjust Plan Agent\n(retry plan)]
    H -->|retry| D
    H -->|give up| X
    G --> I[Build Artifacts Tool\n(json/md + summary)]
    I --> J[Audit Result Agent\n(pass/retry/fail)]
    J -->|retry| H
    J -->|ok| K[Report Agent\n(analysis_md/json)]
    K --> L[Persist Completed\n(DB: completed + paths)]
```

---

## 제안 코드 구조(Backtesting 서비스 레포 내)

> 아래는 “최소 침습”으로 기존 구조를 살리는 파일 배치안입니다.

- `src/backtesting/agents/`
  - `schemas.py`: Pydantic 모델(LLM output_type, tool args/result)
  - `tools.py`: `@function_tool`로 감싼 DB/실행/저장 함수들
  - `agents.py`: Agent 정의(파서/조정/감사/리포트)
  - `orchestrator.py`: 워커가 호출하는 메인 파이프라인(코드 기반 상태머신)
  - `guardrails.py`: input/output/tool guardrails
- `src/backtesting/worker.py`
  - 기존 `_process_one()`에서 `run_backtesting_agent_pipeline(job)` 호출로 대체

---

## 에이전트/툴 인터페이스(스키마 계약)

### 공통 컨텍스트(권장)
- **`job_id`**, **`user_id`**, **`raw_payload`**
- **`retry_count`**, **`max_retries`**
- **`preflight_summary`**, **`audit_findings`**

### Pydantic 모델(예시 설계)
- **`ParsedRequest`**: 자연어/레거시 입력 해석 결과(종목/기간/전략/조건)
- **`BacktestPlan`**: 최종 `BacktestInput` 생성에 필요한 계획(또는 `parameters` 초안)
- **`PreflightReport`**: 데이터 커버리지/결측/권장 수정안
- **`AdjustmentPlan`**: 재시도 시 수정할 항목(기간 축소/종목 제외/조건 완화 등)
- **`AuditResult`**: 결과 정합성 판정(pass/retry/fail + 근거)
- **`FinalNarrative`**: 사용자용 요약/주의사항/한계

> 실제 구현에서는 `BacktestInput`의 전체 필드를 그대로 노출하기보다, “지원할 필드 subset”을 명시해 안정성을 높이는 것을 권장합니다.

---

## Tools 설계(함수 도구: 결정적 실행)

LLM이 호출할 수 있는 “안전한 실행 단위”는 아래처럼 함수 도구로 제공합니다.

- **`resolve_symbols`(DB)**  
  - **입력**: `corp_names: list[str] | None`, `query: str | None`  
  - **출력**: `symbols: list[str]`, `mapping_log: dict`
- **`preflight_data_check`(DB)**  
  - **입력**: `backtest_input: dict`  
  - **출력**: 기간 내 가격/공시/지표 존재 여부, 심볼별 커버리지, 권장 보정안
- **`run_backtest`(엔진)**  
  - **입력**: `backtest_input: dict` (`BacktestInput(**dict)`로 변환)  
  - **출력**: `BacktestOutput`(직렬화 가능한 dict로 변환해 반환)
- **`build_artifacts`(파일)**  
  - **입력**: `job_id`, `output_dict`  
  - **출력**: `result_file_path`, `report_file_path`, `output_summary`
- **`persist_completed/persist_failed`(DB)**  
  - **입력**: job_id, status, summary, paths, error_message 등

### Tool guardrails(필수)
- **경로 안전성**: artifact 저장 경로는 `BACKTEST_RESULTS_DIR` 하위만 허용
- **DB 폭주 방지**: preflight에서 전체 유니버스 실행은 기본 차단(또는 강한 제한)
- **민감정보 방지**: 도구 인자/출력에 비밀번호/DSN 등이 섞이면 차단/마스킹

---

## Agents 설계(LLM: structured outputs 중심)

### 1) Parse Request Agent
- **목적**: `input_json`(query/legacy/parameters)을 해석하여 “의도(종목/기간/전략/조건)”를 구조화
- **output_type**: `ParsedRequest`
- **주의**: 확정이 어려운 값은 “추정”으로 두고, 다음 단계(preflight)에서 검증/보정

### 2) Adjust Plan Agent
- **목적**: preflight/audit 실패 시 “재시도 수정안” 생성
- **output_type**: `AdjustmentPlan`
- **전략 예시**
  - 가격 데이터 부족: 기간을 데이터 있는 구간으로 축소
  - 공시 데이터 부족: 공시 기반 조건을 완화(또는 disclosure=0 허용) / use_dart 재검토
  - 거래 0건: 조건 완화 or 리밸런싱 주기 조정 or 대상 종목 확대(단, 상한 적용)

### 3) Audit Result Agent
- **목적**: 산출된 결과가 “논리적으로 말이 되는지” 평가
- **output_type**: `AuditResult`
- **감사 룰(예시)**
  - `total_trades == 0`인데 `abs(total_return) > ε` 이면 의심
  - `mdd`가 0~100 범위를 벗어나면 의심
  - `total_profit - total_loss`와 누적수익률 방향이 크게 불일치하면 의심

### 4) Report Agent
- **목적**: 사용자용 리포트 생성(요약 + 근거 + 한계/주의사항)
- **output_type**: `FinalNarrative`
- **권장 출력**
  - 핵심 지표 5개, 대표 트레이드 3~5개, 리스크/데이터 한계, 재현 방법

---

## Guardrails(Agents SDK 기능 활용)

### 입력 가드레일(InputGuardrail) – 차단형 권장(`run_in_parallel=False`)
- **전체 유니버스 방지**: 대상 종목이 없으면 실패(또는 제한된 후보만 허용)
- **기간 검증**: 날짜 포맷, start/end 역전, 지나치게 긴 기간 제한
- **파라미터 상한**: `max_portfolio_size`, `max_positions`, `screening_concurrency` 상한

### 출력 가드레일(OutputGuardrail)
- `BacktestInput` 생성 결과가 스키마/상한을 벗어나면 차단

### 도구 가드레일(Tool guardrails)
- DB/파일/네트워크 계열 도구에 대해서는 입력/출력 마스킹 및 차단 규칙 적용

---

## Tracing/Observability(운영)

- Agents SDK 트레이싱은 기본 활성화
- 권장 메타데이터:
  - **`workflow_name`**: `"Stockelper Backtesting Agent"`
  - **`group_id`**: `job_id` (job 단위로 묶기)
  - **`metadata`**: `user_id`, `symbols_count`, `date_range`, `retry_count`, `mode(agents|legacy)` 등
- 민감정보:
  - `RunConfig.trace_include_sensitive_data=False` 고려(DSN/환경변수/원문 payload 노출 방지)

---

## 재시도 정책(권장)

- **max_retries**: 2~3회(운영 비용/지연 제한)
- **재시도 트리거**
  - preflight에서 데이터 부족
  - audit에서 “거래 0건 + 조건 과도” 등 개선 가능성이 있을 때
- **즉시 실패**
  - 종목 해석 실패(그리고 전체 유니버스 불허)
  - DB 연결 실패/치명적 예외

---

## 구현 순서(마일스톤)

### M1. 기반 도구/스키마 구축
- `openai-agents` 의존성 추가(uv)
- `schemas.py`(Pydantic output_type) 작성
- DB/실행/저장 기능을 `@function_tool`로 래핑

### M2. 오케스트레이터(코드 기반) 연결
- `worker.py`에서 claim 이후 `orchestrator.run(job)` 호출로 교체
- 성공/실패 DB 업데이트 경로 정리

### M3. 가드레일/프리플라이트/재시도
- input/output/tool guardrails 적용
- preflight 체크 및 adjust 루프 구현

### M4. 감사/리포트 품질 + 운영
- audit 룰 고도화(정합성, 데이터 부족 설명 품질)
- tracing 메타데이터/대시보드 운영 루틴 정리

---

## 테스트 전략(권장)

- **유닛 테스트**
  - `resolve_symbols`, `preflight_data_check` (DB mock 또는 테스트 DB)
  - guardrails(차단 조건)
  - audit 규칙(샘플 결과 dict로 검증)
- **통합 테스트**
  - 실제 DB(스테이징)에서 작은 종목/짧은 기간으로 e2e
  - “데이터 없음” 케이스에서 adjust 루프가 정상 동작하는지

---

## 운영/보안 체크리스트

- **멀티테넌트 안전**: 조회/다운로드는 `job_id + user_id`로만
- **파일 경로 안전**: `BACKTEST_RESULTS_DIR` 하위만 허용
- **비용 통제**: 가드레일 차단형, max_retries 제한, 모델 혼합(파서/감사는 작은 모델)
- **장애 대응**: tool 실패는 실패 사유를 분류해 `error_message`에 남김

---

## 부록: 모델/구성 권장

- **모델 혼합**
  - Parse/Adjust/Audit: 빠른 모델(예: gpt-4.1 mini급)
  - Report: 품질 우선 모델(예: gpt-5.x)
- **환경변수**
  - `OPENAI_DEFAULT_MODEL` 또는 run 단위 `RunConfig(model=...)`
  - `OPENAI_AGENTS_TRACE_INCLUDE_SENSITIVE_DATA=false`(권장)

