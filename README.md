# Stockelper Backtesting Service

백테스팅 작업 생성, 실행 및 결과 적재를 위한 FastAPI 기반 서비스입니다.

핵심 요구사항(운영):
- LLM 서버에서 요청을 받으면 **즉시 `stockelper_web.public.backtesting`에 placeholder row 생성**
- 백테스트 실행은 워커가 처리(시간 소요)
- 완료 시 **결과 파일 경로 + 상태(completed/failed)로 row 업데이트**

## 🚀 주요 기능

- 백테스팅 작업 생성 및 관리
- 비동기 워커 기반 작업 처리
- PostgreSQL 기반 작업 큐 시스템
- 사용자 알림 관리
- Docker 기반 배포

## 📋 기술 스택

- Python 3.12+
- FastAPI 0.111
- PostgreSQL (asyncpg)
- SQLAlchemy 2.0+ (async)
- Docker & Docker Compose
- uv (패키지 관리자)

## 🔌 API 엔드포인트

### 기본
- `GET /` - 루트 엔드포인트
- `GET /health` - 헬스 체크

### 백테스팅 (Legacy)
- `POST /backtesting/jobs` - 작업 생성
- `GET /backtesting/jobs/{job_id}?user_id=...` - 작업 조회

### 백테스팅 (아키텍처 호환)
- `POST /api/backtesting/execute` - 백테스트 실행
- `GET /api/backtesting/{job_id}/status?user_id=...` - 상태 조회
- `GET /api/backtesting/{job_id}/result?user_id=...` - 결과 조회

### 알림
- `GET /notifications?user_id=...&unread_only=false&limit=50` - 알림 목록
- `POST /notifications/{notification_id}/read` - 읽음 처리

## 🗄️ 데이터베이스 스키마(권장)

### public.backtesting (stockelper_web)
- id (text, PK)
- job_id (text, unique)
- user_id (int)
- request_source (text, default 'llm')
- status (pending|in_progress|completed|failed)
- input_json (JSONB)
- output_json (JSONB) : 가벼운 요약(핵심 지표)
- result_file_path (text) : 결과 JSON 파일 경로
- report_file_path (text) : 리포트(Markdown) 파일 경로
- error_message (text)
- created_at, updated_at, started_at, completed_at, elapsed_seconds

DDL 예시는 `migrations/001_create_public_backtesting.sql` 참고

## ⚙️ 환경 변수

```bash
# 결과 적재 DB (필수): stockelper_web
# 예) postgresql://postgres:postgres@54.180.25.182:5432/stockelper_web
DATABASE_URL=postgresql://user:pass@host:5432/stockelper_web

# (선택) schema/table
STOCKELPER_WEB_SCHEMA=public
STOCKELPER_BACKTESTING_TABLE=backtesting

# 시장데이터 DB(백테스트 입력 데이터): portfolio_backtest.DataLoader가 사용
DB_USER=stockelper
DB_PASSWORD=
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres

# 서버 설정
HOST=0.0.0.0
PORT=21007
DEBUG=false

# 워커 설정
BACKTEST_WORKER_POLL_SECONDS=5   # 폴링 간격
BACKTEST_RESULTS_DIR=outputs/backtesting_results  # 결과 파일 저장 위치

# (선택) LLM 해석 자동화
# - 백테스트 완료 직후, LLM 서버에 해석 생성을 요청합니다.
STOCKELPER_LLM_URL=http://54.180.25.182:21009
BACKTEST_LLM_TRIGGER_TIMEOUT=10
```

## 🚀 빠른 시작

### 로컬 실행

```bash
# 의존성 설치
uv sync --dev

# 서버 실행
uv run python src/main.py
```

### Docker 실행

```bash
# 빌드 및 실행
docker-compose up -d

# 로그 확인
docker-compose logs -f backtesting-server
docker-compose logs -f backtest-worker
```

## 🔄 워커 동작

### 폴링 기반 작업 처리
1. `BACKTEST_WORKER_POLL_SECONDS`마다 DB 폴링
2. `SELECT ... FOR UPDATE SKIP LOCKED`로 작업 예약
3. 상태를 `pending` → `in_progress`로 변경
4. `portfolio_backtest.run_backtest()` 실행
5. 결과(JSON/Markdown) 파일 저장
6. 상태를 `completed`(또는 `failed`)로 변경 + 파일 경로 업데이트

### (선택) OpenAI Agents SDK 기반 오케스트레이션
- `BACKTEST_USE_AGENTS=true`를 설정하면 워커가 아래 단계를 추가로 수행합니다:
  - 입력 파싱(LLM) → 데이터 사전검증(preflight) → 실행 → 결과 감사(audit) → 리포트(LLM) → DB 적재(analysis_* 포함)
- 필요 환경변수:
  - `OPENAI_API_KEY`
  - (선택) `BACKTEST_AGENT_MODEL_PARSE/ADJUST/AUDIT/REPORT`, `BACKTEST_AGENT_MAX_RETRIES`

### 별도 실행

```bash
# 워커만 실행
# src 레이아웃이므로 -m 실행을 권장합니다.
PYTHONPATH=src uv run python -m backtesting.worker
```

## 🐳 Docker 구성

### 서비스
- **backtesting-server** (포트: 21007)
  - FastAPI 애플리케이션
  - 헬스체크: `/health`
- **backtest-worker**
  - 백그라운드 작업 처리
  - 서버 헬스체크 대기 후 시작

### 네트워크
- `stockelper` 브리지 네트워크

## 📝 사용 예시

### 작업 생성

```bash
curl -X POST http://localhost:21007/api/backtesting/execute   -H "Content-Type: application/json"   -d '{
    "user_id": 1,
    "stock_ticker": "005930",
    "strategy_type": "momentum",
    "parameters": {...}
  }'
```

### 상태 조회

```bash
curl http://localhost:21007/api/backtesting/{job_id}/status?user_id=1
```

### 결과 조회

```bash
curl http://localhost:21007/api/backtesting/{job_id}/result?user_id=1
```

## 🔒 보안

- 모든 쿼리 `user_id`로 필터링 (다중 테넌트 안전성)
- 환경 변수로 자격증명 관리
- `.env` 파일 커밋 금지

## 📄 라이선스

MIT License
