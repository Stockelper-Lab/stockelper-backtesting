# Stockelper Backtesting Service

백테스팅 작업 생성, 실행 및 결과 조회를 위한 FastAPI 기반 서비스입니다.

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

## 🗄️ 데이터베이스 스키마

### backtest_jobs
- id, user_id, stock_ticker, strategy_type
- status (pending|in_progress|completed|failed)
- input_json (JSONB), error_message
- created_at, started_at, completed_at

### backtest_results
- id, job_id, user_id, stock_ticker, strategy_type
- results_json (JSONB)
- generated_at

### notifications
- id, user_id, type, title, message
- data (JSONB), is_read, read_at
- created_at

## ⚙️ 환경 변수

```bash
# 데이터베이스 (필수)
ASYNC_DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/db
# 또는
DATABASE_URL=postgresql://user:pass@host:5432/db  # 자동 변환

# 서버 설정
HOST=0.0.0.0
PORT=21011
DEBUG=false

# 워커 설정
BACKTEST_SIMULATE_SECONDS=300    # MVP 시뮬레이션 시간
BACKTEST_WORKER_POLL_SECONDS=5   # 폴링 간격
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
4. MVP 시뮬레이션 실행 (실제 백테스트 로직 예정)
5. 결과 저장 및 상태를 `completed`로 변경
6. 사용자 알림 생성

### 별도 실행

```bash
# 워커만 실행
uv run python src/backtesting/worker.py
```

## 🐳 Docker 구성

### 서비스
- **backtesting-server** (포트: 21011)
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
curl -X POST http://localhost:21011/api/backtesting/execute   -H "Content-Type: application/json"   -d '{
    "user_id": 1,
    "stock_ticker": "005930",
    "strategy_type": "momentum",
    "parameters": {...}
  }'
```

### 상태 조회

```bash
curl http://localhost:21011/api/backtesting/{job_id}/status?user_id=1
```

### 결과 조회

```bash
curl http://localhost:21011/api/backtesting/{job_id}/result?user_id=1
```

## 🔒 보안

- 모든 쿼리 `user_id`로 필터링 (다중 테넌트 안전성)
- 환경 변수로 자격증명 관리
- `.env` 파일 커밋 금지

## 📄 라이선스

MIT License
