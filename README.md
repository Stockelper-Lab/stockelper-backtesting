# stockelper-backtesting

`stockelper-llm`(LangGraph/LangChain 기반)에서 **백테스팅 도메인(잡 생성/조회 + 워커)**을 분리한 로컬 실행 서비스입니다.

## 실행

- **Python**: 3.12+
- **uv** 사용

```bash
cd /Users/oldman/Library/CloudStorage/OneDrive-개인/001_Documents/001_TelePIX/000_workspace/03_PseudoLab/Stockelper-Lab/stockelper-backtesting
uv sync --dev
uv run python src/main.py
```

- 기본 포트: **21011**
- 포트 변경: `PORT` 환경변수로 제어

## 주요 엔드포인트

### Backtesting (기존 호환)

- `POST /backtesting/jobs`
- `GET /backtesting/jobs/{job_id}?user_id=...`

### Backtesting (아키텍처 문서 계약 `/api/backtesting/*`)

- `POST /api/backtesting/execute`
- `GET /api/backtesting/{job_id}/status?user_id=...`
- `GET /api/backtesting/{job_id}/result?user_id=...`

### Notifications

- `GET /notifications?user_id=...`
- `POST /notifications/{notification_id}/read`

## 워커(선택)

현재는 `src/backtesting/worker.py`가 **MVP 시뮬레이션**으로 동작합니다.

```bash
uv run python src/backtesting/worker.py
```

관련 환경변수:
- `BACKTEST_SIMULATE_SECONDS` (기본 300)
- `BACKTEST_WORKER_POLL_SECONDS` (기본 5)

## 필수 환경변수

- `ASYNC_DATABASE_URL`: `backtest_jobs`, `backtest_results`, `notifications` 테이블이 생성/조회될 DB

> 이 서비스는 테이블이 없으면 기동 시점이 아니라 **요청 처리 시점에** `metadata.create_all()`로 생성합니다(멱등).


