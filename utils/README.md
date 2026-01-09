CLI 사용법

cd stockelper-backtesting/utils

# 테이블 목록 조회
uv run python dart_indicator_cli.py list-tables --dart-only

# 미리보기 (100개씩)
uv run python dart_indicator_cli.py preview --limit 100

# 전체 추출 및 DB 적재
uv run python dart_indicator_cli.py extract

# 기존 데이터에 추가
uv run python dart_indicator_cli.py extract --if-exists append