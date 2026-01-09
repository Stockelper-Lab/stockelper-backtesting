FROM python:3.12-slim

WORKDIR /app

# src 레이아웃 import 지원 (worker가 -m/backtesting.* 로 실행될 때 필요)
ENV PYTHONPATH="/app/src"

# PostgreSQL 관련 라이브러리 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    zlib1g-dev \
    libpq-dev \
    curl \
    cmake \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# uv 설치 및 설정
# 참고: https://docs.astral.sh/uv/getting-started/installation/#standalone-installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# 프로젝트 파일 복사 (pyproject.toml 먼저)
COPY pyproject.toml uv.lock* ./

# uv를 사용하여 의존성 설치
RUN uv sync --no-dev

# 소스 코드 복사
COPY . .

# 포트 노출
ENV PORT=21011
EXPOSE 21011

# 애플리케이션 실행 (이미 uv sync로 .venv가 생성되었으므로 직접 실행)
CMD [".venv/bin/python", "src/main.py"]
