from __future__ import annotations

import json
import os
import re
import uuid
from typing import Any, Dict, Optional

import asyncpg

# ============================================================
# Auto init (optional): create/alter table if missing
# ============================================================

def _to_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _auto_init_enabled() -> bool:
    # 기본값: true (초기 구축 단계에서 운영 편의성)
    return _to_bool(os.getenv("STOCKELPER_BACKTESTING_AUTO_INIT_DB"), default=True)


async def ensure_backtesting_table(*, conn: asyncpg.Connection, schema: str, table: str) -> None:
    """public.backtesting 테이블이 없거나 컬럼이 부족하면 보강합니다.

    NOTE:
    - 운영 환경에서는 DB 권한이 제한될 수 있습니다.
      (그 경우 이 함수가 실패하면 insert도 실패하므로, 마이그레이션을 수동 적용해야 합니다.)
    - schema/table 이름은 이미 정규식 검증을 통과한 값이어야 합니다.
    """

    # schema 생성 (public이면 사실상 noop)
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # 테이블 생성 (최신 스키마 기준)
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
          id               text PRIMARY KEY,
          job_id           text NOT NULL UNIQUE,
          user_id          integer NOT NULL,
          request_source   text NOT NULL DEFAULT 'llm',
          status           text NOT NULL,

          input_json       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          output_json      jsonb NOT NULL DEFAULT '{{}}'::jsonb,

          result_file_path text NULL,
          report_file_path text NULL,
          error_message    text NULL,

          analysis_status          text NOT NULL DEFAULT 'pending',
          analysis_md              text NULL,
          analysis_json            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          analysis_model           text NULL,
          analysis_prompt_version  text NULL,
          analysis_error_message   text NULL,
          analysis_started_at      timestamptz NULL,
          analysis_completed_at    timestamptz NULL,
          analysis_elapsed_seconds double precision NULL,

          elapsed_seconds  double precision NULL,
          started_at       timestamptz NULL,
          completed_at     timestamptz NULL,

          created_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # 기존 테이블(구버전) 보강: 컬럼이 없으면 추가
    # - 이미 존재하는 경우에도 IF NOT EXISTS라 안전
    await conn.execute(
        f"""
        ALTER TABLE {schema}.{table}
          ADD COLUMN IF NOT EXISTS request_source   text NOT NULL DEFAULT 'llm',
          ADD COLUMN IF NOT EXISTS status           text NOT NULL DEFAULT 'pending',
          ADD COLUMN IF NOT EXISTS input_json       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          ADD COLUMN IF NOT EXISTS output_json      jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          ADD COLUMN IF NOT EXISTS result_file_path text NULL,
          ADD COLUMN IF NOT EXISTS report_file_path text NULL,
          ADD COLUMN IF NOT EXISTS error_message    text NULL,
          ADD COLUMN IF NOT EXISTS elapsed_seconds  double precision NULL,
          ADD COLUMN IF NOT EXISTS started_at       timestamptz NULL,
          ADD COLUMN IF NOT EXISTS completed_at     timestamptz NULL,
          ADD COLUMN IF NOT EXISTS created_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
          ADD COLUMN IF NOT EXISTS updated_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

          ADD COLUMN IF NOT EXISTS analysis_status          text NOT NULL DEFAULT 'pending',
          ADD COLUMN IF NOT EXISTS analysis_md              text NULL,
          ADD COLUMN IF NOT EXISTS analysis_json            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          ADD COLUMN IF NOT EXISTS analysis_model           text NULL,
          ADD COLUMN IF NOT EXISTS analysis_prompt_version  text NULL,
          ADD COLUMN IF NOT EXISTS analysis_error_message   text NULL,
          ADD COLUMN IF NOT EXISTS analysis_started_at      timestamptz NULL,
          ADD COLUMN IF NOT EXISTS analysis_completed_at    timestamptz NULL,
          ADD COLUMN IF NOT EXISTS analysis_elapsed_seconds double precision NULL
        """
    )

    # 인덱스
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS backtesting_user_created_at_idx ON {schema}.{table} (user_id, created_at DESC)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS backtesting_status_created_at_idx ON {schema}.{table} (status, created_at DESC)"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS backtesting_analysis_status_created_at_idx ON {schema}.{table} (analysis_status, created_at DESC)"
    )

# ============================================================
# stockelper_web DB helpers (asyncpg)
# ============================================================

_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _get_schema() -> str:
    # 요구사항: 기본은 public
    schema = os.getenv("STOCKELPER_WEB_SCHEMA", "public")
    if not _SCHEMA_NAME_RE.match(schema):
        raise ValueError(f"Invalid STOCKELPER_WEB_SCHEMA: {schema!r}")
    return schema


def _get_table() -> str:
    # 요구사항: public.backtesting
    table = os.getenv("STOCKELPER_BACKTESTING_TABLE", "backtesting")
    if not _TABLE_NAME_RE.match(table):
        raise ValueError(f"Invalid STOCKELPER_BACKTESTING_TABLE: {table!r}")
    return table


def _get_database_url_for_asyncpg() -> str:
    """DATABASE_URL / ASYNC_DATABASE_URL을 asyncpg DSN으로 변환해 반환."""
    url = os.getenv("DATABASE_URL") or os.getenv("ASYNC_DATABASE_URL")
    if not url:
        raise RuntimeError("Missing DATABASE_URL or ASYNC_DATABASE_URL")

    # asyncpg는 postgresql+asyncpg 스킴을 이해하지 못함
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return url


async def insert_backtesting_job(
    *,
    user_id: int,
    input_json: Dict[str, Any],
    request_source: str = "llm",
) -> Dict[str, Any]:
    """요청 수신 즉시 placeholder row를 생성합니다."""

    rec_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        # (옵션) 테이블 자동 생성/보강
        if _auto_init_enabled():
            await ensure_backtesting_table(conn=conn, schema=schema, table=table)

        # created_at은 DEFAULT now() 를 권장. (없다면 아래에서 직접 넣도록 DDL을 맞춰주세요)
        await conn.execute(
            f"""
            INSERT INTO {schema}.{table}
              (id, job_id, user_id, request_source, status, input_json, output_json, updated_at)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
            """,
            rec_id,
            job_id,
            int(user_id),
            str(request_source),
            "pending",
            json.dumps(input_json, ensure_ascii=False, default=str),
            json.dumps({}, ensure_ascii=False),
        )
    finally:
        await conn.close()

    return {"id": rec_id, "job_id": job_id, "status": "pending"}


async def get_backtesting_job(*, job_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    """job_id + user_id로 단건 조회(멀티테넌트 안전)."""

    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            f"""
            SELECT
              id,
              job_id,
              user_id,
              status,
              request_source,
              COALESCE(input_json, '{{}}'::jsonb)  AS input_json,
              COALESCE(output_json, '{{}}'::jsonb) AS output_json,
              result_file_path,
              report_file_path,
              error_message,
              analysis_status,
              analysis_md,
              COALESCE(analysis_json, '{{}}'::jsonb) AS analysis_json,
              analysis_model,
              analysis_prompt_version,
              analysis_error_message,
              analysis_started_at,
              analysis_completed_at,
              analysis_elapsed_seconds,
              created_at,
              updated_at,
              started_at,
              completed_at,
              elapsed_seconds
            FROM {schema}.{table}
            WHERE job_id = $1 AND user_id = $2
            """,
            str(job_id),
            int(user_id),
        )
        if not row:
            return None

        # asyncpg.Record -> dict
        data = dict(row)
        
        # jsonb 필드: asyncpg가 문자열로 반환할 수 있으므로 dict로 파싱
        for k in ("input_json", "output_json", "analysis_json"):
            v = data.get(k)
            if v is None:
                data[k] = {}
            elif isinstance(v, str):
                try:
                    data[k] = json.loads(v)
                except Exception:
                    data[k] = {}
            elif not isinstance(v, dict):
                data[k] = {}
        
        # datetime -> iso string (FastAPI가 자동 인코딩해주기도 하지만 안정적으로 문자열화)
        for k in (
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
            "analysis_started_at",
            "analysis_completed_at",
        ):
            v = data.get(k)
            if v is not None:
                data[k] = v.isoformat()
        return data
    finally:
        await conn.close()


async def claim_next_pending_job() -> Optional[Dict[str, Any]]:
    """워커용: pending 1건을 락으로 점유하고 in_progress로 전환 후 반환."""

    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            f"""
            WITH picked AS (
              SELECT id
              FROM {schema}.{table}
              WHERE status = 'pending'
              ORDER BY created_at ASC
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            UPDATE {schema}.{table} b
            SET status = 'in_progress',
                started_at = COALESCE(b.started_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            FROM picked
            WHERE b.id = picked.id
            RETURNING
              b.id,
              b.job_id,
              b.user_id,
              COALESCE(b.input_json, '{{}}'::jsonb) AS input_json
            """,
        )
        if not row:
            return None
        return dict(row)
    finally:
        await conn.close()


async def mark_job_completed(
    *,
    job_id: str,
    output_json: Dict[str, Any],
    result_file_path: Optional[str],
    report_file_path: Optional[str],
    elapsed_seconds: Optional[float],
) -> bool:
    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        res = await conn.execute(
            f"""
            UPDATE {schema}.{table}
            SET status = 'completed',
                output_json = $2,
                result_file_path = $3,
                report_file_path = $4,
                error_message = NULL,
                elapsed_seconds = $5,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = $1
            """,
            str(job_id),
            json.dumps(output_json, ensure_ascii=False, default=str),
            result_file_path,
            report_file_path,
            elapsed_seconds,
        )
        return str(res).strip().endswith("1")
    finally:
        await conn.close()


async def mark_job_completed_with_analysis(
    *,
    job_id: str,
    output_json: Dict[str, Any],
    result_file_path: Optional[str],
    report_file_path: Optional[str],
    elapsed_seconds: Optional[float],
    analysis_md: Optional[str],
    analysis_json: Optional[Dict[str, Any]],
    analysis_model: Optional[str],
    analysis_prompt_version: Optional[str],
    analysis_elapsed_seconds: Optional[float],
) -> bool:
    """completed 업데이트 + analysis_* 컬럼을 함께 적재합니다.

    - analysis_md / analysis_json이 모두 비어 있으면 기존 `mark_job_completed`와 동일하게 동작합니다.
    - analysis 컬럼은 `public.backtesting` 권장 스키마에 포함되어 있어야 합니다.
    """

    has_analysis = bool((analysis_md and str(analysis_md).strip()) or (analysis_json and len(analysis_json) > 0))
    if not has_analysis:
        return await mark_job_completed(
            job_id=job_id,
            output_json=output_json,
            result_file_path=result_file_path,
            report_file_path=report_file_path,
            elapsed_seconds=elapsed_seconds,
        )

    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        res = await conn.execute(
            f"""
            UPDATE {schema}.{table}
            SET status = 'completed',
                output_json = $2,
                result_file_path = $3,
                report_file_path = $4,
                error_message = NULL,
                elapsed_seconds = $5,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,

                analysis_status = 'completed',
                analysis_md = $6,
                analysis_json = $7,
                analysis_model = $8,
                analysis_prompt_version = $9,
                analysis_error_message = NULL,
                analysis_started_at = COALESCE(analysis_started_at, CURRENT_TIMESTAMP),
                analysis_completed_at = CURRENT_TIMESTAMP,
                analysis_elapsed_seconds = $10
            WHERE job_id = $1
            """,
            str(job_id),
            json.dumps(output_json, ensure_ascii=False, default=str),
            result_file_path,
            report_file_path,
            elapsed_seconds,
            str(analysis_md or ""),
            json.dumps(analysis_json or {}, ensure_ascii=False, default=str),
            analysis_model,
            analysis_prompt_version,
            analysis_elapsed_seconds,
        )
        return str(res).strip().endswith("1")
    finally:
        await conn.close()


async def mark_job_failed(
    *,
    job_id: str,
    error_message: str,
    elapsed_seconds: Optional[float],
) -> bool:
    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        res = await conn.execute(
            f"""
            UPDATE {schema}.{table}
            SET status = 'failed',
                error_message = $2,
                elapsed_seconds = $3,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = $1
            """,
            str(job_id),
            str(error_message),
            elapsed_seconds,
        )
        return str(res).strip().endswith("1")
    finally:
        await conn.close()

