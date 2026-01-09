from __future__ import annotations

import os
import re
import uuid
from typing import Any, Dict, Optional

import asyncpg

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
            dict(input_json),
            {},
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
            dict(output_json),
            result_file_path,
            report_file_path,
            elapsed_seconds,
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

