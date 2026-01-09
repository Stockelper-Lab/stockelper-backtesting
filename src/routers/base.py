from fastapi import APIRouter

router = APIRouter(tags=["base"])

@router.get("/")
def read_root():
    """루트 엔드포인트"""
    return {"Hello": "World"}

@router.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "healthy"} 


@router.get("/health/db")
async def health_db():
    """DB 헬스 체크(결과 적재 DB + backtesting 테이블 존재 여부).

    502 원인(환경변수 미설정/DB접속 실패/테이블 미생성)을 빠르게 식별하기 위한 엔드포인트입니다.
    """

    import asyncpg
    from backtesting.web_db import _get_database_url_for_asyncpg, _get_schema, _get_table, ensure_backtesting_table

    schema = _get_schema()
    table = _get_table()
    dsn = _get_database_url_for_asyncpg()

    conn = await asyncpg.connect(dsn)
    try:
        # 테이블 자동 보강(권한 없으면 여기서 예외 발생)
        await ensure_backtesting_table(conn=conn, schema=schema, table=table)
        cnt = await conn.fetchval(f"SELECT COUNT(*) FROM {schema}.{table}")
        return {"status": "healthy", "db": "ok", "schema": schema, "table": table, "row_count": int(cnt or 0)}
    finally:
        await conn.close()