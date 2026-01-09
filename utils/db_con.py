"""
PostgreSQL DB 연결 모듈
공시정보 데이터셋 조회 및 지표 추출을 위한 DB 연결 유틸리티
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# PostgreSQL 접속 정보 (공시정보가 존재하는 테이블)
DB_CONFIG = {
    'host': '',
    'port': '',
    'database': '',
    'user': '',
    'password': ''
}


def get_connection():
    """PostgreSQL DB 연결 객체 반환"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ DB 연결 성공!")
        return conn
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return None


def get_sqlalchemy_engine():
    """SQLAlchemy 엔진 반환"""
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


def get_table_list(conn):
    """DB 내 모든 테이블 목록 조회"""
    query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    df = pd.read_sql(query, conn)
    return df


def query_to_dataframe(conn, query):
    """SQL 쿼리 실행 후 DataFrame으로 반환"""
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return None


def get_table_data(conn, table_name, schema='public', limit=None):
    """특정 테이블 데이터 조회
    
    Args:
        conn: DB 연결 객체
        table_name: 테이블 이름
        schema: 스키마 이름 (기본값: 'public')
        limit: 조회할 행 수 (None이면 전체 조회)
    """
    if limit:
        query = f'SELECT * FROM {schema}."{table_name}" LIMIT {limit};'
    else:
        query = f'SELECT * FROM {schema}."{table_name}";'
    return query_to_dataframe(conn, query)


# 메인 실행
if __name__ == "__main__":
    # 1. DB 연결
    conn = get_connection()
    
    if conn:
        # 2. 테이블 목록 조회
        print("\n📋 테이블 목록:")
        tables = get_table_list(conn)
        print(tables)
        
        # 3. 연결 종료
        conn.close()
        print("\n🔒 DB 연결 종료")
