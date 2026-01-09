#!/usr/bin/env python
"""
DART 공시정보 지표 추출 CLI

공시정보 테이블에서 지표를 추출하여 PostgreSQL에 적재하는 CLI 도구입니다.

사용법:
    uv run python dart_indicator_cli.py --help
    uv run python dart_indicator_cli.py extract          # 지표 추출 및 DB 적재
    uv run python dart_indicator_cli.py list-tables      # 테이블 목록 조회
    uv run python dart_indicator_cli.py preview          # 미리보기 (적재하지 않음)
"""

import argparse
import json
import warnings
from typing import Optional

import pandas as pd

from db_con import (
    DB_CONFIG,
    get_connection,
    get_sqlalchemy_engine,
    get_table_data,
    get_table_list,
)

warnings.filterwarnings('ignore')


# 공시정보 테이블 목록
DART_TABLES = [
    'dart_bdwt_is_decsn',
    'dart_bsn_inh_decsn',
    'dart_bsn_trf_decsn',
    'dart_cmp_dv_decsn',
    'dart_cmp_dvmg_decsn',
    'dart_cmp_mg_decsn',
    'dart_cr_decsn',
    'dart_cvbd_is_decsn',
    'dart_fric_decsn',
    'dart_otcpr_stk_invscr_inh_decsn',
    'dart_otcpr_stk_invscr_trf_decsn',
    'dart_pifric_decsn',
    'dart_piic_decsn',
    'dart_stk_extr_decsn',
    'dart_tgast_inh_decsn',
    'dart_tgast_trf_decsn',
    'dart_tsstk_aq_decsn',
    'dart_tsstk_aq_trctr_cc_decsn',
    'dart_tsstk_aq_trctr_cns_decsn',
    'dart_tsstk_dp_decsn',
]

# report_type 매핑
REPORT_TYPE_MAPPING = {
    'bdwtIsDecsn': '신주인수권부사채발행',
    'bsnInhDecsn': '영업양수 결정',
    'bsnTrfDecsn': '영업양도 결정',
    'cmpDvDecsn': '회사분할 결정',
    'cmpDvmgDecsn': '회사분할합병 결정',
    'cmpMgDecsn': '회사합병 결정',
    'crDecsn': '감자 결정',
    'cvbdIsDecsn': '전환사채권 발행결정',
    'fricDecsn': '무상증자 결정',
    'otcprStkInvscrInhDecsn': '타법인 주식 및 출자증권 양수결정',
    'otcprStkInvscrTrfDecsn': '타법인 주식 및 출자증권 양도결정',
    'pifricDecsn': '유무상증자 결정',
    'piicDecsn': '유상증자 결정',
    'stkExtrDecsn': '주식교환·이전 결정',
    'tgastInhDecsn': '유형자산 양수 결정',
    'tgastTrfDecsn': '유형자산 양도 결정',
    'tsstkAqDecsn': '자기주식 취득 결정',
    'tsstkAqTrctrCcDecsn': '자기주식취득 신탁계약 해지 결정',
    'tsstkAqTrctrCnsDecsn': '자기주식취득 신탁계약 체결 결정',
    'tsstkDpDecsn': '자기주식 처분 결정',
}

# 추출할 지표 목록
INDICATOR_NAMES = [
    '희석률',
    '무상증자 배정비율',
    '감자비율',
    '자본금 감소율',
    '전환희석률',
    'BW 희석률',
    '합병비율',
    '분할비율',
    '분할합병비율',
    '교환이전비율',
]


def json_text_to_dataframe(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    DataFrame의 JSON 텍스트 컬럼을 파싱하여 새로운 DataFrame으로 반환
    
    Args:
        df: 원본 DataFrame
        column_name: JSON이 포함된 컬럼명
        
    Returns:
        JSON 파싱 결과 DataFrame
    """
    result_rows = []
    
    for idx, row in df.iterrows():
        payload = row[column_name]
        if payload is None:
            result_rows.append({})
            continue
            
        try:
            if isinstance(payload, str):
                parsed = json.loads(payload)
            elif isinstance(payload, dict):
                parsed = payload
            else:
                result_rows.append({})
                continue
            result_rows.append(parsed)
        except (json.JSONDecodeError, TypeError):
            result_rows.append({})
    
    return pd.DataFrame(result_rows)


def erase_comma_to_float(num_str) -> Optional[float]:
    """콤마가 포함된 숫자 문자열을 float로 변환"""
    try:
        if num_str is None:
            return None
        return float(str(num_str).replace(',', ''))
    except (ValueError, AttributeError):
        return None


def cal_indicators(indicator_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    지표명에 따라 해당 지표를 계산하여 반환
    
    Args:
        indicator_name: 지표명
        df: 전체 공시정보 DataFrame
        
    Returns:
        지표가 계산된 DataFrame
    """
    tar_df = pd.DataFrame()
    trimmed_df = pd.DataFrame()
    
    if indicator_name == '희석률':
        tar_df = df.loc[df['report_type'] == '유상증자 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'nstk_ostk_cnt' in trimmed_df.columns and 'bfic_tisstk_ostk' in trimmed_df.columns:
            trimmed_df['nstk_ostk_cnt'] = trimmed_df['nstk_ostk_cnt'].apply(erase_comma_to_float)
            trimmed_df['bfic_tisstk_ostk'] = trimmed_df['bfic_tisstk_ostk'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['nstk_ostk_cnt'] / trimmed_df['bfic_tisstk_ostk']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '무상증자 배정비율':
        tar_df = df.loc[df['report_type'] == '무상증자 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'nstk_ascnt_ps_ostk' in trimmed_df.columns:
            trimmed_df['nstk_ascnt_ps_ostk'] = trimmed_df['nstk_ascnt_ps_ostk'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['nstk_ascnt_ps_ostk']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '감자비율':
        tar_df = df.loc[df['report_type'] == '감자 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'cr_rt_ostk' in trimmed_df.columns:
            trimmed_df['cr_rt_ostk'] = trimmed_df['cr_rt_ostk'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['cr_rt_ostk']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '자본금 감소율':
        tar_df = df.loc[df['report_type'] == '감자 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'bfcr_cpt' in trimmed_df.columns and 'atcr_cpt' in trimmed_df.columns:
            trimmed_df['bfcr_cpt'] = trimmed_df['bfcr_cpt'].apply(erase_comma_to_float)
            trimmed_df['atcr_cpt'] = trimmed_df['atcr_cpt'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = (trimmed_df['bfcr_cpt'] - trimmed_df['atcr_cpt']) / trimmed_df['bfcr_cpt']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '전환희석률':
        tar_df = df.loc[df['report_type'] == '전환사채권 발행결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'cvisstk_tisstk_vs' in trimmed_df.columns:
            trimmed_df['cvisstk_tisstk_vs'] = trimmed_df['cvisstk_tisstk_vs'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['cvisstk_tisstk_vs']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == 'BW 희석률':
        tar_df = df.loc[df['report_type'] == '신주인수권부사채발행', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'nstk_isstk_tisstk_vs' in trimmed_df.columns:
            trimmed_df['nstk_isstk_tisstk_vs'] = trimmed_df['nstk_isstk_tisstk_vs'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['nstk_isstk_tisstk_vs']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '합병비율':
        tar_df = df.loc[df['report_type'] == '회사합병 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'mg_rt' in trimmed_df.columns:
            trimmed_df['mg_rt'] = trimmed_df['mg_rt'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['mg_rt']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '분할비율':
        tar_df = df.loc[df['report_type'] == '회사분할 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'dv_rt' in trimmed_df.columns:
            trimmed_df['dv_rt'] = trimmed_df['dv_rt'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['dv_rt']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '분할합병비율':
        tar_df = df.loc[df['report_type'] == '회사분할합병 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'dvmg_rt' in trimmed_df.columns:
            trimmed_df['dvmg_rt'] = trimmed_df['dvmg_rt'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['dvmg_rt']
        else:
            trimmed_df['idc_score'] = None
            
    elif indicator_name == '교환이전비율':
        tar_df = df.loc[df['report_type'] == '주식교환·이전 결정', :].copy()
        if len(tar_df) == 0:
            return pd.DataFrame()
        trimmed_df = json_text_to_dataframe(tar_df, 'payload')
        if 'extr_rt' in trimmed_df.columns:
            trimmed_df['extr_rt'] = trimmed_df['extr_rt'].apply(erase_comma_to_float)
            trimmed_df['idc_score'] = trimmed_df['extr_rt']
        else:
            trimmed_df['idc_score'] = None
    else:
        return pd.DataFrame()
    
    if len(tar_df) == 0:
        return pd.DataFrame()
        
    tar_df = tar_df.reset_index(drop=True)
    tar_df['idc_nm'] = indicator_name
    
    if 'idc_score' in trimmed_df.columns:
        tar_df['idc_score'] = trimmed_df['idc_score'].values
    else:
        tar_df['idc_score'] = None
        
    return tar_df


def load_all_dart_tables(conn, limit: Optional[int] = None, verbose: bool = True) -> pd.DataFrame:
    """
    모든 DART 공시정보 테이블을 로드하고 병합
    
    Args:
        conn: DB 연결 객체
        limit: 테이블당 조회할 행 수 (None이면 전체)
        verbose: 진행상황 출력 여부
        
    Returns:
        병합된 전체 DataFrame
    """
    total_tables = pd.DataFrame()
    
    for table_nm in DART_TABLES:
        if verbose:
            print(f"📥 테이블 로드 중: {table_nm}")
        
        try:
            tar_table = get_table_data(conn, table_nm, limit=limit)
            if tar_table is not None and len(tar_table) > 0:
                total_tables = pd.concat([total_tables, tar_table], axis=0, ignore_index=True)
                if verbose:
                    print(f"   ✅ {len(tar_table)}개 행 로드됨")
            else:
                if verbose:
                    print(f"   ⚠️ 데이터 없음")
        except Exception as e:
            if verbose:
                print(f"   ❌ 오류: {e}")
    
    # report_type 매핑 적용
    if 'report_type' in total_tables.columns:
        total_tables['report_type'] = total_tables['report_type'].map(REPORT_TYPE_MAPPING)
    
    return total_tables


def extract_all_indicators(total_tables: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    전체 데이터에서 모든 지표 추출
    
    Args:
        total_tables: 전체 공시정보 DataFrame
        verbose: 진행상황 출력 여부
        
    Returns:
        모든 지표가 추출된 DataFrame
    """
    result_df = pd.DataFrame()
    
    for indicator_name in INDICATOR_NAMES:
        if verbose:
            print(f"📊 지표 추출 중: {indicator_name}")
        
        try:
            temp_df = cal_indicators(indicator_name, total_tables)
            if len(temp_df) > 0:
                result_df = pd.concat([result_df, temp_df], axis=0, ignore_index=True)
                if verbose:
                    print(f"   ✅ {len(temp_df)}개 행 추출됨")
            else:
                if verbose:
                    print(f"   ⚠️ 해당 데이터 없음")
        except Exception as e:
            if verbose:
                print(f"   ❌ 오류: {e}")
    
    # idc_score가 없는 행 제거
    if 'idc_score' in result_df.columns:
        result_df = result_df.dropna(subset=['idc_score'])
    
    return result_df


def upload_to_db(
    df: pd.DataFrame, 
    table_name: str = 'score_table_dart_idc',
    schema: str = 'public',
    if_exists: str = 'replace',
    verbose: bool = True
) -> bool:
    """
    DataFrame을 PostgreSQL에 업로드
    
    Args:
        df: 업로드할 DataFrame
        table_name: 테이블 이름
        schema: 스키마 이름
        if_exists: 'replace', 'append', 'fail' 중 선택
        verbose: 진행상황 출력 여부
        
    Returns:
        성공 여부
    """
    try:
        engine = get_sqlalchemy_engine()
        
        # payload 컬럼 제외 (dict 타입은 직접 저장 불가)
        upload_df = df.drop(columns=['payload'], errors='ignore')
        
        if verbose:
            print(f"\n📤 DB 업로드 중: {schema}.{table_name}")
            print(f"   행 수: {len(upload_df)}")
            print(f"   컬럼: {list(upload_df.columns)}")
        
        upload_df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False
        )
        
        if verbose:
            print(f"   ✅ 업로드 완료!")
        
        return True
        
    except Exception as e:
        if verbose:
            print(f"   ❌ 업로드 실패: {e}")
        return False


def cmd_list_tables(args):
    """테이블 목록 조회 명령"""
    print("🔍 DB 연결 중...")
    conn = get_connection()
    
    if not conn:
        print("❌ DB 연결 실패")
        return
    
    try:
        tables = get_table_list(conn)
        
        if args.dart_only:
            # DART 테이블만 필터링
            dart_tables = tables[tables['table_name'].str.startswith('dart_')]
            print(f"\n📋 DART 공시정보 테이블 ({len(dart_tables)}개):")
            for _, row in dart_tables.iterrows():
                print(f"   - {row['table_schema']}.{row['table_name']}")
        else:
            print(f"\n📋 전체 테이블 목록 ({len(tables)}개):")
            for _, row in tables.iterrows():
                print(f"   - {row['table_schema']}.{row['table_name']}")
    finally:
        conn.close()
        print("\n🔒 DB 연결 종료")


def cmd_preview(args):
    """미리보기 명령 (DB 적재하지 않음)"""
    print("🔍 DB 연결 중...")
    conn = get_connection()
    
    if not conn:
        print("❌ DB 연결 실패")
        return
    
    try:
        # 데이터 로드
        print("\n📥 공시정보 테이블 로드 중...")
        total_tables = load_all_dart_tables(conn, limit=args.limit)
        print(f"\n✅ 총 {len(total_tables)}개 행 로드됨")
        
        if len(total_tables) == 0:
            print("⚠️ 로드된 데이터가 없습니다.")
            return
        
        # 지표 추출
        print("\n📊 지표 추출 중...")
        result_df = extract_all_indicators(total_tables)
        print(f"\n✅ 총 {len(result_df)}개 지표 추출됨")
        
        # 결과 미리보기
        if len(result_df) > 0:
            print("\n📋 결과 미리보기:")
            print(result_df[['idc_nm', 'idc_score', 'report_type']].head(20).to_string())
            
            print("\n📊 지표별 통계:")
            stats = result_df.groupby('idc_nm')['idc_score'].agg(['count', 'mean', 'min', 'max'])
            print(stats.to_string())
            
    finally:
        conn.close()
        print("\n🔒 DB 연결 종료")


def cmd_extract(args):
    """지표 추출 및 DB 적재 명령"""
    print("🔍 DB 연결 중...")
    conn = get_connection()
    
    if not conn:
        print("❌ DB 연결 실패")
        return
    
    try:
        # 데이터 로드
        print("\n📥 공시정보 테이블 로드 중...")
        total_tables = load_all_dart_tables(conn, limit=args.limit)
        print(f"\n✅ 총 {len(total_tables)}개 행 로드됨")
        
        if len(total_tables) == 0:
            print("⚠️ 로드된 데이터가 없습니다.")
            return
        
        # 지표 추출
        print("\n📊 지표 추출 중...")
        result_df = extract_all_indicators(total_tables)
        print(f"\n✅ 총 {len(result_df)}개 지표 추출됨")
        
        if len(result_df) == 0:
            print("⚠️ 추출된 지표가 없습니다.")
            return
        
        # DB 적재
        success = upload_to_db(
            result_df,
            table_name=args.table_name,
            schema=args.schema,
            if_exists=args.if_exists
        )
        
        if success:
            print(f"\n🎉 작업 완료! {args.schema}.{args.table_name} 테이블에 {len(result_df)}개 행이 적재되었습니다.")
        else:
            print("\n❌ DB 적재에 실패했습니다.")
            
    finally:
        conn.close()
        print("\n🔒 DB 연결 종료")


def main():
    """메인 CLI 진입점"""
    parser = argparse.ArgumentParser(
        description='DART 공시정보 지표 추출 CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python dart_indicator_cli.py list-tables              # 테이블 목록 조회
  python dart_indicator_cli.py list-tables --dart-only  # DART 테이블만 조회
  python dart_indicator_cli.py preview --limit 100      # 미리보기 (100개씩)
  python dart_indicator_cli.py extract                  # 전체 추출 및 DB 적재
  python dart_indicator_cli.py extract --if-exists append  # 기존 데이터에 추가
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='명령어')
    
    # list-tables 명령
    list_parser = subparsers.add_parser('list-tables', help='테이블 목록 조회')
    list_parser.add_argument(
        '--dart-only', 
        action='store_true', 
        help='DART 공시정보 테이블만 표시'
    )
    list_parser.set_defaults(func=cmd_list_tables)
    
    # preview 명령
    preview_parser = subparsers.add_parser('preview', help='지표 추출 미리보기 (DB 적재하지 않음)')
    preview_parser.add_argument(
        '--limit', 
        type=int, 
        default=100, 
        help='테이블당 조회할 행 수 (기본값: 100)'
    )
    preview_parser.set_defaults(func=cmd_preview)
    
    # extract 명령
    extract_parser = subparsers.add_parser('extract', help='지표 추출 및 DB 적재')
    extract_parser.add_argument(
        '--limit', 
        type=int, 
        default=None, 
        help='테이블당 조회할 행 수 (기본값: 전체)'
    )
    extract_parser.add_argument(
        '--table-name', 
        type=str, 
        default='score_table_dart_idc', 
        help='저장할 테이블 이름 (기본값: score_table_dart_idc)'
    )
    extract_parser.add_argument(
        '--schema', 
        type=str, 
        default='public', 
        help='저장할 스키마 (기본값: public)'
    )
    extract_parser.add_argument(
        '--if-exists', 
        type=str, 
        choices=['replace', 'append', 'fail'], 
        default='replace', 
        help='테이블이 존재할 경우 동작 (기본값: replace)'
    )
    extract_parser.set_defaults(func=cmd_extract)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    args.func(args)


if __name__ == '__main__':
    main()
