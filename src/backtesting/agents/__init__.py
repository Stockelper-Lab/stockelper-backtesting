"""OpenAI Agents SDK 기반 백테스팅 오케스트레이션 패키지.

이 패키지는 `stockelper-backtesting` 워커에서 호출되며,
입력 해석(LLM) → 가드레일 → 데이터 사전검증(툴) → 백테스트 실행(툴) → 결과 감사(LLM)
→ 리포트(LLM) → DB 적재(툴) 흐름을 제공합니다.
"""

