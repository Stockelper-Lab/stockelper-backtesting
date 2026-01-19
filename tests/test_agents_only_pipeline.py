import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import pytest


class _DummyRunResult:
    def __init__(self, output: Any):
        self._output = output

    def final_output_as(self, model_cls):
        if isinstance(self._output, model_cls):
            return self._output
        return model_cls.model_validate(self._output)


@contextmanager
def _noop_trace(*args, **kwargs):
    yield


@pytest.mark.asyncio
async def test_process_job_happy_path(monkeypatch):
    """
    네트워크/DB 없이 Agents 파이프라인의 제어 흐름이 정상적으로 completed까지 가는지 검증합니다.
    (Runner.run 및 DB/백테스트/파일 IO는 모두 모킹)
    """

    from backtesting.agents import orchestrator as orch
    from backtesting.agents.schemas import AuditResult, FinalNarrative, ParsedRequest

    os.environ["OPENAI_API_KEY"] = "test-key"

    # 1) Runner/trace 모킹 (LLM 호출 방지)
    async def _fake_runner_run(agent, input_text: str, context=None):
        name = getattr(agent, "name", "")
        if "parse" in name:
            return _DummyRunResult(
                ParsedRequest(
                    target_symbols=None,
                    target_corp_names=["삼성전자"],
                    start_date="2024-01-01",
                    end_date="2024-12-31",
                    sort_by="momentum",
                )
            )
        if "adjust" in name:
            # happy path에서는 호출되지 않아야 함
            raise AssertionError("adjust agent should not be called in happy path")
        if "audit" in name:
            return _DummyRunResult(
                AuditResult(score="pass", needs_retry=False, feedback="ok")
            )
        if "report" in name:
            from backtesting.agents.schemas import FinalNarrative
            return _DummyRunResult(
                FinalNarrative(
                    analysis_md="report",
                    analysis_json=[FinalNarrative.AnalysisItem(key="ok", value=True)],
                )
            )
        raise AssertionError(f"unexpected agent name: {name}")

    monkeypatch.setattr(orch.Runner, "run", _fake_runner_run)
    monkeypatch.setattr(orch, "trace", _noop_trace)

    # 2) Tool impl 모킹 (DB/백테스트/파일 IO 방지)
    monkeypatch.setattr(
        orch,
        "resolve_symbols_impl",
        lambda corp_names=None, query=None: {"symbols": ["005930"], "mapping_log": {}},
    )
    monkeypatch.setattr(
        orch,
        "preflight_data_check_impl",
        lambda backtest_params: {"ok": True, "warnings": []},
    )

    async def _fake_run_backtest_impl(*, backtest_params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "total_return": 0.1,
            "annualized_return": 0.1,
            "mdd": 5.0,
            "sharpe_ratio": 1.0,
            "win_rate": 0.5,
            "total_trades": 1,
            "total_profit": 100.0,
            "total_loss": 0.0,
            "trades": [{"symbol": "005930"}],
            "report": "engine_report",
        }

    monkeypatch.setattr(orch, "run_backtest_impl", _fake_run_backtest_impl)

    # build_artifacts_impl은 orchestrator 내부에서 tools로부터 동적 import 되므로 tools 모듈을 patch
    from backtesting.agents import tools as tools_mod

    monkeypatch.setattr(
        tools_mod,
        "build_artifacts_impl",
        lambda job_id, output_dict: {
            "files": {
                "result_file_path": f"/tmp/{job_id}.json",
                "report_file_path": f"/tmp/{job_id}.md",
            },
            "summary": {"total_return": 0.1},
        },
    )

    captured: Dict[str, Any] = {}

    async def _fake_persist_completed_impl(
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
        captured["job_id"] = job_id
        captured["output_json"] = output_json
        captured["analysis_md"] = analysis_md
        captured["analysis_json"] = analysis_json
        captured["analysis_prompt_version"] = analysis_prompt_version
        return True

    monkeypatch.setattr(orch, "persist_completed_impl", _fake_persist_completed_impl)

    async def _fake_persist_failed_impl(*, job_id: str, error_message: str, elapsed_seconds: Optional[float]) -> bool:
        raise AssertionError(f"should not fail, but failed: {error_message}")

    monkeypatch.setattr(orch, "persist_failed_impl", _fake_persist_failed_impl)

    # 3) 실행
    await orch.process_job(
        {
            "job_id": "job-1",
            "user_id": 1,
            "input_json": {"user_id": 1, "query": "삼성전자 2024년 백테스트"},
        }
    )

    # 4) 검증
    assert captured["job_id"] == "job-1"
    assert isinstance(captured["output_json"], dict)
    assert captured["analysis_md"] == "report"
    assert captured["analysis_json"] == {"ok": True}


@pytest.mark.asyncio
async def test_worker_requires_openai_api_key(monkeypatch):
    """Agents-only worker가 OPENAI_API_KEY 없으면 fail-fast하는지 검증합니다."""

    # OPENAI_API_KEY 비움
    os.environ.pop("OPENAI_API_KEY", None)

    from backtesting import worker

    with pytest.raises(RuntimeError):
        await worker.main()

