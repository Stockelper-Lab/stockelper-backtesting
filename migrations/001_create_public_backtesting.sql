-- stockelper_web DB
-- 결과 적재 테이블(placeholder row → 완료 업데이트) 제안 스키마

CREATE TABLE IF NOT EXISTS public.backtesting (
  id               text PRIMARY KEY,
  job_id           text NOT NULL UNIQUE,
  user_id          integer NOT NULL,
  request_source   text NOT NULL DEFAULT 'llm',
  status           text NOT NULL, -- pending|in_progress|completed|failed

  input_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
  output_json      jsonb NOT NULL DEFAULT '{}'::jsonb,

  result_file_path text NULL,
  report_file_path text NULL,
  error_message    text NULL,

  -- LLM 해석 결과(후처리)
  analysis_status          text NOT NULL DEFAULT 'pending', -- pending|in_progress|completed|failed|skipped
  analysis_md              text NULL,                       -- 사람용 설명(마크다운)
  analysis_json            jsonb NOT NULL DEFAULT '{}'::jsonb, -- 구조화 해석
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
);

CREATE INDEX IF NOT EXISTS backtesting_user_created_at_idx
  ON public.backtesting (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS backtesting_status_created_at_idx
  ON public.backtesting (status, created_at DESC);

CREATE INDEX IF NOT EXISTS backtesting_analysis_status_created_at_idx
  ON public.backtesting (analysis_status, created_at DESC);

