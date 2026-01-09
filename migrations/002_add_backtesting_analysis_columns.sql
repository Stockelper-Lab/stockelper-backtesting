-- 기존 public.backtesting 테이블에 LLM 해석 컬럼 추가(이미 테이블이 만들어진 경우를 대비)

ALTER TABLE public.backtesting
  ADD COLUMN IF NOT EXISTS analysis_status          text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS analysis_md              text NULL,
  ADD COLUMN IF NOT EXISTS analysis_json            jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS analysis_model           text NULL,
  ADD COLUMN IF NOT EXISTS analysis_prompt_version  text NULL,
  ADD COLUMN IF NOT EXISTS analysis_error_message   text NULL,
  ADD COLUMN IF NOT EXISTS analysis_started_at      timestamptz NULL,
  ADD COLUMN IF NOT EXISTS analysis_completed_at    timestamptz NULL,
  ADD COLUMN IF NOT EXISTS analysis_elapsed_seconds double precision NULL;

CREATE INDEX IF NOT EXISTS backtesting_analysis_status_created_at_idx
  ON public.backtesting (analysis_status, created_at DESC);

