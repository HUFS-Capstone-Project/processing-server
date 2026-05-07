DROP SCHEMA IF EXISTS processing CASCADE;

CREATE SCHEMA IF NOT EXISTS processing;

CREATE TABLE IF NOT EXISTS processing.jobs (
    job_id UUID PRIMARY KEY,
    room_id UUID NOT NULL,
    source_url TEXT NOT NULL,
    normalized_source_url TEXT,
    status VARCHAR(16) NOT NULL DEFAULT 'QUEUED',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    error_code TEXT,
    error_message TEXT,
    next_retry_at TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_jobs_status
        CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_status_created_at
    ON processing.jobs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_room_normalized_url_status
    ON processing.jobs (room_id, COALESCE(normalized_source_url, source_url), status);

CREATE TABLE IF NOT EXISTS processing.job_results (
    job_id UUID PRIMARY KEY REFERENCES processing.jobs(job_id) ON DELETE CASCADE,
    caption TEXT,
    instagram_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    extraction_result JSONB,
    place_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    resolved_places JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_job_results_instagram_meta_object
        CHECK (jsonb_typeof(instagram_meta) = 'object')
);

CREATE OR REPLACE FUNCTION processing.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_processing_jobs_updated_at ON processing.jobs;
CREATE TRIGGER trg_processing_jobs_updated_at
BEFORE UPDATE ON processing.jobs
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

DROP TRIGGER IF EXISTS trg_processing_job_results_updated_at ON processing.job_results;
CREATE TRIGGER trg_processing_job_results_updated_at
BEFORE UPDATE ON processing.job_results
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();
