-- Destructive reset migration.
-- Drops the entire processing schema and recreates the current schema shape.
-- Use only when existing processing data can be discarded.

DROP SCHEMA IF EXISTS processing CASCADE;

CREATE SCHEMA IF NOT EXISTS processing;

CREATE TABLE processing.jobs (
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
        CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT chk_processing_jobs_attempts
        CHECK (attempt_count >= 0 AND max_attempts > 0)
);

CREATE INDEX idx_processing_jobs_status_created_at
    ON processing.jobs (status, created_at DESC);

CREATE INDEX idx_processing_jobs_room_normalized_url_status
    ON processing.jobs (room_id, COALESCE(normalized_source_url, source_url), status);

CREATE TABLE processing.job_results (
    job_id UUID PRIMARY KEY REFERENCES processing.jobs(job_id) ON DELETE CASCADE,
    caption TEXT,
    instagram_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    extraction_result JSONB,
    place_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    resolved_places JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_job_results_instagram_meta_object
        CHECK (jsonb_typeof(instagram_meta) = 'object'),
    CONSTRAINT chk_processing_job_results_place_candidates_array
        CHECK (jsonb_typeof(place_candidates) = 'array'),
    CONSTRAINT chk_processing_job_results_resolved_places_array
        CHECK (jsonb_typeof(resolved_places) = 'array'),
    CONSTRAINT chk_processing_job_results_extraction_result_object
        CHECK (extraction_result IS NULL OR jsonb_typeof(extraction_result) = 'object')
);

CREATE TABLE processing.business_hours_jobs (
    job_id UUID PRIMARY KEY,
    kakao_place_id TEXT NOT NULL,
    place_url TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_business_hours_jobs_status
        CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED'))
);

CREATE INDEX idx_processing_business_hours_jobs_status_updated_at
    ON processing.business_hours_jobs (status, updated_at);

CREATE INDEX idx_processing_business_hours_jobs_kakao_place_id_created_at
    ON processing.business_hours_jobs (kakao_place_id, created_at DESC);

CREATE TABLE processing.business_hours_details (
    kakao_place_id TEXT PRIMARY KEY,
    place_url TEXT NOT NULL,
    place_name TEXT,
    business_hours JSONB,
    business_hours_raw TEXT,
    business_hours_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    business_hours_fetched_at TIMESTAMPTZ,
    business_hours_expires_at TIMESTAMPTZ,
    business_hours_source TEXT,
    business_hours_job_id UUID REFERENCES processing.business_hours_jobs(job_id) ON DELETE SET NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT chk_processing_business_hours_details_status
        CHECK (
            business_hours_status IN (
                'PENDING',
                'FETCHING',
                'SUCCEEDED',
                'NOT_FOUND',
                'FAILED'
            )
        ),
    CONSTRAINT chk_processing_business_hours_details_json_object
        CHECK (business_hours IS NULL OR jsonb_typeof(business_hours) = 'object')
);

CREATE INDEX idx_processing_business_hours_details_status_updated_at
    ON processing.business_hours_details (business_hours_status, updated_at);

CREATE INDEX idx_processing_business_hours_details_expires_at
    ON processing.business_hours_details (business_hours_expires_at);

CREATE OR REPLACE FUNCTION processing.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_processing_jobs_updated_at
BEFORE UPDATE ON processing.jobs
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

CREATE TRIGGER trg_processing_job_results_updated_at
BEFORE UPDATE ON processing.job_results
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

CREATE TRIGGER trg_processing_business_hours_jobs_updated_at
BEFORE UPDATE ON processing.business_hours_jobs
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

CREATE TRIGGER trg_processing_business_hours_details_updated_at
BEFORE UPDATE ON processing.business_hours_details
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

