CREATE TABLE IF NOT EXISTS processing.business_hours_jobs (
    job_id UUID PRIMARY KEY,
    kakao_place_id TEXT NOT NULL,
    place_url TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_business_hours_jobs_status
        CHECK (status IN ('PENDING', 'FETCHING', 'SUCCEEDED', 'FAILED', 'ENQUEUE_FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_processing_business_hours_jobs_status_updated_at
    ON processing.business_hours_jobs (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_processing_business_hours_jobs_kakao_place_id_created_at
    ON processing.business_hours_jobs (kakao_place_id, created_at DESC);

CREATE TABLE IF NOT EXISTS processing.business_hours_details (
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
                'SUCCESS',
                'NOT_FOUND',
                'CRAWL_FAILED',
                'PARSE_FAILED',
                'ENQUEUE_FAILED'
            )
        ),
    CONSTRAINT chk_processing_business_hours_details_json_object
        CHECK (business_hours IS NULL OR jsonb_typeof(business_hours) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_processing_business_hours_details_status_updated_at
    ON processing.business_hours_details (business_hours_status, updated_at);

CREATE INDEX IF NOT EXISTS idx_processing_business_hours_details_expires_at
    ON processing.business_hours_details (business_hours_expires_at);

DROP TRIGGER IF EXISTS trg_processing_business_hours_jobs_updated_at
    ON processing.business_hours_jobs;
CREATE TRIGGER trg_processing_business_hours_jobs_updated_at
BEFORE UPDATE ON processing.business_hours_jobs
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

DROP TRIGGER IF EXISTS trg_processing_business_hours_details_updated_at
    ON processing.business_hours_details;
CREATE TRIGGER trg_processing_business_hours_details_updated_at
BEFORE UPDATE ON processing.business_hours_details
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();
