ALTER TABLE processing.jobs
ADD COLUMN IF NOT EXISTS normalized_source_url TEXT,
ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3,
ADD COLUMN IF NOT EXISTS error_code TEXT,
ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

UPDATE processing.jobs
SET normalized_source_url = source_url
WHERE normalized_source_url IS NULL;

CREATE INDEX IF NOT EXISTS idx_processing_jobs_room_normalized_url_status
    ON processing.jobs (room_id, COALESCE(normalized_source_url, source_url), status);

ALTER TABLE processing.job_results
ADD COLUMN IF NOT EXISTS resolved_places JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE processing.job_results
SET resolved_places = selected_places
WHERE resolved_places = '[]'::jsonb
  AND selected_places IS NOT NULL
  AND selected_places <> '[]'::jsonb;

ALTER TABLE processing.job_results
DROP COLUMN IF EXISTS selected_places;

ALTER TABLE processing.business_hours_jobs
DROP CONSTRAINT IF EXISTS chk_processing_business_hours_jobs_status;

UPDATE processing.business_hours_jobs
SET status = CASE status
    WHEN 'PENDING' THEN 'QUEUED'
    WHEN 'FETCHING' THEN 'PROCESSING'
    WHEN 'ENQUEUE_FAILED' THEN 'FAILED'
    ELSE status
END;

ALTER TABLE processing.business_hours_jobs
ALTER COLUMN status SET DEFAULT 'QUEUED';

ALTER TABLE processing.business_hours_jobs
ADD CONSTRAINT chk_processing_business_hours_jobs_status
    CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED'));

ALTER TABLE processing.business_hours_details
DROP CONSTRAINT IF EXISTS chk_processing_business_hours_details_status;

UPDATE processing.business_hours_details
SET business_hours_status = CASE business_hours_status
    WHEN 'SUCCESS' THEN 'SUCCEEDED'
    WHEN 'CRAWL_FAILED' THEN 'FAILED'
    WHEN 'PARSE_FAILED' THEN 'FAILED'
    WHEN 'ENQUEUE_FAILED' THEN 'FAILED'
    ELSE business_hours_status
END;

ALTER TABLE processing.business_hours_details
ADD CONSTRAINT chk_processing_business_hours_details_status
    CHECK (
        business_hours_status IN (
            'PENDING',
            'FETCHING',
            'SUCCEEDED',
            'NOT_FOUND',
            'FAILED'
        )
    );

