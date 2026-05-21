ALTER TABLE processing.jobs
    RENAME COLUMN source_url TO original_url;

ALTER TABLE processing.jobs
    RENAME COLUMN normalized_source_url TO canonical_url;

UPDATE processing.jobs
SET canonical_url = original_url
WHERE canonical_url IS NULL;

ALTER TABLE processing.jobs
    ALTER COLUMN canonical_url SET NOT NULL;

DROP INDEX IF EXISTS processing.idx_processing_jobs_room_normalized_url_status;

CREATE INDEX IF NOT EXISTS idx_processing_jobs_room_canonical_url_status
    ON processing.jobs (room_id, canonical_url, status);

ALTER TABLE processing.crawled_contents
    RENAME COLUMN source_url TO crawl_url;

ALTER TABLE processing.link_stats
    RENAME COLUMN source_url TO crawl_url;
