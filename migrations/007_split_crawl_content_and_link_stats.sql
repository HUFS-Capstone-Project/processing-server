-- Legacy migration. Do not use for new development resets.
-- Use 999_reset_processing_schema_current.sql when existing processing data can be discarded.
--
-- Development-stage cleanup for link analysis storage.
-- Existing data is intentionally discarded because caption/instagram_meta are
-- replaced by normalized crawled content and link stats tables.

DROP TABLE IF EXISTS processing.link_stats;
DROP TABLE IF EXISTS processing.crawled_contents;
DROP TABLE IF EXISTS processing.job_results;

CREATE TABLE processing.job_results (
    job_id UUID PRIMARY KEY REFERENCES processing.jobs(job_id) ON DELETE CASCADE,
    extraction_result JSONB,
    place_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    resolved_places JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_job_results_place_candidates_array
        CHECK (jsonb_typeof(place_candidates) = 'array'),
    CONSTRAINT chk_processing_job_results_resolved_places_array
        CHECK (jsonb_typeof(resolved_places) = 'array'),
    CONSTRAINT chk_processing_job_results_extraction_result_object
        CHECK (extraction_result IS NULL OR jsonb_typeof(extraction_result) = 'object')
);

CREATE TABLE processing.crawled_contents (
    job_id UUID PRIMARY KEY REFERENCES processing.jobs(job_id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    content_text TEXT NOT NULL DEFAULT '',
    extraction_method TEXT,
    raw_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_crawled_contents_source_type
        CHECK (source_type IN ('INSTAGRAM', 'NAVER_BLOG', 'GENERIC_WEB')),
    CONSTRAINT chk_processing_crawled_contents_raw_metadata_object
        CHECK (jsonb_typeof(raw_metadata) = 'object')
);

CREATE INDEX idx_processing_crawled_contents_source_type_created_at
    ON processing.crawled_contents (source_type, created_at DESC);

CREATE TABLE processing.link_stats (
    job_id UUID PRIMARY KEY REFERENCES processing.jobs(job_id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    like_count BIGINT,
    comment_count BIGINT,
    posted_at TEXT,
    collected_at TIMESTAMPTZ,
    stats_source TEXT NOT NULL,
    confidence TEXT NOT NULL,
    unavailable_reason TEXT,
    raw_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_processing_link_stats_source_type
        CHECK (source_type IN ('INSTAGRAM', 'NAVER_BLOG', 'GENERIC_WEB')),
    CONSTRAINT chk_processing_link_stats_source
        CHECK (stats_source IN ('META_TAG', 'INSTAGRAM_META', 'SCRAPED', 'UNAVAILABLE')),
    CONSTRAINT chk_processing_link_stats_confidence
        CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    CONSTRAINT chk_processing_link_stats_raw_stats_object
        CHECK (jsonb_typeof(raw_stats) = 'object')
);

CREATE INDEX idx_processing_link_stats_source_type_collected_at
    ON processing.link_stats (source_type, collected_at DESC);

DROP TRIGGER IF EXISTS trg_processing_job_results_updated_at ON processing.job_results;
CREATE TRIGGER trg_processing_job_results_updated_at
BEFORE UPDATE ON processing.job_results
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

DROP TRIGGER IF EXISTS trg_processing_crawled_contents_updated_at ON processing.crawled_contents;
CREATE TRIGGER trg_processing_crawled_contents_updated_at
BEFORE UPDATE ON processing.crawled_contents
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();

DROP TRIGGER IF EXISTS trg_processing_link_stats_updated_at ON processing.link_stats;
CREATE TRIGGER trg_processing_link_stats_updated_at
BEFORE UPDATE ON processing.link_stats
FOR EACH ROW
EXECUTE FUNCTION processing.touch_updated_at();
