-- Allow YouTube content extraction and YouTube Data API link stats.

ALTER TABLE processing.crawled_contents
    DROP CONSTRAINT IF EXISTS chk_processing_crawled_contents_source_type;

ALTER TABLE processing.crawled_contents
    ADD CONSTRAINT chk_processing_crawled_contents_source_type
        CHECK (source_type IN ('INSTAGRAM', 'NAVER_BLOG', 'YOUTUBE', 'GENERIC_WEB'));

ALTER TABLE processing.link_stats
    DROP CONSTRAINT IF EXISTS chk_processing_link_stats_source_type;

ALTER TABLE processing.link_stats
    ADD CONSTRAINT chk_processing_link_stats_source_type
        CHECK (source_type IN ('INSTAGRAM', 'NAVER_BLOG', 'YOUTUBE', 'GENERIC_WEB'));

ALTER TABLE processing.link_stats
    DROP CONSTRAINT IF EXISTS chk_processing_link_stats_source;

ALTER TABLE processing.link_stats
    ADD CONSTRAINT chk_processing_link_stats_source
        CHECK (
            stats_source IN (
                'META_TAG',
                'INSTAGRAM_META',
                'NAVER_BLOG_DOM',
                'YOUTUBE_DATA_API',
                'SCRAPED',
                'UNAVAILABLE'
            )
        );
