-- Allow Naver Blog DOM-based link stats in existing databases.

ALTER TABLE processing.link_stats
    DROP CONSTRAINT IF EXISTS chk_processing_link_stats_source;

ALTER TABLE processing.link_stats
    ADD CONSTRAINT chk_processing_link_stats_source
        CHECK (stats_source IN ('META_TAG', 'INSTAGRAM_META', 'NAVER_BLOG_DOM', 'SCRAPED', 'UNAVAILABLE'));
