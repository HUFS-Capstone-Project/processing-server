-- Remove unused crawled_contents metadata columns (never populated by extractors).

ALTER TABLE processing.crawled_contents
    DROP CONSTRAINT IF EXISTS chk_processing_crawled_contents_links_array;

ALTER TABLE processing.crawled_contents
    DROP COLUMN IF EXISTS title,
    DROP COLUMN IF EXISTS description,
    DROP COLUMN IF EXISTS thumbnail_url,
    DROP COLUMN IF EXISTS links;
