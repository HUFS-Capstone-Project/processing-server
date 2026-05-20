-- Legacy migration. Do not use for new development resets.
-- Use 999_reset_processing_schema_current.sql when existing processing data can be discarded.

ALTER TABLE processing.job_results
ADD COLUMN IF NOT EXISTS extraction_result JSONB;
