ALTER TABLE processing.job_results
ADD COLUMN IF NOT EXISTS extraction_result JSONB;
