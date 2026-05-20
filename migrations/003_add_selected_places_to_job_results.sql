-- Legacy migration. Do not use for new development resets.
-- Use 999_reset_processing_schema_current.sql when existing processing data can be discarded.

ALTER TABLE processing.job_results
ADD COLUMN IF NOT EXISTS selected_places JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE processing.job_results
SET selected_places = jsonb_build_array(selected_place)
WHERE selected_place IS NOT NULL
  AND selected_places = '[]'::jsonb;
