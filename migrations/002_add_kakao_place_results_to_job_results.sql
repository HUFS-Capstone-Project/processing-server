ALTER TABLE processing.job_results
ADD COLUMN IF NOT EXISTS place_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
ADD COLUMN IF NOT EXISTS selected_place JSONB;
