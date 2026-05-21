-- Macro Data API - extension devises hors G8 et confiance positionnement.
-- Idempotent DuckDB migration.

CREATE SCHEMA IF NOT EXISTS cot;
CREATE SCHEMA IF NOT EXISTS pillars;

ALTER TABLE cot.speculative_positions
  ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'CFTC_COT';

ALTER TABLE cot.speculative_positions
  ADD COLUMN IF NOT EXISTS confidence VARCHAR DEFAULT 'high';

UPDATE cot.speculative_positions
SET source = 'CFTC_COT'
WHERE source IS NULL;

UPDATE cot.speculative_positions
SET confidence = 'high'
WHERE confidence IS NULL;

ALTER TABLE pillars.currency_scores
  ADD COLUMN IF NOT EXISTS data_completeness VARCHAR DEFAULT 'complete';

ALTER TABLE pillars.currency_scores
  ADD COLUMN IF NOT EXISTS score_status VARCHAR DEFAULT 'scored';

ALTER TABLE pillars.currency_scores
  ADD COLUMN IF NOT EXISTS confidence_floor VARCHAR DEFAULT 'high';

ALTER TABLE pillars.currency_scores
  ADD COLUMN IF NOT EXISTS missing_inputs VARCHAR;

-- Down migration, if needed manually:
-- ALTER TABLE cot.speculative_positions DROP COLUMN source;
-- ALTER TABLE cot.speculative_positions DROP COLUMN confidence;
-- ALTER TABLE pillars.currency_scores DROP COLUMN data_completeness;
-- ALTER TABLE pillars.currency_scores DROP COLUMN score_status;
-- ALTER TABLE pillars.currency_scores DROP COLUMN confidence_floor;
-- ALTER TABLE pillars.currency_scores DROP COLUMN missing_inputs;
