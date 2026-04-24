-- AG4-V3 geo-tagging extension.
-- Additive and nullable by design: safe to run before the backfill completes.

ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_region VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_asset_class VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_magnitude VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_fx_pairs VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS tagger_version VARCHAR;

CREATE INDEX IF NOT EXISTS idx_news_impact_asset_class ON main.news_history(impact_asset_class);
CREATE INDEX IF NOT EXISTS idx_news_impact_region ON main.news_history(impact_region);
CREATE INDEX IF NOT EXISTS idx_news_tagger_version ON main.news_history(tagger_version);
