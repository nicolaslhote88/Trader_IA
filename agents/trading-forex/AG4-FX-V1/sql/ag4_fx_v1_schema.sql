CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.fx_digest (
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    section             VARCHAR NOT NULL,
    payload             VARCHAR NOT NULL,
    items_count         INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, section)
);

CREATE INDEX IF NOT EXISTS idx_fxdigest_asof ON main.fx_digest(as_of);

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    news_global_pulled  INTEGER,
    news_fx_channel_pulled INTEGER,
    news_after_dedupe   INTEGER,
    sections_written    INTEGER,
    errors              INTEGER,
    notes               VARCHAR
);
