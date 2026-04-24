-- AG4 Forex dedicated base.
-- Run on a new/empty ag4_forex_v1.duckdb database.

CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.fx_news_history (
    dedupe_key              VARCHAR PRIMARY KEY,
    event_key               VARCHAR,
    run_id                  VARCHAR,
    origin                  VARCHAR,
    canonical_url           VARCHAR,
    published_at            TIMESTAMP,
    title                   VARCHAR,
    source                  VARCHAR,
    source_tier             VARCHAR,
    snippet                 VARCHAR,

    impact_region           VARCHAR,
    impact_magnitude        VARCHAR,
    impact_fx_pairs         VARCHAR,
    currencies_bullish      VARCHAR,
    currencies_bearish      VARCHAR,
    regime                  VARCHAR,
    theme                   VARCHAR,
    urgency                 DOUBLE,
    confidence              DOUBLE,
    impact_score            INTEGER,

    fx_narrative            VARCHAR,
    fx_directional_hint     VARCHAR,
    tagger_version          VARCHAR,

    first_seen_at           TIMESTAMP,
    last_seen_at            TIMESTAMP,
    analyzed_at             TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fxnh_published      ON main.fx_news_history(published_at);
CREATE INDEX IF NOT EXISTS idx_fxnh_magnitude      ON main.fx_news_history(impact_magnitude);
CREATE INDEX IF NOT EXISTS idx_fxnh_pairs          ON main.fx_news_history(impact_fx_pairs);
CREATE INDEX IF NOT EXISTS idx_fxnh_origin         ON main.fx_news_history(origin);

CREATE TABLE IF NOT EXISTS main.fx_macro (
    run_id                  VARCHAR,
    as_of                   TIMESTAMP,
    market_regime           VARCHAR,
    drivers                 VARCHAR,
    confidence              DOUBLE,
    usd_bias                DOUBLE,
    eur_bias                DOUBLE,
    jpy_bias                DOUBLE,
    gbp_bias                DOUBLE,
    chf_bias                DOUBLE,
    aud_bias                DOUBLE,
    cad_bias                DOUBLE,
    nzd_bias                DOUBLE,
    bias_json               VARCHAR,
    source_window_days      INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, as_of)
);

CREATE TABLE IF NOT EXISTS main.fx_pairs (
    id                      VARCHAR PRIMARY KEY,
    run_id                  VARCHAR,
    pair                    VARCHAR,
    symbol_internal         VARCHAR,
    directional_bias        VARCHAR,
    rationale               VARCHAR,
    confidence              DOUBLE,
    urgent_event_window     BOOLEAN,
    as_of                   TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fxp_pair ON main.fx_pairs(pair);
CREATE INDEX IF NOT EXISTS idx_fxp_asof ON main.fx_pairs(as_of);

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id                  VARCHAR PRIMARY KEY,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    news_ingested           INTEGER,
    news_from_global        INTEGER,
    news_from_fx_channels   INTEGER,
    pairs_written           INTEGER,
    errors                  INTEGER,
    notes                   VARCHAR
);

CREATE TABLE IF NOT EXISTS main.news_errors (
    run_id                  VARCHAR,
    occurred_at             TIMESTAMP,
    source                  VARCHAR,
    feed_url                VARCHAR,
    error_type              VARCHAR,
    error_detail            VARCHAR
);
