CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id                  VARCHAR PRIMARY KEY,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    status                  VARCHAR,
    pairs_total             INTEGER,
    pairs_scored            INTEGER,
    currencies_scored       INTEGER,
    macro_observations      INTEGER,
    target_rows             INTEGER,
    errors                  INTEGER,
    notes                   VARCHAR,
    workflow_version        VARCHAR,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.fx_macro_observations (
    observation_id          VARCHAR PRIMARY KEY,
    run_id                  VARCHAR NOT NULL,
    currency                VARCHAR NOT NULL,
    factor                  VARCHAR NOT NULL,
    source                  VARCHAR NOT NULL,
    series_id               VARCHAR,
    observation_date        DATE,
    value                   DOUBLE,
    previous_value          DOUBLE,
    delta_1m                DOUBLE,
    delta_3m                DOUBLE,
    delta_12m               DOUBLE,
    zscore                  DOUBLE,
    normalized_score        DOUBLE,
    weight                  DOUBLE,
    higher_is_bullish       BOOLEAN,
    data_status             VARCHAR,
    fetched_at              TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.fx_currency_scores (
    score_id                VARCHAR PRIMARY KEY,
    run_id                  VARCHAR NOT NULL,
    currency                VARCHAR NOT NULL,
    as_of                   TIMESTAMP NOT NULL,
    monetary_score          DOUBLE,
    real_yield_score        DOUBLE,
    inflation_score         DOUBLE,
    growth_score            DOUBLE,
    labor_score             DOUBLE,
    external_balance_score  DOUBLE,
    risk_regime_score       DOUBLE,
    news_bias_score         DOUBLE,
    composite_score         DOUBLE,
    confidence              DOUBLE,
    data_quality_score      DOUBLE,
    missing_factors         VARCHAR,
    stale_factors           VARCHAR,
    rationale               VARCHAR,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.fx_pair_fundamental_scores (
    score_id                VARCHAR PRIMARY KEY,
    run_id                  VARCHAR NOT NULL,
    pair                    VARCHAR NOT NULL,
    symbol_yf               VARCHAR,
    base_ccy                VARCHAR NOT NULL,
    quote_ccy               VARCHAR NOT NULL,
    as_of                   TIMESTAMP NOT NULL,
    spot                    DOUBLE,
    base_score              DOUBLE,
    quote_score             DOUBLE,
    pair_pressure           DOUBLE,
    pair_score              DOUBLE,
    directional_bias        VARCHAR,
    confidence              DOUBLE,
    data_quality_score      DOUBLE,
    macro_regime            VARCHAR,
    news_bias               VARCHAR,
    news_confidence         DOUBLE,
    rationale               VARCHAR,
    invalidators            VARCHAR,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.fx_equilibrium_targets (
    target_id                       VARCHAR PRIMARY KEY,
    run_id                          VARCHAR NOT NULL,
    pair                            VARCHAR NOT NULL,
    symbol_yf                       VARCHAR,
    as_of                           TIMESTAMP NOT NULL,
    spot                            DOUBLE,
    equilibrium_target_mid          DOUBLE,
    equilibrium_target_low          DOUBLE,
    equilibrium_target_high         DOUBLE,
    equilibrium_shift_pct           DOUBLE,
    equilibrium_band_width_pct      DOUBLE,
    mispricing_pct                  DOUBLE,
    atr_20d_pct                     DOUBLE,
    target_horizon_days             INTEGER,
    target_status                   VARCHAR,
    method                          VARCHAR,
    confidence                      DOUBLE,
    data_quality_score              DOUBLE,
    created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.fx_ag1_summary (
    summary_id              VARCHAR PRIMARY KEY,
    run_id                  VARCHAR NOT NULL,
    pair                    VARCHAR NOT NULL,
    symbol_yf               VARCHAR,
    as_of                   TIMESTAMP NOT NULL,
    directional_bias        VARCHAR,
    score                   DOUBLE,
    confidence              DOUBLE,
    spot                    DOUBLE,
    equilibrium_target_mid  DOUBLE,
    equilibrium_target_low  DOUBLE,
    equilibrium_target_high DOUBLE,
    mispricing_pct          DOUBLE,
    target_horizon_days     INTEGER,
    drivers_json            VARCHAR,
    invalidators_json       VARCHAR,
    data_caveats_json       VARCHAR,
    payload_json            VARCHAR,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW main.v_latest_currency_scores AS
SELECT * EXCLUDE(rn)
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (
               PARTITION BY currency
               ORDER BY as_of DESC, created_at DESC
           ) rn
    FROM main.fx_currency_scores t
)
WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_latest_pair_fundamentals AS
SELECT * EXCLUDE(rn)
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (
               PARTITION BY pair
               ORDER BY as_of DESC, created_at DESC
           ) rn
    FROM main.fx_pair_fundamental_scores t
)
WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_ag3_fx_ag1_summary AS
SELECT * EXCLUDE(rn)
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY pair
               ORDER BY as_of DESC, created_at DESC
           ) rn
    FROM main.fx_ag1_summary s
)
WHERE rn = 1;
