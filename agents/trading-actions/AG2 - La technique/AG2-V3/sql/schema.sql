-- ============================================================
-- AG2-V3 : DuckDB Schema for Technical Analysis Pipeline
-- Scope: actions, ETF and crypto.
-- ============================================================

CREATE TABLE IF NOT EXISTS universe (
    symbol          VARCHAR PRIMARY KEY,
    symbol_yahoo    VARCHAR,
    name            VARCHAR,
    asset_class     VARCHAR DEFAULT 'EQUITY',
    exchange        VARCHAR DEFAULT 'Euronext Paris',
    currency        VARCHAR DEFAULT 'EUR',
    country         VARCHAR,
    sector          VARCHAR,
    industry        VARCHAR,
    isin            VARCHAR,
    enabled         BOOLEAN DEFAULT TRUE,
    boursorama_ref  VARCHAR,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS technical_signals (
    id                  VARCHAR PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    symbol              VARCHAR NOT NULL,
    symbol_internal     VARCHAR,
    symbol_yahoo        VARCHAR,
    asset_class         VARCHAR DEFAULT 'EQUITY',
    exchange            VARCHAR,
    currency            VARCHAR,
    workflow_date       TIMESTAMP NOT NULL,

    h1_date             TIMESTAMP,
    h1_source           VARCHAR,
    h1_status           VARCHAR,
    h1_warnings         VARCHAR,
    h1_action           VARCHAR,
    h1_score            INTEGER,
    h1_confidence       DOUBLE,
    h1_rationale        VARCHAR,

    d1_date             TIMESTAMP,
    d1_source           VARCHAR,
    d1_status           VARCHAR,
    d1_warnings         VARCHAR,
    d1_action           VARCHAR,
    d1_score            INTEGER,
    d1_confidence       DOUBLE,
    d1_rationale        VARCHAR,

    last_close          DOUBLE,

    h1_sma20            DOUBLE,
    h1_sma50            DOUBLE,
    h1_sma200           DOUBLE,
    h1_ema12            DOUBLE,
    h1_ema26            DOUBLE,
    h1_macd             DOUBLE,
    h1_macd_signal      DOUBLE,
    h1_macd_hist        DOUBLE,
    h1_rsi14            DOUBLE,
    h1_volatility       DOUBLE,
    h1_last_close       DOUBLE,
    h1_atr              DOUBLE,
    h1_atr_pct          DOUBLE,
    h1_bb_upper         DOUBLE,
    h1_bb_lower         DOUBLE,
    h1_bb_width         DOUBLE,
    h1_stoch_k          DOUBLE,
    h1_stoch_d          DOUBLE,
    h1_adx              DOUBLE,
    h1_obv_slope        DOUBLE,
    h1_resistance       DOUBLE,
    h1_support          DOUBLE,
    h1_dist_res_pct     DOUBLE,
    h1_dist_sup_pct     DOUBLE,

    d1_sma20            DOUBLE,
    d1_sma50            DOUBLE,
    d1_sma200           DOUBLE,
    d1_ema12            DOUBLE,
    d1_ema26            DOUBLE,
    d1_macd             DOUBLE,
    d1_macd_signal      DOUBLE,
    d1_macd_hist        DOUBLE,
    d1_rsi14            DOUBLE,
    d1_volatility       DOUBLE,
    d1_last_close       DOUBLE,
    d1_atr              DOUBLE,
    d1_atr_pct          DOUBLE,
    d1_bb_upper         DOUBLE,
    d1_bb_lower         DOUBLE,
    d1_bb_width         DOUBLE,
    d1_stoch_k          DOUBLE,
    d1_stoch_d          DOUBLE,
    d1_adx              DOUBLE,
    d1_obv_slope        DOUBLE,
    d1_resistance       DOUBLE,
    d1_support          DOUBLE,
    d1_dist_res_pct     DOUBLE,
    d1_dist_sup_pct     DOUBLE,

    data_quality_flags  VARCHAR,
    data_age_h1_hours   DOUBLE,
    data_age_d1_hours   DOUBLE,
    h1_closed_only      BOOLEAN DEFAULT FALSE,
    d1_closed_only      BOOLEAN DEFAULT FALSE,
    h1_dropped_open     INTEGER DEFAULT 0,
    d1_dropped_open     INTEGER DEFAULT 0,
    h1_dropped_invalid  INTEGER DEFAULT 0,
    d1_dropped_invalid  INTEGER DEFAULT 0,
    strategy_version    VARCHAR,
    config_version      VARCHAR,
    prompt_version      VARCHAR,
    n8n_execution_id    VARCHAR,

    filter_reason       VARCHAR,
    pass_ai             BOOLEAN DEFAULT FALSE,
    pass_pm             BOOLEAN DEFAULT FALSE,

    sig_hash            VARCHAR,
    call_ai             BOOLEAN DEFAULT FALSE,
    dedup_reason        VARCHAR,

    ai_decision         VARCHAR,
    ai_validated        BOOLEAN,
    ai_quality          INTEGER,
    ai_reasoning        VARCHAR,
    ai_chart_pattern    VARCHAR,
    ai_stop_loss        DOUBLE,
    ai_stop_basis       VARCHAR,
    ai_bias_sma200      VARCHAR,
    ai_regime_d1        VARCHAR,
    ai_alignment        VARCHAR,
    ai_bb_status        VARCHAR,
    ai_rsi_status       VARCHAR,
    ai_missing          VARCHAR,
    ai_anomalies        VARCHAR,
    ai_output_ref       VARCHAR,
    ai_model            VARCHAR,
    ai_rr_theoretical   DOUBLE,
    row_hash            VARCHAR,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ts_symbol ON technical_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_ts_symbol_internal ON technical_signals(symbol_internal);
CREATE INDEX IF NOT EXISTS idx_ts_symbol_yahoo ON technical_signals(symbol_yahoo);
CREATE INDEX IF NOT EXISTS idx_ts_asset_class ON technical_signals(asset_class);
CREATE INDEX IF NOT EXISTS idx_ts_run ON technical_signals(run_id);
CREATE INDEX IF NOT EXISTS idx_ts_date ON technical_signals(workflow_date);
CREATE INDEX IF NOT EXISTS idx_ts_pass_pm ON technical_signals(pass_pm);

CREATE TABLE IF NOT EXISTS ai_dedup_cache (
    symbol              VARCHAR NOT NULL,
    interval_key        VARCHAR NOT NULL,
    sig_hash            VARCHAR NOT NULL,
    sig_json            VARCHAR,
    last_ai_at          TIMESTAMP,
    last_ai_run_id      VARCHAR,
    last_ai_reason      VARCHAR,
    last_ai_output_ref  VARCHAR,
    ttl_minutes         INTEGER DEFAULT 240,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, interval_key)
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    status              VARCHAR DEFAULT 'RUNNING',
    batch_start         INTEGER,
    batch_size          INTEGER,
    total_pool          INTEGER,
    symbols_ok          INTEGER DEFAULT 0,
    symbols_error       INTEGER DEFAULT 0,
    ai_calls            INTEGER DEFAULT 0,
    error_detail        VARCHAR,
    version             VARCHAR DEFAULT '3.0.0'
);

CREATE TABLE IF NOT EXISTS batch_state (
    key                 VARCHAR PRIMARY KEY,
    value               INTEGER NOT NULL,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version             VARCHAR PRIMARY KEY,
    applied_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description         VARCHAR
);

CREATE OR REPLACE VIEW v_latest_signals AS
SELECT
    id, run_id, symbol, symbol_internal, symbol_yahoo, asset_class, exchange, currency,
    workflow_date, h1_date, d1_date, h1_status, d1_status,
    h1_closed_only, d1_closed_only, h1_dropped_open, d1_dropped_open,
    h1_dropped_invalid, d1_dropped_invalid,
    h1_action, h1_score, h1_confidence, d1_action, d1_score, d1_confidence,
    last_close, d1_rsi14, d1_macd_hist, d1_sma200, d1_bb_width, d1_adx, d1_volatility,
    data_quality_flags, data_age_h1_hours, data_age_d1_hours,
    ai_decision, ai_validated, ai_quality, ai_alignment, ai_stop_loss, ai_rr_theoretical,
    pass_ai, pass_pm, sig_hash, row_hash, strategy_version, config_version,
    prompt_version, ai_model, n8n_execution_id, created_at, updated_at
FROM technical_signals
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol ORDER BY COALESCE(workflow_date, updated_at, created_at) DESC, id DESC
) = 1;

CREATE OR REPLACE VIEW v_ag1_summary AS
SELECT * FROM v_latest_signals WHERE COALESCE(pass_pm, FALSE);
