-- ============================================================
-- AG2-V3 : DuckDB Schema for Technical Analysis Pipeline
-- Backward compatible with AG2-V3 + FX addon fields.
-- ============================================================

-- Universe cache (loaded from Google Sheets or equivalent source).
CREATE TABLE IF NOT EXISTS universe (
    -- Internal normalized symbol (stable key, e.g. MC.PA or FX:EURUSD)
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

    -- FX-specific extensions
    base_ccy        VARCHAR,
    quote_ccy       VARCHAR,
    pip_size        DOUBLE,
    price_decimals  INTEGER,
    trading_hours   VARCHAR DEFAULT '24x5',

    enabled         BOOLEAN DEFAULT TRUE,
    boursorama_ref  VARCHAR,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Technical signals output table.
CREATE TABLE IF NOT EXISTS technical_signals (
    -- Primary key: one row per symbol per run
    id                  VARCHAR PRIMARY KEY,  -- {run_id}|{symbol_internal}
    run_id              VARCHAR NOT NULL,
    symbol              VARCHAR NOT NULL,     -- Internal symbol (legacy-compatible alias)
    symbol_internal     VARCHAR,
    symbol_yahoo        VARCHAR,
    asset_class         VARCHAR DEFAULT 'EQUITY',
    workflow_date       TIMESTAMP NOT NULL,

    -- FX-specific metadata (NULL for non-FX)
    base_ccy            VARCHAR,
    quote_ccy           VARCHAR,
    pip_size            DOUBLE,
    price_decimals      INTEGER,
    trading_hours       VARCHAR,

    -- H1 Signal
    h1_date             TIMESTAMP,
    h1_source           VARCHAR,              -- 'cache' | 'yahoo_finance_yfinance'
    h1_status           VARCHAR,              -- 'OK' | 'NO_DATA' | 'INSUFFICIENT_DATA' | 'STALE'
    h1_warnings         VARCHAR,
    h1_action           VARCHAR,              -- 'BUY' | 'SELL' | 'NEUTRAL'
    h1_score            INTEGER,
    h1_confidence       DOUBLE,
    h1_rationale        VARCHAR,

    -- D1 Signal
    d1_date             TIMESTAMP,
    d1_source           VARCHAR,
    d1_status           VARCHAR,
    d1_warnings         VARCHAR,
    d1_action           VARCHAR,
    d1_score            INTEGER,
    d1_confidence       DOUBLE,
    d1_rationale        VARCHAR,

    -- Key prices
    last_close          DOUBLE,

    -- H1 Indicators
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

    -- D1 Indicators
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

    -- FX derived indicators
    atr_pips_h1         DOUBLE,
    atr_pips_d1         DOUBLE,
    stop_pips_suggested DOUBLE,
    data_quality_flags  VARCHAR,              -- JSON array of flags
    data_age_h1_hours   DOUBLE,
    data_age_d1_hours   DOUBLE,

    -- Routing / filter
    filter_reason       VARCHAR,              -- 'BUY_IN_UPTREND', 'SELL_SIGNAL_SAFETY', etc.
    pass_ai             BOOLEAN DEFAULT FALSE,
    pass_pm             BOOLEAN DEFAULT FALSE,

    -- Dedup
    sig_hash            VARCHAR,
    call_ai             BOOLEAN DEFAULT FALSE,
    dedup_reason        VARCHAR,

    -- AI validation
    ai_decision         VARCHAR,              -- 'APPROVE' | 'WATCH' | 'REJECT' | 'SKIP'
    ai_validated        BOOLEAN,
    ai_quality          INTEGER,              -- 1-10
    ai_reasoning        VARCHAR,
    ai_chart_pattern    VARCHAR,
    ai_stop_loss        DOUBLE,
    ai_stop_basis       VARCHAR,              -- 'SWING_H1' | 'BAR_ANCHOR' | 'NONE'
    ai_bias_sma200      VARCHAR,              -- 'BULLISH' | 'BEARISH'
    ai_regime_d1        VARCHAR,              -- 'BULLISH' | 'BEARISH' | 'TRANSITION' | 'NEUTRAL_RANGE'
    ai_alignment        VARCHAR,              -- 'WITH_BIAS' | 'AGAINST_BIAS' | 'MIXED' | 'UNKNOWN'
    ai_bb_status        VARCHAR,              -- 'AT_UPPER_BAND' | 'AT_LOWER_BAND' | 'MID_RANGE' | 'SQUEEZE' | 'UNKNOWN'
    ai_rsi_status       VARCHAR,              -- 'OVERBOUGHT' | 'OVERSOLD' | 'NEUTRAL' | 'UNKNOWN'
    ai_missing          VARCHAR,              -- JSON array
    ai_anomalies        VARCHAR,              -- JSON array
    ai_output_ref       VARCHAR,
    ai_rr_theoretical   DOUBLE,
    should_vectorize    BOOLEAN DEFAULT FALSE,

    -- Vectorization tracking
    vector_status       VARCHAR DEFAULT 'PENDING',  -- 'PENDING' | 'DONE' | 'ERROR'
    vector_id           VARCHAR,
    vectorized_at       TIMESTAMP,
    row_hash            VARCHAR,

    -- Timestamps
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ts_symbol ON technical_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_ts_symbol_internal ON technical_signals(symbol_internal);
CREATE INDEX IF NOT EXISTS idx_ts_symbol_yahoo ON technical_signals(symbol_yahoo);
CREATE INDEX IF NOT EXISTS idx_ts_asset_class ON technical_signals(asset_class);
CREATE INDEX IF NOT EXISTS idx_ts_run ON technical_signals(run_id);
CREATE INDEX IF NOT EXISTS idx_ts_date ON technical_signals(workflow_date);
CREATE INDEX IF NOT EXISTS idx_ts_vector ON technical_signals(vector_status);
CREATE INDEX IF NOT EXISTS idx_ts_pass_pm ON technical_signals(pass_pm);

-- AI dedup cache.
CREATE TABLE IF NOT EXISTS ai_dedup_cache (
    symbol              VARCHAR NOT NULL,
    interval_key        VARCHAR NOT NULL,     -- 'h1' | 'd1' | 'combined'
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

-- Run log.
CREATE TABLE IF NOT EXISTS run_log (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    status              VARCHAR DEFAULT 'RUNNING',  -- 'RUNNING' | 'SUCCESS' | 'PARTIAL' | 'FAILED'
    batch_start         INTEGER,
    batch_size          INTEGER,
    total_pool          INTEGER,
    symbols_ok          INTEGER DEFAULT 0,
    symbols_error       INTEGER DEFAULT 0,
    ai_calls            INTEGER DEFAULT 0,
    vectors_written     INTEGER DEFAULT 0,
    error_detail        VARCHAR,
    version             VARCHAR DEFAULT '3.0.0'
);

CREATE TABLE IF NOT EXISTS batch_state (
    key                 VARCHAR PRIMARY KEY,
    value               INTEGER NOT NULL,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Latest row per symbol.
CREATE OR REPLACE VIEW v_latest_signals AS
SELECT ts.*
FROM technical_signals ts
INNER JOIN (
    SELECT symbol, MAX(workflow_date) AS max_date
    FROM technical_signals
    GROUP BY symbol
) latest ON ts.symbol = latest.symbol AND ts.workflow_date = latest.max_date;

-- Rows ready for vectorization.
CREATE OR REPLACE VIEW v_pending_vectors AS
SELECT ts.*, u.name, u.sector, u.industry, u.currency
FROM technical_signals ts
JOIN universe u ON ts.symbol = u.symbol
WHERE ts.vector_status = 'PENDING'
  AND ts.h1_status = 'OK'
ORDER BY ts.workflow_date DESC;

-- AG1-ready summary.
CREATE OR REPLACE VIEW v_ag1_summary AS
SELECT
    symbol,
    symbol_internal,
    symbol_yahoo,
    asset_class,
    workflow_date,
    d1_action,
    d1_score,
    d1_confidence,
    h1_action,
    h1_score,
    h1_confidence,
    ai_decision,
    ai_validated,
    ai_quality,
    ai_alignment,
    ai_stop_loss,
    ai_rr_theoretical,
    pass_pm,
    last_close,
    d1_rsi14,
    d1_macd_hist,
    d1_sma200,
    d1_bb_width,
    d1_adx,
    d1_volatility,
    atr_pips_h1,
    atr_pips_d1,
    stop_pips_suggested,
    data_quality_flags,
    data_age_h1_hours,
    data_age_d1_hours
FROM v_latest_signals
WHERE pass_pm = TRUE;

-- FX dedicated view (AG2_FX output compatible).
CREATE OR REPLACE VIEW v_ag2_fx_output AS
SELECT
    id,
    run_id,
    symbol_internal AS symbol,
    symbol_yahoo,
    asset_class,
    base_ccy,
    quote_ccy,
    pip_size,
    price_decimals,
    trading_hours,
    workflow_date,
    h1_date,
    d1_date,
    h1_status,
    d1_status,
    h1_action,
    d1_action,
    h1_score,
    d1_score,
    h1_confidence,
    d1_confidence,
    h1_atr,
    d1_atr,
    atr_pips_h1,
    atr_pips_d1,
    stop_pips_suggested,
    data_quality_flags,
    data_age_h1_hours,
    data_age_d1_hours,
    ai_decision,
    ai_quality,
    ai_bb_status,
    ai_rsi_status,
    pass_pm,
    updated_at
FROM technical_signals
WHERE UPPER(COALESCE(asset_class, '')) = 'FX';
