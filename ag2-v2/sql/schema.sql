-- ============================================================
-- AG2-V2 : DuckDB Schema for Technical Analysis Pipeline
-- Replaces prior spreadsheet outputs with DuckDB-first persistence
-- File: /files/duckdb/ag2_v2.duckdb (mounted in task-runners)
-- ============================================================

-- â”€â”€â”€ Universe (read from Google Sheets, cached locally) â”€â”€â”€
CREATE TABLE IF NOT EXISTS universe (
    symbol          VARCHAR PRIMARY KEY,
    name            VARCHAR,
    asset_class     VARCHAR DEFAULT 'Equity',
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

-- â”€â”€â”€ Technical Signals (main output table) â”€â”€â”€
CREATE TABLE IF NOT EXISTS technical_signals (
    -- Primary key: one row per symbol per run
    id              VARCHAR PRIMARY KEY,  -- {run_id}|{symbol}
    run_id          VARCHAR NOT NULL,
    symbol          VARCHAR NOT NULL,
    workflow_date   TIMESTAMP NOT NULL,

    -- H1 Signal
    h1_date         TIMESTAMP,
    h1_source       VARCHAR,              -- 'cache' | 'yahoo_finance_yfinance'
    h1_status       VARCHAR,              -- 'OK' | 'NO_DATA' | 'INSUFFICIENT_DATA'
    h1_warnings     VARCHAR,
    h1_action       VARCHAR,              -- 'BUY' | 'SELL' | 'NEUTRAL'
    h1_score        INTEGER,
    h1_confidence   DOUBLE,
    h1_rationale    VARCHAR,

    -- D1 Signal
    d1_date         TIMESTAMP,
    d1_source       VARCHAR,
    d1_status       VARCHAR,
    d1_warnings     VARCHAR,
    d1_action       VARCHAR,
    d1_score        INTEGER,
    d1_confidence   DOUBLE,
    d1_rationale    VARCHAR,

    -- Key prices
    last_close      DOUBLE,

    -- H1 Indicators
    h1_sma20        DOUBLE,
    h1_sma50        DOUBLE,
    h1_sma200       DOUBLE,
    h1_ema12        DOUBLE,
    h1_ema26        DOUBLE,
    h1_macd         DOUBLE,
    h1_macd_signal  DOUBLE,
    h1_macd_hist    DOUBLE,
    h1_rsi14        DOUBLE,
    h1_volatility   DOUBLE,
    h1_last_close   DOUBLE,
    h1_atr          DOUBLE,
    h1_atr_pct      DOUBLE,
    h1_bb_upper     DOUBLE,
    h1_bb_lower     DOUBLE,
    h1_bb_width     DOUBLE,
    h1_stoch_k      DOUBLE,
    h1_stoch_d      DOUBLE,
    h1_adx          DOUBLE,
    h1_obv_slope    DOUBLE,
    h1_resistance   DOUBLE,
    h1_support      DOUBLE,
    h1_dist_res_pct DOUBLE,
    h1_dist_sup_pct DOUBLE,

    -- D1 Indicators
    d1_sma20        DOUBLE,
    d1_sma50        DOUBLE,
    d1_sma200       DOUBLE,
    d1_ema12        DOUBLE,
    d1_ema26        DOUBLE,
    d1_macd         DOUBLE,
    d1_macd_signal  DOUBLE,
    d1_macd_hist    DOUBLE,
    d1_rsi14        DOUBLE,
    d1_volatility   DOUBLE,
    d1_last_close   DOUBLE,
    d1_atr          DOUBLE,
    d1_atr_pct      DOUBLE,
    d1_bb_upper     DOUBLE,
    d1_bb_lower     DOUBLE,
    d1_bb_width     DOUBLE,
    d1_stoch_k      DOUBLE,
    d1_stoch_d      DOUBLE,
    d1_adx          DOUBLE,
    d1_obv_slope    DOUBLE,
    d1_resistance   DOUBLE,
    d1_support      DOUBLE,
    d1_dist_res_pct DOUBLE,
    d1_dist_sup_pct DOUBLE,

    -- Routing / filter
    filter_reason   VARCHAR,              -- 'BUY_IN_UPTREND', 'SELL_SIGNAL_SAFETY', etc.
    pass_ai         BOOLEAN DEFAULT FALSE,
    pass_pm         BOOLEAN DEFAULT FALSE,

    -- Dedup
    sig_hash        VARCHAR,
    call_ai         BOOLEAN DEFAULT FALSE,
    dedup_reason    VARCHAR,

    -- AI Validation
    ai_decision     VARCHAR,              -- 'APPROVE' | 'WATCH' | 'REJECT'
    ai_validated    BOOLEAN,
    ai_quality      INTEGER,              -- 1-10
    ai_reasoning    VARCHAR,
    ai_chart_pattern VARCHAR,
    ai_stop_loss    DOUBLE,
    ai_stop_basis   VARCHAR,              -- 'SWING_H1' | 'BAR_ANCHOR' | 'NONE'
    ai_bias_sma200  VARCHAR,              -- 'BULLISH' | 'BEARISH'
    ai_regime_d1    VARCHAR,              -- 'BULLISH' | 'BEARISH' | 'TRANSITION' | 'NEUTRAL_RANGE'
    ai_alignment    VARCHAR,              -- 'WITH_BIAS' | 'AGAINST_BIAS' | 'MIXED' | 'UNKNOWN'
    ai_missing      VARCHAR,              -- JSON array
    ai_anomalies    VARCHAR,              -- JSON array
    ai_output_ref   VARCHAR,
    ai_rr_theoretical DOUBLE,

    -- Vectorization tracking
    vector_status   VARCHAR DEFAULT 'PENDING',  -- 'PENDING' | 'DONE' | 'ERROR'
    vector_id       VARCHAR,                     -- Qdrant point UUID
    vectorized_at   TIMESTAMP,
    row_hash        VARCHAR,

    -- Timestamps
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_ts_symbol ON technical_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_ts_run ON technical_signals(run_id);
CREATE INDEX IF NOT EXISTS idx_ts_date ON technical_signals(workflow_date);
CREATE INDEX IF NOT EXISTS idx_ts_vector ON technical_signals(vector_status);
CREATE INDEX IF NOT EXISTS idx_ts_pass_pm ON technical_signals(pass_pm);

-- â”€â”€â”€ AI Dedup Cache (replaces "ag2_ai_cache") â”€â”€â”€
CREATE TABLE IF NOT EXISTS ai_dedup_cache (
    symbol          VARCHAR NOT NULL,
    interval_key    VARCHAR NOT NULL,     -- 'h1' | 'd1' | 'combined'
    sig_hash        VARCHAR NOT NULL,
    sig_json        VARCHAR,
    last_ai_at      TIMESTAMP,
    last_ai_run_id  VARCHAR,
    last_ai_reason  VARCHAR,
    last_ai_output_ref VARCHAR,
    ttl_minutes     INTEGER DEFAULT 240,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, interval_key)
);

-- â”€â”€â”€ Run Log (new: traceability) â”€â”€â”€
CREATE TABLE IF NOT EXISTS run_log (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR DEFAULT 'RUNNING',  -- 'RUNNING' | 'SUCCESS' | 'PARTIAL' | 'FAILED'
    batch_start     INTEGER,
    batch_size      INTEGER,
    total_pool      INTEGER,
    symbols_ok      INTEGER DEFAULT 0,
    symbols_error   INTEGER DEFAULT 0,
    ai_calls        INTEGER DEFAULT 0,
    vectors_written INTEGER DEFAULT 0,
    error_detail    VARCHAR,
    version         VARCHAR DEFAULT '2.0.0'
);

-- â”€â”€â”€ View: latest signal per symbol (useful for AG1 consumption) â”€â”€â”€
CREATE OR REPLACE VIEW v_latest_signals AS
SELECT ts.*
FROM technical_signals ts
INNER JOIN (
    SELECT symbol, MAX(workflow_date) AS max_date
    FROM technical_signals
    GROUP BY symbol
) latest ON ts.symbol = latest.symbol AND ts.workflow_date = latest.max_date;

-- â”€â”€â”€ View: signals ready for vectorization â”€â”€â”€
CREATE OR REPLACE VIEW v_pending_vectors AS
SELECT ts.*, u.name, u.sector, u.industry, u.currency
FROM technical_signals ts
JOIN universe u ON ts.symbol = u.symbol
WHERE ts.vector_status = 'PENDING'
  AND ts.h1_status = 'OK'
ORDER BY ts.workflow_date DESC;

-- â”€â”€â”€ View: AG1-ready summary (replaces static GSheets read) â”€â”€â”€
CREATE OR REPLACE VIEW v_ag1_summary AS
SELECT
    symbol,
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
    d1_volatility
FROM v_latest_signals
WHERE pass_pm = TRUE;

