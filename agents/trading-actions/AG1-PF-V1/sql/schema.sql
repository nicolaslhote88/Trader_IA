-- ============================================================
-- AG1-PF-V1 : Portfolio MTM (DuckDB)
-- Source workflow: PF.* portfolio updater in n8n
-- ============================================================

CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_run_log (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR DEFAULT 'RUNNING',
    rows_in         INTEGER DEFAULT 0,
    rows_written    INTEGER DEFAULT 0,
    rows_error      INTEGER DEFAULT 0,
    error_detail    VARCHAR,
    source          VARCHAR DEFAULT 'PF_MTM',
    workflow_name   VARCHAR
);

CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_latest (
    symbol              VARCHAR PRIMARY KEY,
    row_number          INTEGER,
    symbol_raw          VARCHAR,
    name                VARCHAR,
    asset_class         VARCHAR,
    sector              VARCHAR,
    industry            VARCHAR,
    isin                VARCHAR,
    quantity            DOUBLE,
    avg_price           DOUBLE,
    last_price          DOUBLE,
    market_value        DOUBLE,
    unrealized_pnl      DOUBLE,
    updated_at          TIMESTAMP,
    source_updated_at   VARCHAR,
    run_id              VARCHAR,
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_history (
    id                  VARCHAR PRIMARY KEY, -- {run_id}|{symbol}|{row_number}
    run_id              VARCHAR NOT NULL,
    symbol              VARCHAR NOT NULL,
    row_number          INTEGER,
    symbol_raw          VARCHAR,
    name                VARCHAR,
    asset_class         VARCHAR,
    sector              VARCHAR,
    industry            VARCHAR,
    isin                VARCHAR,
    quantity            DOUBLE,
    avg_price           DOUBLE,
    last_price          DOUBLE,
    market_value        DOUBLE,
    unrealized_pnl      DOUBLE,
    updated_at          TIMESTAMP,
    source_updated_at   VARCHAR,
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pf_mtm_latest_updated_at
    ON portfolio_positions_mtm_latest(updated_at);

CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_run
    ON portfolio_positions_mtm_history(run_id);

CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_symbol
    ON portfolio_positions_mtm_history(symbol);

CREATE OR REPLACE VIEW v_portfolio_positions_mtm_latest AS
SELECT
    symbol,
    row_number,
    name,
    asset_class,
    sector,
    industry,
    isin,
    quantity,
    avg_price,
    last_price,
    market_value,
    unrealized_pnl,
    updated_at,
    run_id
FROM portfolio_positions_mtm_latest
ORDER BY market_value DESC NULLS LAST, symbol;
