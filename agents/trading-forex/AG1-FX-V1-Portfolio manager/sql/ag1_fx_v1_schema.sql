CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS cfg;

CREATE TABLE IF NOT EXISTS cfg.portfolio_config (
    config_key              VARCHAR PRIMARY KEY,
    llm_model               VARCHAR NOT NULL,
    initial_capital_eur     DOUBLE NOT NULL DEFAULT 10000,
    leverage_max            DOUBLE NOT NULL DEFAULT 1.0,
    max_pos_pct             DOUBLE NOT NULL DEFAULT 0.20,
    max_pair_pct            DOUBLE NOT NULL DEFAULT 0.20,
    max_currency_exposure_pct DOUBLE NOT NULL DEFAULT 0.50,
    max_daily_drawdown_pct  DOUBLE NOT NULL DEFAULT 0.05,
    kill_switch_active      BOOLEAN NOT NULL DEFAULT FALSE,
    universe_filter         VARCHAR,
    notes                   VARCHAR,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.runs (
    run_id              VARCHAR PRIMARY KEY,
    llm_model           VARCHAR NOT NULL,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    decision_json       VARCHAR,
    decisions_count     INTEGER,
    orders_count        INTEGER,
    fills_count         INTEGER,
    errors              INTEGER,
    leverage_max_used   DOUBLE,
    kill_switch_active  BOOLEAN,
    notes               VARCHAR
);

CREATE TABLE IF NOT EXISTS core.orders (
    order_id            VARCHAR PRIMARY KEY,
    client_order_id     VARCHAR NOT NULL UNIQUE,
    run_id              VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,
    order_type          VARCHAR NOT NULL,
    size_lots           DOUBLE NOT NULL,
    notional_quote      DOUBLE NOT NULL,
    notional_eur        DOUBLE NOT NULL,
    leverage_used       DOUBLE NOT NULL DEFAULT 1.0,
    limit_price         DOUBLE,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    requested_at        TIMESTAMP NOT NULL,
    status              VARCHAR NOT NULL,
    rejection_reason    VARCHAR,
    risk_check_passed   BOOLEAN NOT NULL DEFAULT TRUE,
    risk_check_notes    VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_orders_run ON core.orders(run_id);
CREATE INDEX IF NOT EXISTS idx_orders_pair ON core.orders(pair);

CREATE TABLE IF NOT EXISTS core.fills (
    fill_id             VARCHAR PRIMARY KEY,
    order_id            VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,
    fill_price          DOUBLE NOT NULL,
    fill_size_lots      DOUBLE NOT NULL,
    fees_eur            DOUBLE NOT NULL DEFAULT 0,
    swap_eur            DOUBLE NOT NULL DEFAULT 0,
    filled_at           TIMESTAMP NOT NULL,
    fill_source         VARCHAR DEFAULT 'simulated_yfinance'
);

CREATE TABLE IF NOT EXISTS core.position_lots (
    lot_id              VARCHAR PRIMARY KEY,
    run_id_open         VARCHAR NOT NULL,
    run_id_close        VARCHAR,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,
    size_lots           DOUBLE NOT NULL,
    open_price          DOUBLE NOT NULL,
    open_at             TIMESTAMP NOT NULL,
    close_price         DOUBLE,
    close_at            TIMESTAMP,
    pnl_quote           DOUBLE,
    pnl_eur             DOUBLE,
    fees_eur            DOUBLE DEFAULT 0,
    swap_eur_total      DOUBLE DEFAULT 0,
    leverage_used       DOUBLE NOT NULL DEFAULT 1.0,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    status              VARCHAR NOT NULL,
    notes               VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_lots_pair ON core.position_lots(pair);
CREATE INDEX IF NOT EXISTS idx_lots_status ON core.position_lots(status);
CREATE INDEX IF NOT EXISTS idx_lots_open_at ON core.position_lots(open_at);

CREATE TABLE IF NOT EXISTS core.portfolio_snapshot (
    snapshot_id         VARCHAR PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    cash_eur            DOUBLE NOT NULL,
    equity_eur          DOUBLE NOT NULL,
    margin_used_eur     DOUBLE NOT NULL DEFAULT 0,
    margin_free_eur     DOUBLE NOT NULL,
    leverage_effective  DOUBLE,
    open_lots_count     INTEGER NOT NULL,
    pnl_day_eur         DOUBLE,
    pnl_total_eur       DOUBLE,
    drawdown_day_pct    DOUBLE,
    drawdown_total_pct  DOUBLE,
    notes               VARCHAR
);

CREATE TABLE IF NOT EXISTS core.cash_ledger (
    ledger_id           VARCHAR PRIMARY KEY,
    run_id              VARCHAR,
    as_of               TIMESTAMP NOT NULL,
    movement_type       VARCHAR NOT NULL,
    amount_eur          DOUBLE NOT NULL,
    balance_after_eur   DOUBLE NOT NULL,
    related_lot_id      VARCHAR,
    notes               VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_cash_asof ON core.cash_ledger(as_of);

CREATE TABLE IF NOT EXISTS core.ai_signals (
    signal_id           VARCHAR PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    decision            VARCHAR NOT NULL,
    conviction          DOUBLE,
    rationale           VARCHAR,
    target_size_lots    DOUBLE,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    horizon             VARCHAR,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.alerts (
    alert_id            VARCHAR PRIMARY KEY,
    run_id              VARCHAR,
    occurred_at         TIMESTAMP NOT NULL,
    severity            VARCHAR NOT NULL,
    category            VARCHAR NOT NULL,
    message             VARCHAR NOT NULL,
    payload             VARCHAR
);

INSERT INTO cfg.portfolio_config (
    config_key, llm_model, initial_capital_eur, leverage_max, max_pos_pct,
    max_pair_pct, max_currency_exposure_pct, max_daily_drawdown_pct,
    kill_switch_active, universe_filter, notes
)
SELECT 'default', 'unset', 10000, 1.0, 0.20, 0.20, 0.50, 0.05, FALSE, 'forex_27', 'Seeded by ag1_fx_v1_schema'
WHERE NOT EXISTS (SELECT 1 FROM cfg.portfolio_config WHERE config_key = 'default');

INSERT INTO core.cash_ledger (
    ledger_id, run_id, as_of, movement_type, amount_eur, balance_after_eur, related_lot_id, notes
)
SELECT 'INITIAL_DEPOSIT', NULL, CURRENT_TIMESTAMP, 'deposit', 10000, 10000, NULL, 'Initial AG1-FX-V1 sandbox capital'
WHERE NOT EXISTS (SELECT 1 FROM core.cash_ledger WHERE ledger_id = 'INITIAL_DEPOSIT');
