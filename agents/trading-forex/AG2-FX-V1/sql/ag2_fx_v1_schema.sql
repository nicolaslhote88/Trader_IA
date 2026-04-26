CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.universe_fx (
    pair                VARCHAR PRIMARY KEY,
    symbol_yf           VARCHAR NOT NULL,
    base_ccy            VARCHAR NOT NULL,
    quote_ccy           VARCHAR NOT NULL,
    pip_size            DOUBLE NOT NULL,
    price_decimals      INTEGER NOT NULL,
    liquidity_tier      VARCHAR,
    enabled             BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.technical_signals_fx (
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    pair                VARCHAR NOT NULL,
    last_close          DOUBLE,
    ret_1d              DOUBLE,
    ret_5d              DOUBLE,
    ret_20d             DOUBLE,
    rsi14               DOUBLE,
    atr14               DOUBLE,
    sma20               DOUBLE,
    sma50               DOUBLE,
    sma200              DOUBLE,
    ema12               DOUBLE,
    ema26               DOUBLE,
    macd                DOUBLE,
    macd_signal         DOUBLE,
    macd_hist           DOUBLE,
    bb_upper            DOUBLE,
    bb_lower            DOUBLE,
    bb_width            DOUBLE,
    "pivot"             DOUBLE,
    r1                  DOUBLE,
    r2                  DOUBLE,
    s1                  DOUBLE,
    s2                  DOUBLE,
    regime              VARCHAR,
    signal_score        DOUBLE,
    signal_label        VARCHAR,
    pip_size            DOUBLE,
    base_ccy            VARCHAR,
    quote_ccy           VARCHAR,
    PRIMARY KEY (run_id, pair)
);

CREATE INDEX IF NOT EXISTS idx_tsfx_pair ON main.technical_signals_fx(pair);
CREATE INDEX IF NOT EXISTS idx_tsfx_asof ON main.technical_signals_fx(as_of);

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    pairs_fetched       INTEGER,
    pairs_with_signal   INTEGER,
    errors              INTEGER,
    notes               VARCHAR
);
