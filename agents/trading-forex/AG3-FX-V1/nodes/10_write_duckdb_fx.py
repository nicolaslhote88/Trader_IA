import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag3_fx_v1.duckdb"

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS main;
CREATE TABLE IF NOT EXISTS main.run_log (
    run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR,
    pairs_total INTEGER, pairs_scored INTEGER, currencies_scored INTEGER, macro_observations INTEGER,
    target_rows INTEGER, errors INTEGER, notes VARCHAR, workflow_version VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS main.fx_macro_observations (
    observation_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, currency VARCHAR NOT NULL,
    factor VARCHAR NOT NULL, source VARCHAR NOT NULL, series_id VARCHAR, observation_date DATE,
    value DOUBLE, previous_value DOUBLE, delta_1m DOUBLE, delta_3m DOUBLE, delta_12m DOUBLE,
    zscore DOUBLE, normalized_score DOUBLE, weight DOUBLE, higher_is_bullish BOOLEAN,
    data_status VARCHAR, fetched_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS main.fx_currency_scores (
    score_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, currency VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL, monetary_score DOUBLE, real_yield_score DOUBLE,
    inflation_score DOUBLE, growth_score DOUBLE, labor_score DOUBLE,
    external_balance_score DOUBLE, risk_regime_score DOUBLE, news_bias_score DOUBLE,
    composite_score DOUBLE, confidence DOUBLE, data_quality_score DOUBLE,
    missing_factors VARCHAR, stale_factors VARCHAR, rationale VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS main.fx_pair_fundamental_scores (
    score_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, pair VARCHAR NOT NULL,
    symbol_yf VARCHAR, base_ccy VARCHAR NOT NULL, quote_ccy VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL, spot DOUBLE, base_score DOUBLE, quote_score DOUBLE,
    pair_pressure DOUBLE, pair_score DOUBLE, directional_bias VARCHAR, confidence DOUBLE,
    data_quality_score DOUBLE, macro_regime VARCHAR, news_bias VARCHAR, news_confidence DOUBLE,
    rationale VARCHAR, invalidators VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS main.fx_equilibrium_targets (
    target_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, pair VARCHAR NOT NULL,
    symbol_yf VARCHAR, as_of TIMESTAMP NOT NULL, spot DOUBLE, equilibrium_target_mid DOUBLE,
    equilibrium_target_low DOUBLE, equilibrium_target_high DOUBLE, equilibrium_shift_pct DOUBLE,
    equilibrium_band_width_pct DOUBLE, mispricing_pct DOUBLE, atr_20d_pct DOUBLE,
    target_horizon_days INTEGER, target_status VARCHAR, method VARCHAR, confidence DOUBLE,
    data_quality_score DOUBLE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS main.fx_ag1_summary (
    summary_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, pair VARCHAR NOT NULL,
    symbol_yf VARCHAR, as_of TIMESTAMP NOT NULL, directional_bias VARCHAR, score DOUBLE,
    confidence DOUBLE, spot DOUBLE, equilibrium_target_mid DOUBLE, equilibrium_target_low DOUBLE,
    equilibrium_target_high DOUBLE, mispricing_pct DOUBLE, target_horizon_days INTEGER,
    drivers_json VARCHAR, invalidators_json VARCHAR, data_caveats_json VARCHAR,
    payload_json VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE OR REPLACE VIEW main.v_latest_currency_scores AS
SELECT * EXCLUDE(rn) FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY as_of DESC, created_at DESC) rn
    FROM main.fx_currency_scores t
) WHERE rn = 1;
CREATE OR REPLACE VIEW main.v_latest_pair_fundamentals AS
SELECT * EXCLUDE(rn) FROM (
    SELECT t.*, ROW_NUMBER() OVER (PARTITION BY pair ORDER BY as_of DESC, created_at DESC) rn
    FROM main.fx_pair_fundamental_scores t
) WHERE rn = 1;
CREATE OR REPLACE VIEW main.v_ag3_fx_ag1_summary AS
SELECT * EXCLUDE(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY pair ORDER BY as_of DESC, created_at DESC) rn
    FROM main.fx_ag1_summary s
) WHERE rn = 1;
"""


def split_sql(text):
    buff, out, sq, dq = [], [], False, False
    for ch in text:
        if ch == "'" and not dq:
            sq = not sq
        elif ch == '"' and not sq:
            dq = not dq
        if ch == ";" and not sq and not dq:
            stmt = "".join(buff).strip()
            if stmt:
                out.append(stmt)
            buff = []
        else:
            buff.append(ch)
    stmt = "".join(buff).strip()
    if stmt:
        out.append(stmt)
    return out


def insert_many(con, table, cols, rows):
    if not rows:
        return 0
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    con.executemany(sql, [[r.get(c) for c in cols] for r in rows])
    return len(rows)


with duckdb.connect(db_path) as con:
    for stmt in split_sql(SCHEMA_SQL):
        con.execute(stmt)

    obs_cols = [
        "observation_id", "run_id", "currency", "factor", "source", "series_id",
        "observation_date", "value", "previous_value", "delta_1m", "delta_3m",
        "delta_12m", "zscore", "normalized_score", "weight", "higher_is_bullish",
        "data_status", "fetched_at",
    ]
    ccy_cols = [
        "score_id", "run_id", "currency", "as_of", "monetary_score", "real_yield_score",
        "inflation_score", "growth_score", "labor_score", "external_balance_score",
        "risk_regime_score", "news_bias_score", "composite_score", "confidence",
        "data_quality_score", "missing_factors", "stale_factors", "rationale",
    ]
    pair_cols = [
        "score_id", "run_id", "pair", "symbol_yf", "base_ccy", "quote_ccy", "as_of",
        "spot", "base_score", "quote_score", "pair_pressure", "pair_score",
        "directional_bias", "confidence", "data_quality_score", "macro_regime",
        "news_bias", "news_confidence", "rationale", "invalidators",
    ]
    target_cols = [
        "target_id", "run_id", "pair", "symbol_yf", "as_of", "spot",
        "equilibrium_target_mid", "equilibrium_target_low", "equilibrium_target_high",
        "equilibrium_shift_pct", "equilibrium_band_width_pct", "mispricing_pct",
        "atr_20d_pct", "target_horizon_days", "target_status", "method",
        "confidence", "data_quality_score",
    ]
    summary_cols = [
        "summary_id", "run_id", "pair", "symbol_yf", "as_of", "directional_bias",
        "score", "confidence", "spot", "equilibrium_target_mid",
        "equilibrium_target_low", "equilibrium_target_high", "mispricing_pct",
        "target_horizon_days", "drivers_json", "invalidators_json",
        "data_caveats_json", "payload_json",
    ]

    written = {
        "macro_observations": insert_many(con, "main.fx_macro_observations", obs_cols, ctx.get("macro_observations") or []),
        "currency_scores": insert_many(con, "main.fx_currency_scores", ccy_cols, ctx.get("currency_scores") or []),
        "pair_scores": insert_many(con, "main.fx_pair_fundamental_scores", pair_cols, ctx.get("pair_fundamental_scores") or []),
        "equilibrium_targets": insert_many(con, "main.fx_equilibrium_targets", target_cols, ctx.get("equilibrium_targets") or []),
        "ag1_summary": insert_many(con, "main.fx_ag1_summary", summary_cols, ctx.get("ag1_summary") or []),
    }
    try:
        con.execute("CHECKPOINT")
    except Exception:
        pass

return [{"json": {**ctx, "duckdb_written": written}}]
