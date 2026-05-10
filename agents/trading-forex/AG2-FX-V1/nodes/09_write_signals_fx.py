import duckdb
from datetime import datetime, timezone

items = _items or []
if not items:
    return []

db_path = (items[0].get("json", {}) or {}).get("db_path") or "/files/duckdb/ag2_fx_v1.duckdb"
cols = [
    "run_id", "as_of", "pair", "last_close", "ret_1d", "ret_5d", "ret_20d",
    "rsi14", "atr14", "sma20", "sma50", "sma200", "ema12", "ema26",
    "macd", "macd_signal", "macd_hist", "bb_upper", "bb_lower", "bb_width",
    "pivot", "r1", "r2", "s1", "s2", "regime", "signal_score", "signal_label",
    "pip_size", "base_ccy", "quote_ccy", "ibkr_bid", "ibkr_ask", "ibkr_mid",
    "ibkr_last", "ibkr_spread", "ibkr_spread_pct", "ibkr_bid_size", "ibkr_ask_size",
    "ibkr_market_data_availability", "ibkr_market_data_source", "ibkr_snapshot_at",
    "ibkr_snapshot_error",
]
rows = []
as_of = datetime.now(timezone.utc).isoformat()
for it in items:
    j = it.get("json", {}) or {}
    if not j.get("pair"):
        continue
    row = {k: j.get(k) for k in cols}
    row["as_of"] = j.get("as_of_bar") or as_of
    rows.append(tuple(row.get(k) for k in cols))

quoted_cols = [f'"{c}"' for c in cols]

with duckdb.connect(db_path) as con:
    schema_sql = """
    CREATE SCHEMA IF NOT EXISTS main;
    CREATE TABLE IF NOT EXISTS main.technical_signals_fx (
      "run_id" VARCHAR NOT NULL, "as_of" TIMESTAMP NOT NULL, "pair" VARCHAR NOT NULL,
      "last_close" DOUBLE, "ret_1d" DOUBLE, "ret_5d" DOUBLE, "ret_20d" DOUBLE,
      "rsi14" DOUBLE, "atr14" DOUBLE, "sma20" DOUBLE, "sma50" DOUBLE, "sma200" DOUBLE,
      "ema12" DOUBLE, "ema26" DOUBLE, "macd" DOUBLE, "macd_signal" DOUBLE, "macd_hist" DOUBLE,
      "bb_upper" DOUBLE, "bb_lower" DOUBLE, "bb_width" DOUBLE,
      "pivot" DOUBLE, "r1" DOUBLE, "r2" DOUBLE, "s1" DOUBLE, "s2" DOUBLE,
      "regime" VARCHAR, "signal_score" DOUBLE, "signal_label" VARCHAR,
      "pip_size" DOUBLE, "base_ccy" VARCHAR, "quote_ccy" VARCHAR,
      "ibkr_bid" DOUBLE, "ibkr_ask" DOUBLE, "ibkr_mid" DOUBLE, "ibkr_last" DOUBLE,
      "ibkr_spread" DOUBLE, "ibkr_spread_pct" DOUBLE, "ibkr_bid_size" DOUBLE,
      "ibkr_ask_size" DOUBLE, "ibkr_market_data_availability" VARCHAR,
      "ibkr_market_data_source" VARCHAR, "ibkr_snapshot_at" VARCHAR,
      "ibkr_snapshot_error" VARCHAR,
      PRIMARY KEY ("run_id", "pair")
    )
    """
    con.execute(schema_sql)
    for col, typ in [
        ("ibkr_bid", "DOUBLE"), ("ibkr_ask", "DOUBLE"), ("ibkr_mid", "DOUBLE"),
        ("ibkr_last", "DOUBLE"), ("ibkr_spread", "DOUBLE"), ("ibkr_spread_pct", "DOUBLE"),
        ("ibkr_bid_size", "DOUBLE"), ("ibkr_ask_size", "DOUBLE"),
        ("ibkr_market_data_availability", "VARCHAR"), ("ibkr_market_data_source", "VARCHAR"),
        ("ibkr_snapshot_at", "VARCHAR"), ("ibkr_snapshot_error", "VARCHAR"),
    ]:
        try:
            con.execute(f'ALTER TABLE main.technical_signals_fx ADD COLUMN "{col}" {typ}')
        except Exception:
            pass
    if rows:
        con.executemany(
            f"INSERT OR REPLACE INTO main.technical_signals_fx ({', '.join(quoted_cols)}) VALUES ({', '.join(['?'] * len(cols))})",
            rows,
        )

return [{"json": {**(it.get("json", {}) or {}), "signals_written": len(rows)}} for it in items]
