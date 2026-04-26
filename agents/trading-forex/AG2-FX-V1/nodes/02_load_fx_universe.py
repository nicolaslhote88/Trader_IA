import duckdb
from datetime import datetime, timezone

AG4_V3_ALLOWED_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD",
    "AUDJPY", "AUDNZD", "AUDCAD",
    "NZDJPY", "NZDCAD",
    "CADJPY", "CHFJPY", "CADCHF",
    "CHFCAD", "JPYNZD",
]

MAJORS = {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD"}


def meta(pair):
    base, quote = pair[:3], pair[3:]
    return {
        "pair": pair,
        "symbol_yf": f"{pair}=X",
        "base_ccy": base,
        "quote_ccy": quote,
        "pip_size": 0.01 if quote == "JPY" else 0.0001,
        "price_decimals": 3 if quote == "JPY" else 5,
        "liquidity_tier": "major" if pair in MAJORS else "cross",
        "enabled": True,
    }


ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or datetime.now(timezone.utc).strftime("AG2FX_%Y%m%d%H%M%S")
db_path = ctx.get("db_path") or "/files/duckdb/ag2_fx_v1.duckdb"
pairs = list(ctx.get("universe") or AG4_V3_ALLOWED_PAIRS)
if pairs != AG4_V3_ALLOWED_PAIRS:
    raise ValueError("AG2-FX universe drift: expected exact AG4-V3 ALLOWED_PAIRS order")

rows = [meta(p) for p in pairs]
now_ts = datetime.now(timezone.utc)
with duckdb.connect(db_path) as con:
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.universe_fx (
            pair VARCHAR PRIMARY KEY,
            symbol_yf VARCHAR NOT NULL,
            base_ccy VARCHAR NOT NULL,
            quote_ccy VARCHAR NOT NULL,
            pip_size DOUBLE NOT NULL,
            price_decimals INTEGER NOT NULL,
            liquidity_tier VARCHAR,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.run_log (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            pairs_fetched INTEGER,
            pairs_with_signal INTEGER,
            errors INTEGER,
            notes VARCHAR
        )
        """
    )
    con.execute(
        "INSERT OR REPLACE INTO main.run_log VALUES (?, ?, NULL, 0, 0, 0, ?)",
        [run_id, now_ts, "AG2-FX-V1 started"],
    )
    con.executemany(
        """
        INSERT INTO main.universe_fx (
          pair, symbol_yf, base_ccy, quote_ccy, pip_size, price_decimals, liquidity_tier, enabled, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (pair) DO UPDATE SET
          symbol_yf = excluded.symbol_yf,
          base_ccy = excluded.base_ccy,
          quote_ccy = excluded.quote_ccy,
          pip_size = excluded.pip_size,
          price_decimals = excluded.price_decimals,
          liquidity_tier = excluded.liquidity_tier,
          enabled = excluded.enabled,
          updated_at = excluded.updated_at
        """,
        [
            (
                r["pair"], r["symbol_yf"], r["base_ccy"], r["quote_ccy"], r["pip_size"],
                r["price_decimals"], r["liquidity_tier"], r["enabled"], now_ts,
            )
            for r in rows
        ],
    )

ctx_clean = {k: v for k, v in ctx.items() if k != "universe"}
return [{"json": {**ctx_clean, **r}} for r in rows]
