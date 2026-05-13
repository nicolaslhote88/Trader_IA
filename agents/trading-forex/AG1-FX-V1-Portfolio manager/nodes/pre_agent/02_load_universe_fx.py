import os
import time
from contextlib import contextmanager

import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"
rows = []


def is_retryable_duckdb_error(exc):
    msg = str(exc).lower()
    return any(token in msg for token in ("lock", "locked", "conflict", "busy", "timeout"))


@contextmanager
def duckdb_connect_retry(db_path, read_only=True, attempts=8, base_delay=0.35):
    con = None
    for attempt in range(attempts):
        try:
            con = duckdb.connect(db_path, read_only=read_only)
            break
        except Exception as exc:
            if is_retryable_duckdb_error(exc) and attempt < attempts - 1:
                time.sleep(base_delay * (1.7 ** attempt))
                continue
            raise
    try:
        yield con
    finally:
        if con is not None:
            con.close()


if os.path.exists(path):
    try:
        with duckdb_connect_retry(path, read_only=True) as con:
            cur = con.execute(
                """
                SELECT pair, symbol_yf, base_ccy, quote_ccy, pip_size, price_decimals, liquidity_tier
                FROM main.universe_fx
                WHERE enabled = TRUE
                ORDER BY pair
                """
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        ctx["universe_error"] = str(exc)
        try:
            with duckdb_connect_retry(path, read_only=True, attempts=3) as con:
                # Last-chance lightweight query, useful if an older AG2 schema is present.
                cur = con.execute(
                    """
                    SELECT pair, symbol_yf, base_ccy, quote_ccy, pip_size, price_decimals, liquidity_tier
                    FROM main.universe_fx
                    WHERE enabled = TRUE
                    ORDER BY pair
                    """
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            rows = []

if not rows:
    fallback = [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
        "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
        "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD",
        "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
        "CADJPY", "CHFJPY", "NZDJPY", "NZDCHF", "NZDCAD",
    ]
    rows = [
        {
            "pair": p,
            "symbol_yf": f"{p}=X",
            "base_ccy": p[:3],
            "quote_ccy": p[3:],
            "pip_size": 0.01 if p.endswith("JPY") else 0.0001,
            "price_decimals": 3 if p.endswith("JPY") else 5,
            "liquidity_tier": "major" if p in {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD"} else "cross",
        }
        for p in fallback
    ]

return [{"json": {**ctx, "universe_fx": rows}}]
