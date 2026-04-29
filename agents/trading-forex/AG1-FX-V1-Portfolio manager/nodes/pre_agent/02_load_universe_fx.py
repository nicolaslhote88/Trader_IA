import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"
rows = []
if os.path.exists(path):
    with duckdb.connect(path, read_only=True) as con:
        try:
            rows = con.execute(
                """
                SELECT pair, symbol_yf, base_ccy, quote_ccy, pip_size, price_decimals, liquidity_tier
                FROM main.universe_fx
                WHERE enabled = TRUE
                ORDER BY pair
                """
            ).fetchdf().to_dict("records")
        except Exception:
            rows = []

if not rows:
    fallback = [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
        "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
        "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD",
        "AUDJPY", "AUDNZD", "AUDCAD",
        "NZDJPY", "NZDCAD", "CADJPY", "CHFJPY", "CADCHF", "CHFCAD", "JPYNZD",
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
