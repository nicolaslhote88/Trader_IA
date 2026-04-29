import duckdb
from datetime import datetime, timezone

FEE_BPS = 0.5
LOT_UNITS = 100000

ctx = (_items or [{"json": {}}])[0].get("json", {})
orders = ctx.get("executable_orders") or []
brief = ctx.get("brief") or {}

prices = {r.get("pair"): r.get("last_close") for r in brief.get("technical_signals", []) if r.get("pair")}
meta = {r.get("pair"): r for r in (brief.get("universe", {}).get("metadata") or [])}

fills = []
for o in orders:
    if o.get("status") != "pending":
        continue
    pair = o.get("pair")
    px = float(prices.get(pair) or 0)
    if px <= 0:
        continue
    pip = float((meta.get(pair) or {}).get("pip_size") or (0.01 if str(pair).endswith("JPY") else 0.0001))
    fill_price = px + pip if o.get("side") in {"buy_base", "close_short"} else px - pip
    notional_eur = abs(float(o.get("notional_eur") or 0))
    fees_eur = FEE_BPS * notional_eur / 10000.0
    fills.append({
        "fill_id": f"FIL_{o['order_id']}",
        "order_id": o["order_id"],
        "pair": pair,
        "side": o.get("side"),
        "fill_price": fill_price,
        "fill_size_lots": float(o.get("size_lots") or 0),
        "fees_eur": fees_eur,
        "swap_eur": 0.0,
        "filled_at": datetime.now(timezone.utc).isoformat(),
        "fill_source": "simulated_yfinance",
    })

return [{"json": {**ctx, "fills": fills}}]
