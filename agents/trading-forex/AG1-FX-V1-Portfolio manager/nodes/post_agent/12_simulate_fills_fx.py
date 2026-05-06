import os
from datetime import datetime, timezone

FEE_BPS = 0.5
LOT_UNITS = 100000

ctx = (_items or [{"json": {}}])[0].get("json", {})
orders = ctx.get("executable_orders") or []
brief = ctx.get("brief") or {}
dry_run = os.environ.get("IBKR_DRY_RUN", "true").lower() != "false"

prices = {r.get("pair"): r.get("last_close") for r in brief.get("technical_signals", []) if r.get("pair")}
meta = {r.get("pair"): r for r in (brief.get("universe", {}).get("metadata") or [])}
open_lots = {
    str(l.get("lot_id") or ""): l
    for l in ((brief.get("portfolio_state") or {}).get("open_lots") or [])
    if l.get("lot_id")
}


def valid_close_order(order):
    side = order.get("side")
    if side not in {"close_long", "close_short"}:
        return True
    lot = open_lots.get(str(order.get("lot_id_to_close") or ""))
    if not lot:
        order["status"] = "rejected"
        order["rejection_reason"] = "LOT_TO_CLOSE_NOT_FOUND"
        return False
    expected_side = "close_long" if lot.get("side") == "long" else "close_short" if lot.get("side") == "short" else ""
    if side != expected_side:
        order["status"] = "rejected"
        order["rejection_reason"] = "CLOSE_SIDE_MISMATCH"
        return False
    size_lots = float(order.get("size_lots") or 0)
    lot_size = float(lot.get("size_lots") or 0)
    if size_lots <= 0 or size_lots > lot_size + 1e-9:
        order["status"] = "rejected"
        order["rejection_reason"] = "CLOSE_SIZE_INVALID"
        return False
    return True


def fill_from_ibkr(order):
    raw = order.get("ibkr_fill") or {}
    if not isinstance(raw, dict):
        return None
    pair = order.get("pair")
    price = raw.get("price") or raw.get("avgPrice") or raw.get("avg_price")
    size = raw.get("size") or raw.get("quantity") or raw.get("shares") or raw.get("filledQuantity")
    try:
        price = float(str(price).replace(",", ""))
        size_units = abs(float(str(size).replace(",", "")))
    except Exception:
        return None
    if price <= 0 or size_units <= 0:
        return None
    size_lots = size_units / LOT_UNITS
    commission = raw.get("commission") or raw.get("commission_eur") or raw.get("fees_eur") or 0
    try:
        fees_eur = abs(float(str(commission).replace(",", "")))
    except Exception:
        fees_eur = 0.0
    fill_id = raw.get("execution_id") or raw.get("execId") or raw.get("id") or f"FIL_{order['order_id']}"
    filled_at = raw.get("trade_time") or raw.get("tradeTime") or raw.get("time") or order.get("ibkr_filled_at")
    if not filled_at:
        filled_at = datetime.now(timezone.utc).isoformat()
    return {
        "fill_id": f"IBKR_{fill_id}",
        "order_id": order["order_id"],
        "pair": pair,
        "side": order.get("side"),
        "fill_price": price,
        "fill_size_lots": size_lots,
        "fees_eur": fees_eur,
        "swap_eur": 0.0,
        "filled_at": filled_at,
        "fill_source": "ibkr_confirmed",
        "lot_id_to_close": order.get("lot_id_to_close") or "",
    }


fills = []
if dry_run:
    for o in orders:
        if o.get("status") != "pending":
            continue
        if not valid_close_order(o):
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
            "lot_id_to_close": o.get("lot_id_to_close") or "",
        })
else:
    for o in orders:
        if o.get("status") != "filled":
            continue
        if not valid_close_order(o):
            continue
        fill = fill_from_ibkr(o)
        if fill:
            fills.append(fill)

rejection_reasons = {}
for o in orders:
    if o.get("status") == "rejected":
        reason = o.get("rejection_reason") or "UNKNOWN"
        rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

safety_summary = {
    **(ctx.get("safety_summary") or {}),
    "rejected_orders_count": sum(rejection_reasons.values()),
    "rejection_reasons": rejection_reasons,
}

return [{"json": {**ctx, "executable_orders": orders, "fills": fills, "safety_summary": safety_summary}}]
