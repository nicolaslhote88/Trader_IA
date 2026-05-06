import json
import os
import re
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


def normalize_timestamp(value):
    if value is None or value == "":
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    if re.match(r"^\d{8}-\d{2}:\d{2}:\d{2}$", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]} {text[9:17]}"
    if "T" in text:
        text = text.replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1].strip()
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text):
        return text
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def to_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        n = float(text)
        return n if n == n and abs(n) != float("inf") else default
    except Exception:
        return default


def quote_to_eur(ccy):
    ccy = str(ccy or "").upper().strip()
    if ccy == "EUR":
        return 1.0
    direct = to_float(prices.get(f"{ccy}EUR"), 0.0)
    if direct > 0:
        return direct
    inverse = to_float(prices.get(f"EUR{ccy}"), 0.0)
    if inverse > 0:
        return 1.0 / inverse
    if ccy == "USD":
        eurusd = to_float(prices.get("EURUSD"), 0.0)
        return 1.0 / eurusd if eurusd > 0 else 1.0
    ccy_usd = to_float(prices.get(f"{ccy}USD"), 0.0)
    usd_ccy = to_float(prices.get(f"USD{ccy}"), 0.0)
    eurusd = to_float(prices.get("EURUSD"), 0.0)
    usd_eur = 1.0 / eurusd if eurusd > 0 else 0.0
    if ccy_usd > 0 and usd_eur > 0:
        return ccy_usd * usd_eur
    if usd_ccy > 0 and usd_eur > 0:
        return (1.0 / usd_ccy) * usd_eur
    return 1.0


def raw_fx_pair(raw, fallback_pair=""):
    text = str(
        raw.get("contract_description_1")
        or raw.get("contractDesc")
        or raw.get("description")
        or fallback_pair
        or ""
    ).upper()
    letters = "".join(ch for ch in text if ch.isalpha())
    if len(letters) >= 6:
        return letters[:6]
    return "".join(ch for ch in str(fallback_pair or "").upper() if ch.isalpha())[:6]


def infer_ibkr_commission_ccy(raw, pair):
    for field in ("commissionCurrency", "commission_currency", "commission_ccy", "ibCommissionCurrency", "feeCurrency"):
        if raw.get(field) not in (None, ""):
            return str(raw.get(field)).upper().strip(), "reported"
    fx_pair = raw_fx_pair(raw, pair)
    asset = str(raw.get("sec_type") or raw.get("secType") or raw.get("assetClass") or "").upper()
    if len(fx_pair) == 6 and asset in {"", "CASH", "FX", "FOREX", "CURRENCY"}:
        return fx_pair[3:6], "inferred_quote"
    return "", "missing"


def ibkr_commission_fields(raw, pair=""):
    amount = None
    amount_field = ""
    for field in ("commission", "ibCommission", "ib_commission", "commission_amount", "fees_eur", "fee"):
        if raw.get(field) not in (None, ""):
            amount = to_float(raw.get(field), 0.0)
            amount_field = field
            break
    if amount is None:
        return {
            "fee_amount": 0.0,
            "fee_ccy": "",
            "fees_eur": 0.0,
            "fee_source": "ibkr_commission_missing",
        }
    ccy, ccy_source = infer_ibkr_commission_ccy(raw, pair)
    if ccy_source == "inferred_quote":
        source = f"ibkr_{amount_field}_inferred_{ccy}_quote_no_ccy"
    elif not ccy:
        ccy = "EUR"
        source = f"ibkr_{amount_field}_fallback_eur_no_ccy"
    elif ccy == "EUR":
        source = f"ibkr_{amount_field}_reported_eur"
    else:
        source = f"ibkr_{amount_field}_converted_{ccy}_to_eur"
    return {
        "fee_amount": abs(amount),
        "fee_ccy": ccy,
        "fees_eur": abs(amount) * quote_to_eur(ccy),
        "fee_source": source,
    }


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
    fee = ibkr_commission_fields(raw, pair)
    fill_id = raw.get("execution_id") or raw.get("execId") or raw.get("id") or f"FIL_{order['order_id']}"
    filled_at = raw.get("trade_time") or raw.get("tradeTime") or raw.get("time") or order.get("ibkr_filled_at")
    filled_at = normalize_timestamp(filled_at)
    return {
        "fill_id": f"IBKR_{fill_id}",
        "order_id": order["order_id"],
        "pair": pair,
        "side": order.get("side"),
        "fill_price": price,
        "fill_size_lots": size_lots,
        "fees_eur": fee["fees_eur"],
        "fee_amount": fee["fee_amount"],
        "fee_ccy": fee["fee_ccy"],
        "fee_source": fee["fee_source"],
        "broker": "IBKR",
        "broker_execution_id": str(fill_id),
        "raw_fill_json": json.dumps(raw, ensure_ascii=False),
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
            "fee_amount": fees_eur,
            "fee_ccy": "EUR",
            "fee_source": "simulated_bps",
            "broker": "SIM",
            "broker_execution_id": "",
            "raw_fill_json": "",
            "swap_eur": 0.0,
            "filled_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
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
