import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def to_float(value, default=None):
    try:
        if value is None:
            return default
        n = float(value)
        return n
    except Exception:
        return default


items = _items or []
if not items:
    return []

first = items[0].get("json", {}) or {}
enabled = bool(first.get("ibkr_marketdata_enabled", True))
broker_url = str(first.get("ibkr_broker_url") or os.getenv("IBKR_BROKER_URL") or "http://ibkr-broker:8080").rstrip("/")
pairs = sorted({str((it.get("json", {}) or {}).get("pair") or "").upper() for it in items if (it.get("json", {}) or {}).get("pair")})

quotes = {}
errors = []
if enabled and pairs:
    try:
        params = urlencode({"pairs": ",".join(pairs)})
        req = Request(f"{broker_url}/marketdata/fx/snapshot?{params}", headers={"Accept": "application/json"})
        with urlopen(req, timeout=12) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        quotes = {str(q.get("pair") or "").upper(): q for q in (payload.get("quotes") or [])}
        errors = payload.get("errors") or []
    except Exception as exc:
        errors = [{"error": str(exc), "source": "ibkr_broker"}]

fetched_at = datetime.now(timezone.utc).isoformat()
out = []
for it in items:
    j = it.get("json", {}) or {}
    pair = str(j.get("pair") or "").upper()
    q = quotes.get(pair) or {}
    mid = to_float(q.get("mid"))
    bid = to_float(q.get("bid"))
    ask = to_float(q.get("ask"))
    spread_pct = to_float(q.get("spread_pct"))
    history = dict(j.get("history") or {})
    bars = list(history.get("bars") or [])
    if mid and mid > 0 and bars:
        last_bar = dict(bars[-1])
        old_close = to_float(last_bar.get("c"), mid) or mid
        last_bar["c"] = mid
        last_bar["h"] = max(to_float(last_bar.get("h"), mid) or mid, mid, old_close)
        last_bar["l"] = min(to_float(last_bar.get("l"), mid) or mid, mid, old_close)
        last_bar["ibkr_overlay"] = True
        bars[-1] = last_bar
        history["bars"] = bars
        history["source"] = f"{history.get('source') or 'history'}+ibkr_snapshot"

    out.append({
        "json": {
            **j,
            "history": history,
            "ibkr_bid": bid,
            "ibkr_ask": ask,
            "ibkr_mid": mid,
            "ibkr_last": to_float(q.get("last")),
            "ibkr_spread": to_float(q.get("spread")),
            "ibkr_spread_pct": spread_pct,
            "ibkr_bid_size": to_float(q.get("bid_size")),
            "ibkr_ask_size": to_float(q.get("ask_size")),
            "ibkr_market_data_availability": q.get("market_data_availability"),
            "ibkr_market_data_source": q.get("source") or ("disabled" if not enabled else "unavailable"),
            "ibkr_snapshot_at": q.get("updated_ms") or fetched_at,
            "ibkr_snapshot_error": "; ".join(str(e.get("pair", "")) + ":" + str(e.get("error", "")) for e in errors if not e.get("pair") or e.get("pair") == pair),
        }
    })

return out
