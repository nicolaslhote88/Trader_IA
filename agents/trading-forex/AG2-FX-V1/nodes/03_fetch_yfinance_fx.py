import json
import math
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def synthetic_bars(pair, days=260):
    seed = sum(ord(c) for c in pair)
    price = 1.0 + (seed % 70) / 100.0
    if pair.endswith("JPY"):
        price = 100.0 + (seed % 60)
    bars = []
    now = datetime.now(timezone.utc)
    for i in range(days):
        t = now - timedelta(days=days - i)
        drift = math.sin((i + seed) / 17.0) * 0.002
        price = max(0.0001, price * (1.0 + drift / 10.0))
        spread = price * (0.0015 + abs(math.sin(i / 9.0)) * 0.001)
        bars.append({
            "t": t.isoformat().replace("+00:00", "Z"),
            "o": price - spread / 3,
            "h": price + spread,
            "l": price - spread,
            "c": price,
            "v": 0,
        })
    return bars


items = _items or []
out = []
for it in items:
    j = it.get("json", {}) or {}
    pair = j.get("pair")
    symbol = j.get("symbol_yf") or f"{pair}=X"
    dry_run = bool(j.get("dry_run")) or os.getenv("AG1_FX_DRY_RUN") == "1"
    try:
        if dry_run:
            payload = {"ok": True, "symbol": symbol, "source": "synthetic", "bars": synthetic_bars(pair)}
        else:
            base = str(j.get("yfinance_api_base") or "http://yfinance-api:8080").rstrip("/")
            params = {
                "symbol": symbol,
                "interval": j.get("interval") or "1d",
                "lookback_days": int(j.get("lookback_days") or 420),
                "max_bars": int(j.get("max_bars") or 420),
                "allow_stale": "true",
            }
            url = f"{base}/history?{urlencode(params)}"
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=60) as resp:
                status = getattr(resp, "status", 200)
                body = resp.read()
            if status >= 400:
                raise RuntimeError(f"yfinance-api HTTP {status}: {body[:200]!r}")
            payload = json.loads(body.decode("utf-8"))
        out.append({"json": {**j, "history": payload, "fetch_error": ""}})
    except Exception as exc:
        out.append({"json": {**j, "history": {"ok": False, "bars": []}, "fetch_error": str(exc)}})

return out
