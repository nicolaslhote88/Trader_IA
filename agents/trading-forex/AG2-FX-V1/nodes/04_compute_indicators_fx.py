import math


def sf(v):
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return None


def sma(vals, n):
    return sum(vals[-n:]) / n if len(vals) >= n else None


def ema(vals, n):
    if len(vals) < n:
        return []
    k = 2.0 / (n + 1)
    out = [sum(vals[:n]) / n]
    for v in vals[n:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(vals, n=14):
    if len(vals) < n + 1:
        return None
    deltas = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
    gain = sum(max(0, d) for d in deltas[:n]) / n
    loss = sum(max(0, -d) for d in deltas[:n]) / n
    for d in deltas[n:]:
        gain = (gain * (n - 1) + max(0, d)) / n
        loss = (loss * (n - 1) + max(0, -d)) / n
    if loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + gain / loss))


def atr(highs, lows, closes, n=14):
    if len(closes) < n + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    a = sum(trs[:n]) / n
    for tr in trs[n:]:
        a = (a * (n - 1) + tr) / n
    return a


def macd(vals):
    e12 = ema(vals, 12)
    e26 = ema(vals, 26)
    if not e12 or not e26:
        return None, None, None, None, None
    offset = 14
    line = [e12[i + offset] - e26[i] for i in range(min(len(e26), len(e12) - offset))]
    sig = ema(line, 9)
    m = line[-1] if line else None
    s = sig[-1] if sig else None
    return e12[-1], e26[-1], m, s, (m - s if m is not None and s is not None else None)


def boll(vals, n=20):
    if len(vals) < n:
        return None, None, None
    w = vals[-n:]
    mid = sum(w) / n
    sd = math.sqrt(sum((x - mid) ** 2 for x in w) / n)
    upper = mid + 2 * sd
    lower = mid - 2 * sd
    width = (upper - lower) / mid if mid else None
    return upper, lower, width


out = []
for it in _items or []:
    j = it.get("json", {}) or {}
    bars = ((j.get("history") or {}).get("bars") or [])
    clean = []
    for b in bars:
        o, h, l, c = sf(b.get("o")), sf(b.get("h")), sf(b.get("l")), sf(b.get("c"))
        if o is None or h is None or l is None or c is None:
            continue
        clean.append({"t": b.get("t"), "o": o, "h": h, "l": l, "c": c})
    closes = [b["c"] for b in clean]
    highs = [b["h"] for b in clean]
    lows = [b["l"] for b in clean]
    last = closes[-1] if closes else None
    e12, e26, m, ms, mh = macd(closes)
    bu, bl, bw = boll(closes)
    row = {
        **j,
        "bars_count": len(clean),
        "as_of_bar": clean[-1]["t"] if clean else None,
        "last_close": last,
        "ret_1d": (last / closes[-2] - 1.0) if len(closes) >= 2 and closes[-2] else None,
        "ret_5d": (last / closes[-6] - 1.0) if len(closes) >= 6 and closes[-6] else None,
        "ret_20d": (last / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] else None,
        "rsi14": rsi(closes),
        "atr14": atr(highs, lows, closes),
        "sma20": sma(closes, 20),
        "sma50": sma(closes, 50),
        "sma200": sma(closes, 200),
        "ema12": e12,
        "ema26": e26,
        "macd": m,
        "macd_signal": ms,
        "macd_hist": mh,
        "bb_upper": bu,
        "bb_lower": bl,
        "bb_width": bw,
        "_last_high": highs[-1] if highs else None,
        "_last_low": lows[-1] if lows else None,
        "_last_prev_close": closes[-2] if len(closes) >= 2 else None,
    }
    out.append({"json": row})

return out
