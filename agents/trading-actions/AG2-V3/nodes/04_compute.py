import json
import math
import duckdb
import time
import gc
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = "/files/duckdb/ag2_v3.duckdb"


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


# ===================================================
# Helpers
# ===================================================
def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def sanitize(d):
    if isinstance(d, dict):
        out = {}
        for k, v in d.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                out[k] = None
            else:
                out[k] = sanitize(v)
        return out
    if isinstance(d, list):
        return [sanitize(x) for x in d]
    return d


def to_upper_text(v):
    return str(v or "").strip().upper()


def infer_fx_meta(symbol_yahoo):
    s = to_upper_text(symbol_yahoo)
    if not s.endswith("=X"):
        return None
    core = s[:-2].replace("/", "").replace("-", "").replace("_", "")
    core = "".join(ch for ch in core if ch.isalpha())
    if len(core) < 6:
        return None
    pair = core[:6]
    base = pair[:3]
    quote = pair[3:]
    pip_size = 0.01 if quote == "JPY" else 0.0001
    price_decimals = 3 if quote == "JPY" else 5
    return {
        "symbol_internal": f"FX:{pair}",
        "base_ccy": base,
        "quote_ccy": quote,
        "pip_size": pip_size,
        "price_decimals": price_decimals,
        "trading_hours": "24x5",
    }


def normalize_identity(d):
    asset_class = to_upper_text(d.get("asset_class") or "EQUITY")
    symbol_internal = str(d.get("symbol_internal") or d.get("symbol") or "").strip().upper()
    symbol_yahoo = str(d.get("symbol_yahoo") or d.get("symbol") or symbol_internal).strip().upper()

    base_ccy = to_upper_text(d.get("base_ccy") or "")
    quote_ccy = to_upper_text(d.get("quote_ccy") or "")
    pip_size = safe_float(d.get("pip_size"))
    price_decimals = d.get("price_decimals")
    trading_hours = str(d.get("trading_hours") or "")

    fx_meta = infer_fx_meta(symbol_yahoo)
    if asset_class == "FX" or fx_meta is not None:
        asset_class = "FX"
        if fx_meta:
            if not symbol_internal:
                symbol_internal = fx_meta["symbol_internal"]
            if not base_ccy:
                base_ccy = fx_meta["base_ccy"]
            if not quote_ccy:
                quote_ccy = fx_meta["quote_ccy"]
            if pip_size is None:
                pip_size = fx_meta["pip_size"]
            if price_decimals is None:
                price_decimals = fx_meta["price_decimals"]
            if not trading_hours:
                trading_hours = fx_meta["trading_hours"]
    else:
        if not symbol_internal:
            symbol_internal = symbol_yahoo

    try:
        price_decimals = int(price_decimals) if price_decimals is not None else None
    except Exception:
        price_decimals = None

    return {
        "asset_class": asset_class or "EQUITY",
        "symbol_internal": symbol_internal,
        "symbol_yahoo": symbol_yahoo,
        "base_ccy": base_ccy or None,
        "quote_ccy": quote_ccy or None,
        "pip_size": pip_size,
        "price_decimals": price_decimals,
        "trading_hours": trading_hours or None,
    }


def has_fx_daily_issue(bars):
    # Lightweight anomaly detector for known Yahoo FX daily quirks.
    if not isinstance(bars, list) or len(bars) < 20:
        return False
    valid = []
    same_ohlc = 0
    repeat_close = 0
    prev_close = None
    for b in bars[-60:]:
        o = safe_float((b or {}).get("o"))
        h = safe_float((b or {}).get("h"))
        l = safe_float((b or {}).get("l"))
        c = safe_float((b or {}).get("c"))
        if o is None or h is None or l is None or c is None:
            continue
        valid.append((o, h, l, c))
        if abs(c - o) < 1e-10 and abs(h - l) < 1e-10:
            same_ohlc += 1
        if prev_close is not None and abs(c - prev_close) < 1e-10:
            repeat_close += 1
        prev_close = c

    if len(valid) < 20:
        return False

    ratio_same = same_ohlc / len(valid)
    ratio_repeat = repeat_close / max(1, len(valid) - 1)
    return ratio_same >= 0.35 or ratio_repeat >= 0.7


def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values, period):
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    init = sum(values[:period]) / period
    result = [init]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def rsi_wilder(closes, period=14):
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    ag = sum(max(0, d) for d in deltas[:period]) / period
    al = sum(max(0, -d) for d in deltas[:period]) / period
    for d in deltas[period:]:
        ag = (ag * (period - 1) + max(0, d)) / period
        al = (al * (period - 1) + max(0, -d)) / period
    if al == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + ag / al))


def macd_calc(closes):
    e12 = ema_series(closes, 12)
    e26 = ema_series(closes, 26)
    if not e12 or not e26:
        return {"ema12": None, "ema26": None, "macd": None, "macd_signal": None, "macd_hist": None}
    offset = 26 - 12
    if len(e12) <= offset:
        return {"ema12": safe_float(e12[-1]), "ema26": safe_float(e26[-1]), "macd": None, "macd_signal": None, "macd_hist": None}
    ml = [e12[i + offset] - e26[i] for i in range(len(e26))]
    sl = ema_series(ml, 9)
    mv = safe_float(ml[-1])
    sv = safe_float(sl[-1]) if sl else None
    hv = round(mv - sv, 6) if mv is not None and sv is not None else None
    return {"ema12": safe_float(e12[-1]), "ema26": safe_float(e26[-1]), "macd": mv, "macd_signal": sv, "macd_hist": hv}


def atr_calc(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
    if len(trs) < period:
        return None
    a = sum(trs[:period]) / period
    for tr in trs[period:]:
        a = (a * (period - 1) + tr) / period
    return a


def bollinger(closes, period=20, ns=2.0):
    if len(closes) < period:
        return {"bb_upper": None, "bb_lower": None, "bb_width": None}
    w = closes[-period:]
    m = sum(w) / period
    s = math.sqrt(sum((x - m) ** 2 for x in w) / period)
    u, l = m + ns * s, m - ns * s
    bw = (u - l) / m * 100 if m != 0 else None
    return {"bb_upper": round(u, 4), "bb_lower": round(l, 4), "bb_width": round(bw, 4) if bw is not None else None}


def stochastic(highs, lows, closes, kp=14, dp=3):
    if len(closes) < kp + dp - 1:
        return {"stoch_k": None, "stoch_d": None}
    kv = []
    for i in range(kp - 1, len(closes)):
        hh = max(highs[i - kp + 1 : i + 1])
        ll = min(lows[i - kp + 1 : i + 1])
        kv.append((closes[i] - ll) / (hh - ll) * 100 if hh != ll else 50.0)
    if len(kv) < dp:
        return {"stoch_k": round(kv[-1], 2) if kv else None, "stoch_d": None}
    return {"stoch_k": round(kv[-1], 2), "stoch_d": round(sum(kv[-dp:]) / dp, 2)}


def adx_calc(highs, lows, closes, period=14):
    if len(closes) < period * 2 + 1:
        return None
    pdm, mdm, trl = [], [], []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i - 1]
        dn = lows[i - 1] - lows[i]
        pdm.append(up if up > dn and up > 0 else 0)
        mdm.append(dn if dn > up and dn > 0 else 0)
        trl.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    if len(trl) < period:
        return None

    def ws(vals, p):
        s = sum(vals[:p])
        r = [s]
        for v in vals[p:]:
            s = s - s / p + v
            r.append(s)
        return r

    st, sp, sm = ws(trl, period), ws(pdm, period), ws(mdm, period)
    dxl = []
    for i in range(len(st)):
        if st[i] == 0:
            continue
        dp_ = sp[i] / st[i] * 100
        dm_ = sm[i] / st[i] * 100
        ds = dp_ + dm_
        dxl.append(abs(dp_ - dm_) / ds * 100 if ds != 0 else 0)
    if len(dxl) < period:
        return None
    av = ws(dxl, period)
    return round(av[-1] / period, 2) if av else None


def obv_slope(closes, volumes, lb=20):
    if len(closes) < lb + 1 or len(volumes) < lb + 1:
        return None
    obv = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    seg = obv[-lb:]
    n = len(seg)
    xm = (n - 1) / 2.0
    ym = sum(seg) / n
    num = sum((i - xm) * (seg[i] - ym) for i in range(n))
    den = sum((i - xm) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    av = sum(volumes[-lb:]) / lb
    return round(num / den / av, 4) if av != 0 else 0.0


def find_sr(highs, lows, closes, window=5, lb=50):
    if len(closes) < lb:
        return {"resistance": None, "support": None, "dist_res_pct": None, "dist_sup_pct": None}
    p = closes[-1]
    h, l = highs[-lb:], lows[-lb:]
    shs, sls = [], []
    for i in range(window, len(h) - window):
        if all(h[i] >= h[i - j] for j in range(1, window + 1)) and all(h[i] >= h[i + j] for j in range(1, window + 1)):
            shs.append(h[i])
        if all(l[i] <= l[i - j] for j in range(1, window + 1)) and all(l[i] <= l[i + j] for j in range(1, window + 1)):
            sls.append(l[i])
    res = sorted([s for s in shs if s > p])
    resistance = res[0] if res else max(h)
    sups = sorted([s for s in sls if s < p], reverse=True)
    support = sups[0] if sups else min(l)
    dr = round((resistance - p) / p * 100, 2) if resistance and p > 0 else None
    ds = round((p - support) / p * 100, 2) if support and p > 0 else None
    return {
        "resistance": round(resistance, 4) if resistance else None,
        "support": round(support, 4) if support else None,
        "dist_res_pct": dr,
        "dist_sup_pct": ds,
    }


def volatility_ann(closes, interval):
    if len(closes) < 20:
        return None
    lr = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0 and closes[i] > 0]
    if len(lr) < 10:
        return None
    m = sum(lr) / len(lr)
    v = sum((r - m) ** 2 for r in lr) / len(lr)
    s = math.sqrt(v)
    il = (interval or "").lower()
    if il in ("1d", "5d"):
        f = math.sqrt(252)
    elif il in ("1wk",):
        f = math.sqrt(52)
    elif il in ("1mo", "3mo"):
        f = math.sqrt(12)
    elif "h" in il or il in ("60m", "90m"):
        f = math.sqrt(252 * 8)
    else:
        f = math.sqrt(252)
    return round(s * f, 4)


def compute_indicators(bars, interval):
    opens, highs, lows, closes, volumes, times = [], [], [], [], [], []
    for b in bars or []:
        o, h, l, c = safe_float(b.get("o")), safe_float(b.get("h")), safe_float(b.get("l")), safe_float(b.get("c"))
        vol = safe_float(b.get("v")) or 0.0
        if o is None or h is None or l is None or c is None:
            continue
        opens.append(o)
        highs.append(h)
        lows.append(l)
        closes.append(c)
        volumes.append(vol)
        times.append(b.get("t", ""))
    n = len(closes)
    if n == 0:
        return {
            "status": "NO_DATA",
            "bars_count": 0,
            "indicators": {},
            "signal": {"action": "NEUTRAL", "score": 0, "confidence": 0, "rationale": "No data"},
            "warnings": ["No valid bars"],
            "last_bar_time": None,
        }

    min_req = 50 if ("h" in (interval or "").lower() or "m" in (interval or "").lower()) else 200
    warnings = []
    if n < min_req:
        warnings.append(f"Bars {n} < {min_req}")

    ind = {}
    ind["sma20"] = safe_float(sma(closes, 20))
    ind["sma50"] = safe_float(sma(closes, 50))
    ind["sma200"] = safe_float(sma(closes, 200))
    ind.update(macd_calc(closes))
    ind["rsi14"] = safe_float(rsi_wilder(closes, 14))
    ind["volatility"] = safe_float(volatility_ann(closes, interval))
    atr_v = atr_calc(highs, lows, closes, 14)
    ind["atr"] = safe_float(atr_v)
    ind["atr_pct"] = round(atr_v / closes[-1] * 100, 4) if atr_v and closes[-1] > 0 else None
    ind.update(bollinger(closes, 20, 2.0))
    ind.update(stochastic(highs, lows, closes, 14, 3))
    ind["adx"] = safe_float(adx_calc(highs, lows, closes, 14))
    ind["obv_slope"] = safe_float(obv_slope(closes, volumes, 20))
    ind.update(find_sr(highs, lows, closes, 5, 50))
    ind["last_close"] = closes[-1]

    score = 0
    reasons = []
    cl = ind["last_close"]
    s50 = ind.get("sma50")
    s200 = ind.get("sma200")
    mh = ind.get("macd_hist")
    rsi = ind.get("rsi14")
    sk = ind.get("stoch_k")
    bbl = ind.get("bb_lower")
    bbu = ind.get("bb_upper")

    if cl is not None and s50 is not None:
        if cl > s50:
            score += 1
            reasons.append("Prix > SMA50")
        else:
            score -= 1
            reasons.append("Prix < SMA50")
    if s50 is not None and s200 is not None:
        if s50 > s200:
            score += 1
            reasons.append("SMA50 > SMA200")
        else:
            score -= 1
            reasons.append("SMA50 < SMA200")
    if mh is not None:
        if mh > 0:
            score += 1
            reasons.append("MACD Hist > 0")
        else:
            score -= 1
            reasons.append("MACD Hist < 0")
    if rsi is not None:
        if rsi < 30:
            score += 1
            reasons.append("RSI survente")
        elif rsi > 70:
            score -= 1
            reasons.append("RSI surachat")
    if sk is not None:
        if sk < 20:
            score += 1
            reasons.append("Stoch survente")
        elif sk > 80:
            score -= 1
            reasons.append("Stoch surachat")
    if cl is not None and bbl is not None and bbu is not None and (bbu - bbl) > 0:
        pos = (cl - bbl) / (bbu - bbl)
        if pos < 0.1:
            score += 1
            reasons.append("Prix sur BB basse")
        elif pos > 0.9:
            score -= 1
            reasons.append("Prix sur BB haute")

    action = "BUY" if score >= 2 else ("SELL" if score <= -2 else "NEUTRAL")
    confidence = round(abs(score) / 6 * 100, 1)
    signal = {"action": action, "score": score, "confidence": min(100, confidence), "rationale": ", ".join(reasons) if reasons else "Aucun signal clair"}
    status = "OK" if n >= min_req else "INSUFFICIENT_DATA"

    return sanitize(
        {
            "status": status,
            "bars_count": n,
            "warnings": warnings,
            "indicators": ind,
            "signal": signal,
            "last_bar_time": times[-1] if times else None,
        }
    )


def check_freshness(last_bar_time, interval, now=None):
    if not last_bar_time:
        return False, None, "NO_TIMESTAMP"
    if now is None:
        now = datetime.now(timezone.utc)
    try:
        t = last_bar_time
        if isinstance(t, str):
            t = t.replace("Z", "+00:00")
            if "+" not in t and "-" not in t[10:]:
                t += "+00:00"
            t = datetime.fromisoformat(t)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        age = now - t
        age_hours = round(age.total_seconds() / 3600, 1)
    except Exception:
        return False, None, "PARSE_ERROR"

    il = (interval or "").lower()
    if "h" in il or "m" in il:
        max_age_h = 72
        wd = now.weekday()
        hr = now.hour
        if wd < 5 and 7 <= hr <= 18:
            max_age_h = 3
        if age_hours > max_age_h:
            return False, age_hours, "STALE"
    else:
        if age_hours > 96:
            return False, age_hours, "STALE"
    return True, age_hours, "FRESH"


def pre_filter(h1_result, d1_ind):
    h1_status = (h1_result or {}).get("status")
    if h1_status in (None, "NO_DATA"):
        return False, "NO_H1_DATA"

    h1_sig = (h1_result or {}).get("signal", {}) or {}
    h1_ind = (h1_result or {}).get("indicators", {}) or {}

    h1_action = (h1_sig.get("action") or "").upper()
    h1_score = h1_sig.get("score", 0) or 0
    rsi = h1_ind.get("rsi14")
    d1_sma200 = (d1_ind or {}).get("sma200")
    d1_close = (d1_ind or {}).get("last_close")

    if not h1_action:
        return False, "NO_H1_SIGNAL"
    if h1_action == "SELL":
        return True, "SELL_SIGNAL_SAFETY"
    if h1_action == "NEUTRAL":
        return False, "NEUTRAL"

    if d1_sma200 is None or d1_close is None:
        return False, "NO_DAILY_CONTEXT"
    if d1_close > d1_sma200:
        return True, "BUY_IN_UPTREND"
    if abs(h1_score) >= 3 or (rsi is not None and rsi < 30):
        return True, "STRONG_REVERSAL_BUY"
    return False, "WEAK_BUY_AGAINST_TREND"


def fnv1a(s):
    h = 0x811C9DC5
    for c in s.encode():
        h ^= c
        h = (h * 0x01000193) & 0xFFFFFFFF
    return format(h, "08x")


def compute_sig_hash(symbol_key, h1_sig, h1_ind, d1_ind):
    parts = [symbol_key, (h1_sig.get("action", "") or ""), str(h1_sig.get("score", 0) or 0)]
    rsi_val = h1_ind.get("rsi14") if h1_ind else None
    rsi_val = 50 if rsi_val is None else rsi_val
    parts.append(str(round(rsi_val / 2) * 2))
    mh = (h1_ind or {}).get("macd_hist")
    mh = 0 if mh is None else mh
    parts.append("POS" if mh >= 0 else "NEG")
    if d1_ind:
        d1c = d1_ind.get("last_close", 0) or 0
        d1s200 = d1_ind.get("sma200", 0) or 0
        parts.append("ABOVE200" if d1c > d1s200 else "BELOW200")
    return fnv1a("|".join(parts))


def check_dedup(symbol_key, sig_hash, h1_action, con):
    ttl_buy = 240
    ttl_sell = 60
    row = con.execute(
        "SELECT sig_hash, last_ai_at, ttl_minutes FROM ai_dedup_cache WHERE symbol = ? AND interval_key = 'combined'",
        [symbol_key],
    ).fetchone()
    if row is None:
        return True, "NO_CACHE"

    old_hash, last_ai_at, ttl = row
    ttl = ttl or (ttl_sell if h1_action == "SELL" else ttl_buy)

    if old_hash != sig_hash:
        return True, "SIGNATURE_CHANGED"

    if last_ai_at:
        age_r = con.execute(
            "SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - CAST(? AS TIMESTAMP)))/60",
            [str(last_ai_at)],
        ).fetchone()
        age_min = age_r[0] if age_r else 999
        if age_min < ttl:
            return False, "UNCHANGED_WITHIN_TTL"
        return True, "TTL_EXPIRED"

    return True, "NO_TIMESTAMP"


def slim_response(resp, keep=3):
    if not isinstance(resp, dict):
        return {}
    r = dict(resp)
    bars = r.get("bars")
    if isinstance(bars, list) and len(bars) > keep * 2:
        r["bars_head"] = bars[:keep]
        r["bars_tail"] = bars[-keep:]
        r["bars_count"] = len(bars)
        r.pop("bars", None)
    return r


# ===================================================
# Main
# ===================================================
items = _items or []
results = []

IND_KEYS = [
    "sma20",
    "sma50",
    "sma200",
    "ema12",
    "ema26",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi14",
    "volatility",
    "last_close",
    "atr",
    "atr_pct",
    "bb_upper",
    "bb_lower",
    "bb_width",
    "stoch_k",
    "stoch_d",
    "adx",
    "obv_slope",
    "resistance",
    "support",
    "dist_res_pct",
    "dist_sup_pct",
]

for it in items:
    d = it.get("json", {}) or {}
    run_id = str(d.get("run_id", "") or "")
    identity = normalize_identity(d)
    symbol_internal = identity["symbol_internal"]
    symbol_yahoo = identity["symbol_yahoo"]
    asset_class = identity["asset_class"]

    if not symbol_internal or not symbol_yahoo:
        continue

    try:
        h1_resp = d.get("h1_response", {}) or {}
        d1_resp = d.get("d1_response", {}) or {}

        h1_interval = str(h1_resp.get("interval") or "1h")
        d1_interval = str(d1_resp.get("interval") or "1d")

        data_quality_flags = []

        # Symbol consistency check against expected Yahoo symbol.
        h1_sym = to_upper_text(h1_resp.get("symbol", ""))
        d1_sym = to_upper_text(d1_resp.get("symbol", ""))
        if h1_sym and h1_sym != to_upper_text(symbol_yahoo):
            data_quality_flags.append("H1_SYMBOL_MISMATCH")
        if d1_sym and d1_sym != to_upper_text(symbol_yahoo):
            data_quality_flags.append("D1_SYMBOL_MISMATCH")

        h1_result = compute_indicators(h1_resp.get("bars", []), h1_interval)
        d1_result = compute_indicators(d1_resp.get("bars", []), d1_interval)

        now_utc = datetime.now(timezone.utc)
        h1_fresh, h1_age_h, h1_freshness = check_freshness(h1_result.get("last_bar_time"), h1_interval, now_utc)
        d1_fresh, d1_age_h, d1_freshness = check_freshness(d1_result.get("last_bar_time"), d1_interval, now_utc)

        if not h1_fresh and h1_freshness == "STALE":
            h1_result["status"] = "STALE"
            h1_result.setdefault("warnings", []).append(f"H1 data is {h1_age_h}h old - STALE")
            data_quality_flags.append("STALE_H1")
        if not d1_fresh and d1_freshness == "STALE":
            d1_result["status"] = "STALE"
            d1_result.setdefault("warnings", []).append(f"D1 data is {d1_age_h}h old - STALE")
            data_quality_flags.append("STALE_D1")

        if h1_result.get("status") in ("NO_DATA", "INSUFFICIENT_DATA"):
            data_quality_flags.append("MISSING_H1")
        if d1_result.get("status") in ("NO_DATA", "INSUFFICIENT_DATA"):
            data_quality_flags.append("MISSING_D1")

        if asset_class == "FX" and has_fx_daily_issue(d1_resp.get("bars", [])):
            data_quality_flags.append("FX_YAHOO_DAILY_ISSUE")

        h1_ind = h1_result.get("indicators", {}) or {}
        d1_ind = d1_result.get("indicators", {}) or {}
        h1_sig = h1_result.get("signal", {}) or {}
        d1_sig = d1_result.get("signal", {}) or {}

        atr_pips_h1 = None
        atr_pips_d1 = None
        stop_pips_suggested = None
        pip_size = safe_float(identity.get("pip_size"))
        if asset_class == "FX" and pip_size and pip_size > 0:
            if h1_ind.get("atr") is not None:
                atr_pips_h1 = round(float(h1_ind.get("atr")) / pip_size, 2)
            if d1_ind.get("atr") is not None:
                atr_pips_d1 = round(float(d1_ind.get("atr")) / pip_size, 2)
            if atr_pips_h1 is not None and atr_pips_h1 > 0:
                stop_pips_suggested = round(max(5.0, atr_pips_h1 * 1.5), 1)

        pass_ai, filter_reason = pre_filter(h1_result, d1_ind)
        if pass_ai and not h1_fresh:
            pass_ai = False
            filter_reason = "STALE_H1_DATA"

        dedup_key = symbol_internal
        sig_hash = compute_sig_hash(dedup_key, h1_sig, h1_ind, d1_ind)

        call_ai = False
        dedup_reason = "FILTERED_OUT"

        with db_con() as con:
            if pass_ai:
                call_ai, dedup_reason = check_dedup(dedup_key, sig_hash, (h1_sig.get("action") or ""), con)

            signal_id = run_id + "|" + symbol_internal
            row = {
                "id": signal_id,
                "run_id": run_id,
                "symbol": symbol_internal,
                "symbol_internal": symbol_internal,
                "symbol_yahoo": symbol_yahoo,
                "asset_class": asset_class,
                "workflow_date": datetime.now(timezone.utc).isoformat(),
                "base_ccy": identity.get("base_ccy"),
                "quote_ccy": identity.get("quote_ccy"),
                "pip_size": identity.get("pip_size"),
                "price_decimals": identity.get("price_decimals"),
                "trading_hours": identity.get("trading_hours"),
                "h1_date": h1_result.get("last_bar_time"),
                "h1_source": h1_resp.get("source", "unknown"),
                "h1_status": h1_result.get("status", "NO_DATA"),
                "h1_warnings": json.dumps(h1_result.get("warnings", [])),
                "h1_action": h1_sig.get("action"),
                "h1_score": h1_sig.get("score"),
                "h1_confidence": h1_sig.get("confidence"),
                "h1_rationale": h1_sig.get("rationale"),
                "d1_date": d1_result.get("last_bar_time"),
                "d1_source": d1_resp.get("source", "unknown"),
                "d1_status": d1_result.get("status", "NO_DATA"),
                "d1_warnings": json.dumps(d1_result.get("warnings", [])),
                "d1_action": d1_sig.get("action"),
                "d1_score": d1_sig.get("score"),
                "d1_confidence": d1_sig.get("confidence"),
                "d1_rationale": d1_sig.get("rationale"),
                "last_close": h1_ind.get("last_close") or d1_ind.get("last_close"),
                "atr_pips_h1": atr_pips_h1,
                "atr_pips_d1": atr_pips_d1,
                "stop_pips_suggested": stop_pips_suggested,
                "data_quality_flags": json.dumps(sorted(set(data_quality_flags))),
                "filter_reason": filter_reason,
                "pass_ai": pass_ai,
                "pass_pm": False,
                "sig_hash": sig_hash,
                "call_ai": call_ai,
                "dedup_reason": dedup_reason,
                "vector_status": "PENDING",
                "should_vectorize": True,
                "data_age_h1_hours": h1_age_h,
                "data_age_d1_hours": d1_age_h,
            }

            for k in IND_KEYS:
                row["h1_" + k] = h1_ind.get(k)
            for k in IND_KEYS:
                row["d1_" + k] = d1_ind.get(k)

            cols = list(row.keys())
            placeholders = ", ".join(["?"] * len(cols))
            con.execute("INSERT OR REPLACE INTO technical_signals (" + ", ".join(cols) + ") VALUES (" + placeholders + ")", list(row.values()))

        out = dict(row)
        h1_bars = h1_resp.get("bars", []) or []
        d1_bars = d1_resp.get("bars", []) or []
        out["h1_bars_60"] = h1_bars[-60:] if len(h1_bars) > 60 else h1_bars
        out["h1_bars_count"] = len(h1_bars)
        out["d1_bars_count"] = len(d1_bars)

        out["h1_response"] = slim_response(h1_resp, keep=3)
        out["d1_response"] = slim_response(d1_resp, keep=3)
        out["h1_indicators"] = h1_ind
        out["d1_indicators"] = d1_ind
        out["h1_signal"] = h1_sig
        out["d1_signal"] = d1_sig
        out["h1_freshness"] = h1_freshness
        out["d1_freshness"] = d1_freshness
        out["_status"] = "ok"
        results.append({"json": out})

    except Exception as e:
        results.append(
            {
                "json": {
                    "symbol": symbol_internal or "",
                    "symbol_yahoo": symbol_yahoo or "",
                    "run_id": run_id,
                    "_status": "error",
                    "error": str(e),
                    "call_ai": False,
                    "pass_ai": False,
                    "pass_pm": False,
                }
            }
        )

return results
