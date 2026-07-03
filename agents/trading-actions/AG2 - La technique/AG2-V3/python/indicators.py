"""
AG2-V3 Technical Indicators Engine
===================================
Fixes vs V1:
  - RSI: Wilder smoothing (exponential) instead of simple average
  - EMA: SMA warmup for first N periods instead of values[0] init
  - Scoring: symmetric thresholds (BUY >= +2, SELL <= -2)
  - Support/Resistance: pivot-based detection instead of naive max/min
  - New indicators: Bollinger Bands, Stochastic, ADX, OBV slope
  - Single code path for both H1 and D1 (DRY)

Usage in n8n Python node:
  from indicators import compute_all_indicators
  result = compute_all_indicators(bars, interval="1h")
"""

import math
from typing import List, Dict, Optional, Any, Tuple


# =============================================================================
# Safe math helpers
# =============================================================================

def safe_float(v) -> Optional[float]:
    """Convert to float, returning None for NaN/Inf/None."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def sanitize(d: dict) -> dict:
    """Replace NaN/Inf with None in output dict."""
    out = {}
    for k, v in d.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        elif isinstance(v, dict):
            out[k] = sanitize(v)
        else:
            out[k] = v
    return out


# =============================================================================
# Core indicator functions
# =============================================================================

def sma(values: List[float], period: int) -> Optional[float]:
    """Simple Moving Average over last `period` values."""
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema_series(values: List[float], period: int) -> List[float]:
    """
    EMA series with SMA warmup (fix M3: proper initialization).
    First EMA value = SMA of first `period` values.
    """
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    # SMA warmup
    initial_sma = sum(values[:period]) / period
    result = [initial_sma]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def rsi_wilder(closes: List[float], period: int = 14) -> Optional[float]:
    """
    RSI with Wilder's exponential smoothing (fix M1).
    Uses the standard Wilder method: avg_gain and avg_loss are
    exponentially smoothed with factor (period-1)/period.
    """
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # First average: simple mean of first `period` deltas
    gains = [max(0, d) for d in deltas[:period]]
    losses = [max(0, -d) for d in deltas[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # Wilder smoothing for remaining deltas
    for d in deltas[period:]:
        avg_gain = (avg_gain * (period - 1) + max(0, d)) / period
        avg_loss = (avg_loss * (period - 1) + max(0, -d)) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd_calc(closes: List[float]) -> Dict[str, Optional[float]]:
    """MACD with properly initialized EMAs (fix M3)."""
    ema12 = ema_series(closes, 12)
    ema26 = ema_series(closes, 26)

    if not ema12 or not ema26:
        return {"ema12": None, "ema26": None, "macd": None,
                "macd_signal": None, "macd_hist": None}

    # Align: ema12 starts at index 12, ema26 at index 26
    # MACD = EMA12 - EMA26 (aligned from index 26)
    offset = 26 - 12  # = 14
    if len(ema12) <= offset:
        return {"ema12": safe_float(ema12[-1]) if ema12 else None,
                "ema26": safe_float(ema26[-1]) if ema26 else None,
                "macd": None, "macd_signal": None, "macd_hist": None}

    macd_line = [ema12[i + offset] - ema26[i] for i in range(len(ema26))]
    signal_line = ema_series(macd_line, 9)

    macd_val = safe_float(macd_line[-1]) if macd_line else None
    signal_val = safe_float(signal_line[-1]) if signal_line else None
    hist_val = None
    if macd_val is not None and signal_val is not None:
        hist_val = round(macd_val - signal_val, 6)

    return {
        "ema12": safe_float(ema12[-1]),
        "ema26": safe_float(ema26[-1]),
        "macd": safe_float(macd_val),
        "macd_signal": safe_float(signal_val),
        "macd_hist": safe_float(hist_val),
    }


def atr_calc(highs: List[float], lows: List[float], closes: List[float],
             period: int = 14) -> Optional[float]:
    """Average True Range (Wilder smoothing)."""
    if len(closes) < period + 1:
        return None

    trs = []
    for i in range(1, len(closes)):
        h = highs[i]
        l = lows[i]
        cp = closes[i - 1]
        tr = max(h - l, abs(h - cp), abs(l - cp))
        trs.append(tr)

    if len(trs) < period:
        return None

    # First ATR = simple average
    atr_val = sum(trs[:period]) / period
    # Wilder smoothing
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period

    return atr_val


def bollinger_bands(closes: List[float], period: int = 20,
                    num_std: float = 2.0) -> Dict[str, Optional[float]]:
    """Bollinger Bands: middle = SMA, upper/lower = SMA ± num_std × StdDev."""
    if len(closes) < period:
        return {"bb_upper": None, "bb_lower": None, "bb_width": None}

    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    std = math.sqrt(variance)

    upper = middle + num_std * std
    lower = middle - num_std * std
    width = (upper - lower) / middle * 100 if middle != 0 else None

    return {
        "bb_upper": round(upper, 4),
        "bb_lower": round(lower, 4),
        "bb_width": round(width, 4) if width is not None else None,
    }


def stochastic(highs: List[float], lows: List[float], closes: List[float],
               k_period: int = 14, d_period: int = 3) -> Dict[str, Optional[float]]:
    """Stochastic Oscillator (%K and %D)."""
    if len(closes) < k_period + d_period - 1:
        return {"stoch_k": None, "stoch_d": None}

    k_values = []
    for i in range(k_period - 1, len(closes)):
        window_h = highs[i - k_period + 1:i + 1]
        window_l = lows[i - k_period + 1:i + 1]
        hh = max(window_h)
        ll = min(window_l)
        if hh == ll:
            k_values.append(50.0)
        else:
            k_values.append((closes[i] - ll) / (hh - ll) * 100)

    if len(k_values) < d_period:
        return {"stoch_k": safe_float(k_values[-1]) if k_values else None,
                "stoch_d": None}

    # %D = SMA of %K
    d_val = sum(k_values[-d_period:]) / d_period

    return {
        "stoch_k": round(k_values[-1], 2),
        "stoch_d": round(d_val, 2),
    }


def adx_calc(highs: List[float], lows: List[float], closes: List[float],
             period: int = 14) -> Optional[float]:
    """
    Average Directional Index.
    Measures trend strength regardless of direction (0-100).
    """
    if len(closes) < period * 2 + 1:
        return None

    plus_dm = []
    minus_dm = []
    tr_list = []

    for i in range(1, len(closes)):
        h = highs[i]
        l = lows[i]
        ph = highs[i - 1]
        pl = lows[i - 1]
        cp = closes[i - 1]

        up = h - ph
        down = pl - l

        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        tr_list.append(max(h - l, abs(h - cp), abs(l - cp)))

    if len(tr_list) < period:
        return None

    # Wilder smoothing for +DM, -DM, TR
    def wilder_smooth(values, p):
        s = sum(values[:p])
        result = [s]
        for v in values[p:]:
            s = s - s / p + v
            result.append(s)
        return result

    sm_tr = wilder_smooth(tr_list, period)
    sm_plus = wilder_smooth(plus_dm, period)
    sm_minus = wilder_smooth(minus_dm, period)

    # DI+ and DI-
    dx_list = []
    for i in range(len(sm_tr)):
        if sm_tr[i] == 0:
            continue
        di_plus = sm_plus[i] / sm_tr[i] * 100
        di_minus = sm_minus[i] / sm_tr[i] * 100
        di_sum = di_plus + di_minus
        if di_sum == 0:
            dx_list.append(0)
        else:
            dx_list.append(abs(di_plus - di_minus) / di_sum * 100)

    if len(dx_list) < period:
        return None

    # ADX = Wilder smooth of DX
    adx_vals = wilder_smooth(dx_list, period)
    return round(adx_vals[-1] / period, 2) if adx_vals else None


def obv_slope(closes: List[float], volumes: List[float],
              lookback: int = 20) -> Optional[float]:
    """
    OBV slope: normalized slope of On-Balance Volume over last `lookback` bars.
    Positive = accumulation, Negative = distribution.
    """
    if len(closes) < lookback + 1 or len(volumes) < lookback + 1:
        return None

    # Compute OBV
    obv = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])

    # Linear regression slope of last `lookback` OBV values
    segment = obv[-lookback:]
    n = len(segment)
    x_mean = (n - 1) / 2.0
    y_mean = sum(segment) / n
    num = sum((i - x_mean) * (segment[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.0

    slope = num / den
    # Normalize by average volume to make comparable across stocks
    avg_vol = sum(volumes[-lookback:]) / lookback
    if avg_vol == 0:
        return 0.0
    return round(slope / avg_vol, 4)


def find_support_resistance(highs: List[float], lows: List[float],
                            closes: List[float], window: int = 5,
                            lookback: int = 50) -> Dict[str, Optional[float]]:
    """
    Pivot-based support/resistance detection (fix M5).
    Identifies swing highs and swing lows within the lookback period,
    then returns the nearest resistance above price and support below price.
    """
    if len(closes) < lookback or len(highs) < lookback:
        return {"resistance": None, "support": None,
                "dist_res_pct": None, "dist_sup_pct": None}

    price = closes[-1]
    h = highs[-lookback:]
    l = lows[-lookback:]

    swing_highs = []
    swing_lows = []

    for i in range(window, len(h) - window):
        # Swing high: higher than `window` bars on each side
        if all(h[i] >= h[i - j] for j in range(1, window + 1)) and \
           all(h[i] >= h[i + j] for j in range(1, window + 1)):
            swing_highs.append(h[i])

        # Swing low: lower than `window` bars on each side
        if all(l[i] <= l[i - j] for j in range(1, window + 1)) and \
           all(l[i] <= l[i + j] for j in range(1, window + 1)):
            swing_lows.append(l[i])

    # Nearest resistance above price
    resistances = sorted([sh for sh in swing_highs if sh > price])
    resistance = resistances[0] if resistances else (max(h) if h else None)

    # Nearest support below price
    supports = sorted([sl for sl in swing_lows if sl < price], reverse=True)
    support = supports[0] if supports else (min(l) if l else None)

    dist_res = None
    dist_sup = None
    if resistance is not None and price > 0:
        dist_res = round((resistance - price) / price * 100, 2)
    if support is not None and price > 0:
        dist_sup = round((price - support) / price * 100, 2)

    return {
        "resistance": round(resistance, 4) if resistance is not None else None,
        "support": round(support, 4) if support is not None else None,
        "dist_res_pct": dist_res,
        "dist_sup_pct": dist_sup,
    }


def volatility_annualized(closes: List[float], interval: str) -> Optional[float]:
    """
    Annualized volatility from log returns.
    Fix M8: correct bars_per_day based on interval.
    """
    if len(closes) < 20:
        return None

    log_returns = [math.log(closes[i] / closes[i - 1])
                   for i in range(1, len(closes))
                   if closes[i - 1] > 0 and closes[i] > 0]

    if len(log_returns) < 10:
        return None

    mean = sum(log_returns) / len(log_returns)
    var = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
    std = math.sqrt(var)

    # Annualization factor depends on interval
    interval_lower = interval.lower()
    if interval_lower in ("1d", "5d"):
        factor = math.sqrt(252)
    elif interval_lower in ("1wk",):
        factor = math.sqrt(52)
    elif interval_lower in ("1mo", "3mo"):
        factor = math.sqrt(12)
    elif "h" in interval_lower or interval_lower in ("60m", "90m"):
        # European markets: ~8.5h/day (09:00-17:30)
        factor = math.sqrt(252 * 8)
    elif "m" in interval_lower:
        # Intraday minutes
        mins = int("".join(c for c in interval_lower if c.isdigit()) or "5")
        bars_per_day = int(8.5 * 60 / mins)
        factor = math.sqrt(252 * bars_per_day)
    else:
        factor = math.sqrt(252)

    return round(std * factor, 4)


# =============================================================================
# Signal scoring — symmetric (fix M2)
# =============================================================================

def compute_signal(indicators: dict) -> Dict[str, Any]:
    """
    Generate BUY/SELL/NEUTRAL signal with symmetric scoring.
    Fix M2: BUY threshold = +2, SELL threshold = -2 (symmetric).
    Added ADX and Bollinger inputs.

    Score range: -6 to +6
    """
    score = 0
    reasons = []

    close = indicators.get("last_close")
    sma50_val = indicators.get("sma50")
    sma200_val = indicators.get("sma200")
    macd_hist = indicators.get("macd_hist")
    rsi = indicators.get("rsi14")
    stoch_k = indicators.get("stoch_k")
    adx_val = indicators.get("adx")
    bb_lower = indicators.get("bb_lower")
    bb_upper = indicators.get("bb_upper")

    # Trend: price vs SMA50
    if close is not None and sma50_val is not None:
        if close > sma50_val:
            score += 1
            reasons.append("Prix > SMA50")
        else:
            score -= 1
            reasons.append("Prix < SMA50")

    # Trend structure: SMA50 vs SMA200
    if sma50_val is not None and sma200_val is not None:
        if sma50_val > sma200_val:
            score += 1
            reasons.append("SMA50 > SMA200")
        else:
            score -= 1
            reasons.append("SMA50 < SMA200")

    # Momentum: MACD histogram
    if macd_hist is not None:
        if macd_hist > 0:
            score += 1
            reasons.append("MACD Hist > 0")
        else:
            score -= 1
            reasons.append("MACD Hist < 0")

    # RSI extremes
    if rsi is not None:
        if rsi < 30:
            score += 1
            reasons.append("RSI survente (<30)")
        elif rsi > 70:
            score -= 1
            reasons.append("RSI surachat (>70)")

    # Stochastic extremes
    if stoch_k is not None:
        if stoch_k < 20:
            score += 1
            reasons.append("Stoch survente (<20)")
        elif stoch_k > 80:
            score -= 1
            reasons.append("Stoch surachat (>80)")

    # Bollinger squeeze: price near lower band = bullish signal
    if close is not None and bb_lower is not None and bb_upper is not None:
        bb_range = bb_upper - bb_lower
        if bb_range > 0:
            position = (close - bb_lower) / bb_range
            if position < 0.1:
                score += 1
                reasons.append("Prix sur BB basse")
            elif position > 0.9:
                score -= 1
                reasons.append("Prix sur BB haute")

    # Determine action — symmetric thresholds
    if score >= 2:
        action = "BUY"
    elif score <= -2:
        action = "SELL"
    else:
        action = "NEUTRAL"

    max_score = 6
    confidence = round(abs(score) / max_score * 100, 1)

    return {
        "action": action,
        "score": score,
        "confidence": min(100, confidence),
        "rationale": ", ".join(reasons) if reasons else "Aucun signal clair",
    }


# =============================================================================
# Main entry point
# =============================================================================

def compute_all_indicators(bars: List[Dict], interval: str = "1d") -> Dict[str, Any]:
    """
    Compute all technical indicators from OHLCV bars.

    Args:
        bars: List of dicts with keys {t, o, h, l, c, v}
        interval: "1h", "1d", etc.

    Returns:
        Dict with: status, indicators, signal, warnings, bars_count
    """
    warnings = []

    # Extract aligned OHLCV series, filtering out bars with null values
    opens, highs, lows, closes, volumes, timestamps = [], [], [], [], [], []
    for b in bars:
        o = safe_float(b.get("o"))
        h = safe_float(b.get("h"))
        l = safe_float(b.get("l"))
        c = safe_float(b.get("c"))
        v = safe_float(b.get("v")) or 0.0
        if o is None or h is None or l is None or c is None:
            continue
        opens.append(o)
        highs.append(h)
        lows.append(l)
        closes.append(c)
        volumes.append(v)
        timestamps.append(b.get("t", ""))

    n = len(closes)

    # Minimum bars check
    min_bars_required = 50 if "h" in interval.lower() or "m" in interval.lower() else 200
    if n == 0:
        return sanitize({
            "status": "NO_DATA",
            "bars_count": 0,
            "warnings": ["No valid bars"],
            "indicators": {},
            "signal": {"action": "NEUTRAL", "score": 0, "confidence": 0,
                       "rationale": "No data"},
        })

    if n < min_bars_required:
        warnings.append(f"Bars {n} < {min_bars_required}")

    # ── Compute all indicators ──
    ind = {}

    # Moving averages
    ind["sma20"] = safe_float(sma(closes, 20))
    ind["sma50"] = safe_float(sma(closes, 50))
    ind["sma200"] = safe_float(sma(closes, 200))

    # MACD (with EMA12, EMA26)
    macd_data = macd_calc(closes)
    ind.update(macd_data)

    # RSI (Wilder)
    ind["rsi14"] = safe_float(rsi_wilder(closes, 14))

    # Volatility
    ind["volatility"] = safe_float(volatility_annualized(closes, interval))

    # ATR
    atr_val = atr_calc(highs, lows, closes, 14)
    ind["atr"] = safe_float(atr_val)
    ind["atr_pct"] = round(atr_val / closes[-1] * 100, 4) \
        if atr_val is not None and closes[-1] > 0 else None

    # Bollinger Bands
    bb = bollinger_bands(closes, 20, 2.0)
    ind.update(bb)

    # Stochastic
    stoch = stochastic(highs, lows, closes, 14, 3)
    ind.update(stoch)

    # ADX
    ind["adx"] = safe_float(adx_calc(highs, lows, closes, 14))

    # OBV slope
    ind["obv_slope"] = safe_float(obv_slope(closes, volumes, 20))

    # Support / Resistance (pivot-based)
    sr = find_support_resistance(highs, lows, closes, window=5, lookback=50)
    ind.update(sr)

    # Last close
    ind["last_close"] = closes[-1]

    # ── Signal ──
    signal = compute_signal(ind)

    status = "OK"
    if n < min_bars_required:
        status = "INSUFFICIENT_DATA"

    return sanitize({
        "status": status,
        "bars_count": n,
        "warnings": warnings,
        "indicators": ind,
        "signal": signal,
        "last_bar_time": timestamps[-1] if timestamps else None,
    })
