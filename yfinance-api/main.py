# yfinance-api - FastAPI endpoint with on-disk cache + incremental fetch + smart cooldown
# VERSION: 2.0.0 (Reliable cache + per-symbol cooldown + staleness detection)

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

import os
import json
import time
import random
import math
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Config (ENV)
# =========================
APP_VERSION = "2.0.0"

TZ = os.getenv("TZ", "UTC")

DATA_DIR = os.getenv("YF_DATA_DIR", "/data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
STATE_DIR = os.path.join(DATA_DIR, "state")

# History depth kept on disk
YF_INIT_LOOKBACK_DAYS = int(os.getenv("YF_INIT_LOOKBACK_DAYS", "400"))
YF_OVERLAP_BARS = int(os.getenv("YF_OVERLAP_BARS", "3"))

# Per-symbol cooldown parameters (much less aggressive than v1)
COOLDOWN_BASE_SEC = int(os.getenv("YF_COOLDOWN_BASE_SEC", "300"))       # 5 min default
COOLDOWN_MAX_SEC = int(os.getenv("YF_COOLDOWN_MAX_SEC", "3600"))        # 1h max default
COOLDOWN_RATELIMIT_SEC = int(os.getenv("YF_COOLDOWN_RATELIMIT_SEC", "1800"))  # 30 min for actual 429

# Minimum seconds between any two Yahoo calls (global rate limit)
MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("YF_MIN_SECONDS_BETWEEN_CALLS", "5"))

# Cache staleness thresholds (seconds) - per interval
# If the newest bar in cache is older than this, we consider the cache stale
# and attempt a refresh. These account for market close hours/weekends.
CACHE_TTL_OVERRIDES = json.loads(os.getenv("YF_CACHE_TTL_JSON", "{}"))

# Short-lived caches for quote/options/calendar endpoints
QUOTE_CACHE_TTL_SEC = int(os.getenv("YF_QUOTE_CACHE_TTL_SEC", "20"))
OPTIONS_CACHE_TTL_SEC = int(os.getenv("YF_OPTIONS_CACHE_TTL_SEC", "300"))
CALENDAR_CACHE_TTL_SEC = int(os.getenv("YF_CALENDAR_CACHE_TTL_SEC", "1800"))

DEFAULT_CACHE_TTL = {
    "1m":  900,       # 15 min
    "2m":  1800,      # 30 min
    "5m":  1800,      # 30 min
    "15m": 2100,      # 35 min  (interval + 5-min buffer)
    "30m": 2400,      # 40 min  (interval + 10-min buffer)
    "60m": 3900,      # 1h 5min (interval + 5-min buffer)
    "1h":  3900,      # 1h 5min (interval + 5-min buffer)
    "90m": 5700,      # 1h 35min (interval + 5-min buffer)
    "1d":  14400,     # 4h  (covers market close + some buffer)
    "5d":  86400,     # 24h
    "1wk": 172800,    # 48h
    "1mo": 604800,    # 7d
    "3mo": 604800,    # 7d
}

# yfinance lookback caps
INTERVAL_MAX_LOOKBACK = {
    "1m": 7, "2m": 60, "5m": 60, "15m": 60, "30m": 60,
    "60m": 730, "1h": 730, "90m": 60,
    "1d": 5000, "5d": 5000, "1wk": 5000, "1mo": 5000, "3mo": 5000,
}

# Shared HTTP session for yfinance upstream calls with retry/backoff.
yf_session = requests.Session()
yf_retry = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
yf_adapter = HTTPAdapter(max_retries=yf_retry)
yf_session.mount("http://", yf_adapter)
yf_session.mount("https://", yf_adapter)

app = FastAPI(title="yfinance-api", version=APP_VERSION)


def _safe_num(v) -> Optional[float]:
    """Convert scalar-like value to finite float, else None."""
    try:
        if v is None or isinstance(v, bool):
            return None
        n = float(v)
        if not math.isfinite(n):
            return None
        return n
    except Exception:
        return None


def _safe_int(v) -> Optional[int]:
    n = _safe_num(v)
    if n is None:
        return None
    try:
        return int(round(n))
    except Exception:
        return None


def _safe_str(v, max_len: int = 4000) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    return s[:max_len]


def _pick_num(*vals) -> Optional[float]:
    for v in vals:
        n = _safe_num(v)
        if n is not None:
            return n
    return None


def _pick_str(*vals, max_len: int = 4000) -> str:
    for v in vals:
        s = _safe_str(v, max_len=max_len)
        if s:
            return s
    return ""


# =========================
# Helpers: FS + Time
# =========================
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _atomic_write(path: str, data: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp, path)


# =========================
# Per-symbol state management
# =========================
def _state_path(symbol: str, interval: str) -> str:
    safe = f"{symbol}__{interval}".replace("/", "_").replace(":", "_")
    return os.path.join(STATE_DIR, f"{safe}.state.json")


def _global_state_path() -> str:
    return os.path.join(STATE_DIR, "_global.json")


def _load_symbol_state(symbol: str, interval: str) -> dict:
    _safe_mkdir(STATE_DIR)
    path = _state_path(symbol, interval)
    if not os.path.exists(path):
        return {
            "cooldown_until_ts": 0,
            "cooldown_sec": 0,
            "cooldown_reason": None,
            "consecutive_errors": 0,
            "last_success_ts": 0,
        }
    try:
        with open(path, "r", encoding="utf-8") as f:
            st = json.load(f)
        st.setdefault("cooldown_until_ts", 0)
        st.setdefault("cooldown_sec", 0)
        st.setdefault("cooldown_reason", None)
        st.setdefault("consecutive_errors", 0)
        st.setdefault("last_success_ts", 0)
        return st
    except Exception:
        return {
            "cooldown_until_ts": 0,
            "cooldown_sec": 0,
            "cooldown_reason": None,
            "consecutive_errors": 0,
            "last_success_ts": 0,
        }


def _save_symbol_state(symbol: str, interval: str, st: dict) -> None:
    _safe_mkdir(STATE_DIR)
    _atomic_write(_state_path(symbol, interval), json.dumps(st, ensure_ascii=False))


def _load_global_state() -> dict:
    _safe_mkdir(STATE_DIR)
    path = _global_state_path()
    if not os.path.exists(path):
        return {"last_call_ts": 0}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_call_ts": 0}


def _save_global_state(st: dict) -> None:
    _safe_mkdir(STATE_DIR)
    _atomic_write(_global_state_path(), json.dumps(st, ensure_ascii=False))


def _is_in_cooldown(st: dict) -> bool:
    until_ts = int(st.get("cooldown_until_ts", 0) or 0)
    return until_ts > int(time.time())


def _set_cooldown(st: dict, symbol: str, interval: str, reason: str, is_ratelimit: bool = False) -> None:
    """Set cooldown with backoff. Rate-limit errors get longer cooldowns."""
    consecutive = int(st.get("consecutive_errors", 0) or 0) + 1
    st["consecutive_errors"] = consecutive

    if is_ratelimit:
        # Rate limit: start high, escalate slowly
        base = COOLDOWN_RATELIMIT_SEC
        cooldown = min(COOLDOWN_MAX_SEC, int(base * (1.2 ** min(consecutive - 1, 5))))
    else:
        # Other errors: start low, escalate
        base = COOLDOWN_BASE_SEC
        cooldown = min(COOLDOWN_MAX_SEC, int(base * (1.3 ** min(consecutive - 1, 5))))

    jitter = random.randint(0, max(10, cooldown // 10))
    cooldown = min(COOLDOWN_MAX_SEC, cooldown + jitter)

    st["cooldown_sec"] = cooldown
    st["cooldown_until_ts"] = int(time.time()) + cooldown
    st["cooldown_reason"] = reason[:240]
    _save_symbol_state(symbol, interval, st)
    print(f"[COOLDOWN] {symbol}/{interval}: {cooldown}s (reason: {reason[:80]}, consecutive: {consecutive})", flush=True)


def _clear_cooldown(st: dict, symbol: str, interval: str) -> None:
    st["cooldown_until_ts"] = 0
    st["cooldown_sec"] = 0
    st["cooldown_reason"] = None
    st["consecutive_errors"] = 0
    st["last_success_ts"] = int(time.time())
    _save_symbol_state(symbol, interval, st)


def _global_rate_limit_sleep() -> None:
    """Global rate limit: ensure minimum delay between ANY two Yahoo calls."""
    gst = _load_global_state()
    last = float(gst.get("last_call_ts", 0) or 0)
    now = time.time()
    delta = now - last
    if delta < MIN_SECONDS_BETWEEN_CALLS:
        wait = MIN_SECONDS_BETWEEN_CALLS - delta
        print(f"[RATE_LIMIT] Sleeping {wait:.1f}s (global)", flush=True)
        time.sleep(wait)
    gst["last_call_ts"] = time.time()
    _save_global_state(gst)


# =========================
# Cache staleness detection
# =========================
def _get_cache_ttl(interval: str) -> int:
    """Return cache TTL in seconds for a given interval."""
    if interval in CACHE_TTL_OVERRIDES:
        return int(CACHE_TTL_OVERRIDES[interval])
    return DEFAULT_CACHE_TTL.get(interval, 14400)


def _weekend_buffer(now: datetime, interval: str) -> float:
    """
    Return extra seconds to add to TTL to account for weekends/non-market hours.
    Euronext Paris: ~08:00-17:30 UTC (CET/CEST). We use 07:00-18:00 UTC as safe range.
    """
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    hour = now.hour

    is_daily_plus = interval in ("1d", "5d", "1wk", "1mo", "3mo")

    if weekday == 5:  # Saturday
        # Last bar: Friday ~17:00. Add ~24h buffer.
        return 1.0 * 86400 if is_daily_plus else 0.75 * 86400
    elif weekday == 6:  # Sunday
        # Last bar: Friday ~17:00. Add ~48h buffer.
        return 1.5 * 86400 if is_daily_plus else 1.5 * 86400
    elif weekday == 0:  # Monday
        if is_daily_plus:
            return 2 * 86400  # Friday daily bar → Monday
        # Intraday on Monday: before market open, last bar was Friday ~17:00
        if hour < 9:
            return 2.5 * 86400  # ~60h buffer (Fri 17:00 → Mon 09:00)
        return 0
    else:
        # Tue-Fri: off-hours buffer for intraday only
        if not is_daily_plus and (hour < 7 or hour >= 18):
            # Overnight: last bar was ~17:00 yesterday
            return 15 * 3600  # 15h buffer
        return 0


def _is_cache_stale(df: Optional[pd.DataFrame], interval: str) -> bool:
    """
    Check if cached data is stale based on the age of the most recent bar.
    Returns True if cache should be refreshed.
    Accounts for weekends and non-market hours (Euronext: ~07-18 UTC).
    """
    if df is None or df.empty:
        return True

    if "Datetime" not in df.columns:
        return True

    try:
        last_bar_dt = df["Datetime"].max()
        if pd.isna(last_bar_dt):
            return True

        # Make sure it's timezone-aware
        if last_bar_dt.tzinfo is None:
            last_bar_dt = last_bar_dt.replace(tzinfo=timezone.utc)

        now = _utcnow()
        age_seconds = (now - last_bar_dt).total_seconds()
        ttl = _get_cache_ttl(interval) + _weekend_buffer(now, interval)

        is_stale = age_seconds > ttl
        if is_stale:
            print(f"[CACHE] Stale: last bar age={age_seconds:.0f}s > ttl={ttl:.0f}s ({interval})", flush=True)
        else:
            print(f"[CACHE] Fresh: last bar age={age_seconds:.0f}s <= ttl={ttl:.0f}s ({interval})", flush=True)
        return is_stale

    except Exception as e:
        print(f"[CACHE] Error checking staleness: {e}", flush=True)
        return True


# =========================
# Cache I/O
# =========================
def _cache_key(symbol: str, interval: str) -> str:
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    safe_interval = interval.replace("/", "_")
    return f"{safe_symbol}__{safe_interval}"


def _cache_paths(symbol: str, interval: str) -> Tuple[str, str]:
    key = _cache_key(symbol, interval)
    csv_path = os.path.join(CACHE_DIR, f"{key}.csv.gz")
    meta_path = os.path.join(CACHE_DIR, f"{key}.meta.json")
    return csv_path, meta_path


def _read_cache(symbol: str, interval: str) -> Tuple[Optional[pd.DataFrame], Optional[dict]]:
    csv_path, meta_path = _cache_paths(symbol, interval)
    if not os.path.exists(csv_path):
        return None, None
    try:
        df = pd.read_csv(csv_path, compression="gzip")
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True, errors="coerce")
            df = df.dropna(subset=["Datetime"])
            df = df.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")
        meta = None
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        return df, meta
    except Exception as e:
        print(f"[CACHE] Error reading cache for {symbol}/{interval}: {e}", flush=True)
        return None, None


def _write_cache(symbol: str, interval: str, df: pd.DataFrame, meta: dict) -> None:
    csv_path, meta_path = _cache_paths(symbol, interval)
    df2 = df.copy()
    df2 = df2.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")
    _safe_mkdir(CACHE_DIR)
    df2.to_csv(csv_path, index=False, compression="gzip")
    _atomic_write(meta_path, json.dumps(meta, ensure_ascii=False))


def _safe_file_token(value: str) -> str:
    return str(value or "").strip().replace("/", "_").replace("\\", "_").replace(":", "_").replace(" ", "_")


def _json_cache_path(namespace: str, key: str) -> str:
    safe_key = _safe_file_token(key)
    return os.path.join(CACHE_DIR, f"{namespace}__{safe_key}.json")


def _read_json_cache(path: str, max_age_seconds: int) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        cached_ts = float(payload.get("_cached_at_ts", 0) or 0)
        if cached_ts <= 0:
            return None
        age = time.time() - cached_ts
        if age > max(1, int(max_age_seconds)):
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _write_json_cache(path: str, data: dict) -> None:
    _safe_mkdir(CACHE_DIR)
    payload = {
        "_cached_at": _utcnow().isoformat(),
        "_cached_at_ts": time.time(),
        "data": data,
    }
    _atomic_write(path, json.dumps(payload, ensure_ascii=False))


def _to_iso_from_epoch(value: Any) -> Optional[str]:
    ts = _safe_num(value)
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _normalize_symbol_list(symbol: Optional[str], symbols: Optional[str]) -> List[str]:
    raw_tokens: List[str] = []
    if symbol:
        raw_tokens.extend(str(symbol).replace(";", ",").split(","))
    if symbols:
        raw_tokens.extend(str(symbols).replace(";", ",").split(","))

    out: List[str] = []
    seen = set()
    for token in raw_tokens:
        t = _safe_str(token, max_len=32).upper()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("1", "true", "yes", "y"):
            return True
        if s in ("0", "false", "no", "n"):
            return False
    return default


# =========================
# yfinance normalization
# =========================
def _normalize_download_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes raw output of yf.download() and returns normalized columns:
    Datetime, Open, High, Low, Close, Volume (UTC), sorted + deduped.
    Handles MultiIndex gracefully.
    """
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])

    # Fix MultiIndex columns (yfinance returns ('Price', 'Ticker') tuples)
    if isinstance(df.columns, pd.MultiIndex):
        level_0_vals = set(df.columns.get_level_values(0))
        if "Close" in level_0_vals or "Open" in level_0_vals:
            df.columns = df.columns.get_level_values(0)
        else:
            df.columns = [c[-1] for c in df.columns]

    # Bring index into column
    df = df.reset_index()

    # Identify time column
    if "Datetime" not in df.columns:
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "Datetime"})
        else:
            first = df.columns[0]
            df = df.rename(columns={first: "Datetime"})

    if "Datetime" not in df.columns:
        raise ValueError(f"Cannot find time column. cols={list(df.columns)}")

    # Ensure OHLCV columns exist
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in df.columns:
            if c == "Close" and "Adj Close" in df.columns:
                df["Close"] = df["Adj Close"]
            else:
                df[c] = pd.NA

    out = df[["Datetime", "Open", "High", "Low", "Close", "Volume"]].copy()

    out["Datetime"] = pd.to_datetime(out["Datetime"], utc=True, errors="coerce")
    out = out.dropna(subset=["Datetime"])

    for c in ["Open", "High", "Low", "Close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")
    out = out.dropna(subset=["Open", "High", "Low", "Close"], how="all")

    return out


def _download_incremental(symbol: str, interval: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """Download data from yfinance with proper error handling."""
    print(f"[FETCH] {symbol} {interval} from {start_dt.isoformat()} to {end_dt.isoformat()}", flush=True)

    raw = yf.download(
        symbol,
        start=start_dt,
        end=end_dt,
        interval=interval,
        progress=False,
        threads=False,
        auto_adjust=False,
        session=yf_session,
    )
    return _normalize_download_df(raw)


def _df_to_bars(df: pd.DataFrame, max_bars: int):
    if df is None or df.empty:
        return []
    df = df.sort_values("Datetime")
    if max_bars and max_bars > 0:
        df = df.tail(int(max_bars))
    bars = []
    for row in df.itertuples(index=False):
        bars.append({
            "t": row.Datetime.isoformat().replace("+00:00", "Z"),
            "o": float(row.Open) if pd.notna(row.Open) else None,
            "h": float(row.High) if pd.notna(row.High) else None,
            "l": float(row.Low) if pd.notna(row.Low) else None,
            "c": float(row.Close) if pd.notna(row.Close) else None,
            "v": float(row.Volume) if pd.notna(row.Volume) else None,
        })
    return bars


# =========================
# Error classification
# =========================
RATELIMIT_MARKERS = [
    "429", "Too Many Requests", "RateLimit", "rate limit",
]

NETWORK_MARKERS = [
    "Failed to perform", "Could not connect", "Could not resolve host",
    "ConnectionError", "TimeoutError", "ReadTimeout", "ConnectTimeout",
]


def _classify_error(msg: str) -> str:
    """Classify error as 'ratelimit', 'network', 'empty', or 'other'."""
    if any(m.lower() in msg.lower() for m in RATELIMIT_MARKERS):
        return "ratelimit"
    if any(m.lower() in msg.lower() for m in NETWORK_MARKERS):
        return "network"
    if "EMPTY" in msg.upper() or "No data" in msg:
        return "empty"
    return "other"


def _json_error_response(status_code: int, content: dict) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=content)


def _extract_quote_snapshot(
    symbol: str,
    info: Dict[str, Any],
    fast: Dict[str, Any],
    qty: float = 0.0,
    side: str = "BUY",
) -> dict:
    regular_market_price = _pick_num(
        info.get("regularMarketPrice"),
        fast.get("lastPrice"),
        fast.get("regularMarketPrice"),
        info.get("currentPrice"),
        info.get("previousClose"),
        fast.get("previous_close"),
    )
    bid = _pick_num(info.get("bid"), fast.get("bid"))
    ask = _pick_num(info.get("ask"), fast.get("ask"))
    bid_size = _pick_num(info.get("bidSize"), fast.get("bidSize"))
    ask_size = _pick_num(info.get("askSize"), fast.get("askSize"))
    volume = _pick_num(
        info.get("regularMarketVolume"),
        fast.get("lastVolume"),
        info.get("volume"),
    )
    market_state = _pick_str(info.get("marketState"), max_len=64)
    currency = _pick_str(info.get("currency"), fast.get("currency"), max_len=16)
    exchange = _pick_str(info.get("exchange"), fast.get("exchange"), max_len=64)
    short_name = _pick_str(info.get("shortName"), info.get("longName"), max_len=200)

    spread_abs = None
    spread_pct = None
    mid = None
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        spread_abs = ask - bid
        mid = (ask + bid) / 2.0
        if mid > 0:
            spread_pct = (spread_abs / mid) * 100.0

    side_u = _pick_str(side, max_len=10).upper() or "BUY"
    if side_u not in ("BUY", "SELL"):
        side_u = "BUY"

    slippage_proxy_pct = None
    qty_num = _safe_num(qty)
    if spread_pct is not None and qty_num is not None and qty_num > 0:
        top_size = ask_size if side_u == "BUY" else bid_size
        base = spread_pct / 2.0
        if top_size is None or top_size <= 0:
            slippage_proxy_pct = base * 2.0
        else:
            size_penalty = max(0.0, (qty_num - top_size) / max(qty_num, 1.0))
            slippage_proxy_pct = base * (1.0 + size_penalty)

    delayed_by = _safe_int(info.get("exchangeDataDelayedBy"))
    regular_market_time = _to_iso_from_epoch(_pick_num(info.get("regularMarketTime"), fast.get("lastPriceTime")))
    last_trade_time = _to_iso_from_epoch(_pick_num(info.get("postMarketTime"), info.get("preMarketTime")))

    return {
        "ok": True,
        "symbol": symbol,
        "regularMarketPrice": regular_market_price,
        "bid": bid,
        "ask": ask,
        "bidSize": bid_size,
        "askSize": ask_size,
        "spreadAbs": spread_abs,
        "spreadPct": spread_pct,
        "mid": mid,
        "slippageProxyPct": slippage_proxy_pct,
        "currency": currency,
        "exchange": exchange,
        "marketState": market_state,
        "regularMarketTime": regular_market_time,
        "lastTradeTime": last_trade_time,
        "exchangeDataDelayedBy": delayed_by,
        "isDelayed": bool(delayed_by is not None and delayed_by > 0),
        "volume": volume,
        "shortName": short_name,
    }


def _parse_expiration_date(value: str) -> Optional[datetime]:
    s = _safe_str(value, max_len=32)
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _choose_expiration(expirations: List[str], requested: str, target_days: int) -> Tuple[Optional[str], Optional[str]]:
    exp_list = [_safe_str(x, max_len=32) for x in expirations if _safe_str(x, max_len=32)]
    if not exp_list:
        return None, "NO_EXPIRATIONS_AVAILABLE"

    if requested:
        req = _safe_str(requested, max_len=32)
        if req in exp_list:
            return req, None
        return None, f"REQUESTED_EXPIRATION_NOT_AVAILABLE: {req}"

    today = _utcnow().date()
    parsed: List[Tuple[str, int]] = []
    for exp in exp_list:
        dt = _parse_expiration_date(exp)
        if dt is None:
            continue
        parsed.append((exp, (dt.date() - today).days))

    if not parsed:
        return exp_list[0], "EXPIRATION_PARSE_FALLBACK"

    parsed_future = [x for x in parsed if x[1] >= 0]
    pool = parsed_future if parsed_future else parsed
    tgt = max(0, int(target_days))
    selected = min(pool, key=lambda x: abs(x[1] - tgt))[0]
    return selected, None


def _normalize_option_rows(df: Optional[pd.DataFrame], max_rows: int) -> List[dict]:
    if df is None or df.empty:
        return []

    wk = df.copy()
    if "strike" not in wk.columns:
        return []

    for c in ["strike", "lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility"]:
        if c not in wk.columns:
            wk[c] = pd.NA
        wk[c] = pd.to_numeric(wk[c], errors="coerce")

    if "contractSymbol" not in wk.columns:
        wk["contractSymbol"] = ""
    if "inTheMoney" not in wk.columns:
        wk["inTheMoney"] = False
    if "lastTradeDate" not in wk.columns:
        wk["lastTradeDate"] = pd.NaT
    wk["lastTradeDate"] = pd.to_datetime(wk["lastTradeDate"], errors="coerce", utc=True)
    wk = wk.dropna(subset=["strike"]).sort_values("strike")

    if max_rows > 0:
        wk = wk.head(int(max_rows))

    rows: List[dict] = []
    for rec in wk.to_dict(orient="records"):
        bid = _safe_num(rec.get("bid"))
        ask = _safe_num(rec.get("ask"))
        spread_abs = None
        spread_pct = None
        mid = None
        if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
            spread_abs = ask - bid
            mid = (ask + bid) / 2.0
            if mid > 0:
                spread_pct = (spread_abs / mid) * 100.0

        rows.append(
            {
                "contractSymbol": _safe_str(rec.get("contractSymbol"), max_len=64),
                "strike": _safe_num(rec.get("strike")),
                "lastPrice": _safe_num(rec.get("lastPrice")),
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spreadAbs": spread_abs,
                "spreadPct": spread_pct,
                "volume": _safe_int(rec.get("volume")),
                "openInterest": _safe_int(rec.get("openInterest")),
                "impliedVolatility": _safe_num(rec.get("impliedVolatility")),
                "inTheMoney": _safe_bool(rec.get("inTheMoney"), default=False),
                "lastTradeDate": rec.get("lastTradeDate").isoformat().replace("+00:00", "Z")
                if isinstance(rec.get("lastTradeDate"), pd.Timestamp) and pd.notna(rec.get("lastTradeDate"))
                else None,
            }
        )
    return rows


def _pick_nearest_iv(
    df: Optional[pd.DataFrame],
    target_strike: float,
    prefer_above: Optional[bool] = None,
) -> Tuple[Optional[float], Optional[float]]:
    if df is None or df.empty or target_strike is None:
        return None, None
    wk = df.copy()
    if "strike" not in wk.columns or "impliedVolatility" not in wk.columns:
        return None, None

    wk["strike"] = pd.to_numeric(wk["strike"], errors="coerce")
    wk["impliedVolatility"] = pd.to_numeric(wk["impliedVolatility"], errors="coerce")
    wk = wk.dropna(subset=["strike", "impliedVolatility"])
    wk = wk[wk["impliedVolatility"] > 0]
    if wk.empty:
        return None, None

    if prefer_above is True:
        above = wk[wk["strike"] >= float(target_strike)]
        if not above.empty:
            wk = above
    elif prefer_above is False:
        below = wk[wk["strike"] <= float(target_strike)]
        if not below.empty:
            wk = below

    wk["dist"] = (wk["strike"] - float(target_strike)).abs()
    row = wk.sort_values("dist").iloc[0]
    return _safe_num(row.get("impliedVolatility")), _safe_num(row.get("strike"))


def _json_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        ts = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(value, pd.Series):
        return [_json_scalar(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_json_scalar(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_scalar(v) for k, v in value.items()}
    if isinstance(value, (bool, int, float, str)):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return str(value)


# =========================
# Endpoints
# =========================
@app.get("/health")
def health():
    _safe_mkdir(DATA_DIR)
    _safe_mkdir(CACHE_DIR)
    _safe_mkdir(STATE_DIR)
    return {
        "ok": True,
        "ts": _utcnow().isoformat(),
        "version": APP_VERSION,
        "dataDir": DATA_DIR,
        "cacheDir": CACHE_DIR,
        "config": {
            "YF_INIT_LOOKBACK_DAYS": YF_INIT_LOOKBACK_DAYS,
            "YF_OVERLAP_BARS": YF_OVERLAP_BARS,
            "COOLDOWN_BASE_SEC": COOLDOWN_BASE_SEC,
            "COOLDOWN_MAX_SEC": COOLDOWN_MAX_SEC,
            "COOLDOWN_RATELIMIT_SEC": COOLDOWN_RATELIMIT_SEC,
            "MIN_SECONDS_BETWEEN_CALLS": MIN_SECONDS_BETWEEN_CALLS,
        },
        "cache_ttl": {**DEFAULT_CACHE_TTL, **CACHE_TTL_OVERRIDES},
    }


@app.get("/cache/status")
def cache_status(symbol: str = Query(...), interval: str = Query("1d")):
    """Check the status of cache for a given symbol/interval without fetching."""
    _safe_mkdir(CACHE_DIR)
    cached_df, cached_meta = _read_cache(symbol, interval)
    sym_state = _load_symbol_state(symbol, interval)
    cached_ok = cached_df is not None and not cached_df.empty

    result = {
        "symbol": symbol,
        "interval": interval,
        "cached": cached_ok,
        "stale": _is_cache_stale(cached_df, interval) if cached_ok else None,
        "rows": len(cached_df) if cached_ok else 0,
        "meta": cached_meta,
        "cooldown": {
            "active": _is_in_cooldown(sym_state),
            "until_ts": sym_state.get("cooldown_until_ts"),
            "reason": sym_state.get("cooldown_reason"),
            "consecutive_errors": sym_state.get("consecutive_errors"),
        },
    }
    if cached_ok:
        last_dt = cached_df["Datetime"].max()
        age = (_utcnow() - last_dt).total_seconds() if pd.notna(last_dt) else None
        result["last_bar"] = last_dt.isoformat() if pd.notna(last_dt) else None
        result["age_seconds"] = age
        result["ttl_seconds"] = _get_cache_ttl(interval)
    return result


@app.get("/cooldown/reset")
def reset_cooldown(symbol: str = Query(None), interval: str = Query(None)):
    """Reset cooldown for a symbol/interval or all if not specified."""
    _safe_mkdir(STATE_DIR)
    if symbol and interval:
        st = _load_symbol_state(symbol, interval)
        _clear_cooldown(st, symbol, interval)
        return {"ok": True, "reset": f"{symbol}/{interval}"}
    elif symbol:
        # Reset all intervals for this symbol
        import glob
        safe = symbol.replace("/", "_").replace(":", "_")
        pattern = os.path.join(STATE_DIR, f"{safe}__*.state.json")
        files = glob.glob(pattern)
        for f in files:
            try:
                os.remove(f)
            except Exception:
                pass
        return {"ok": True, "reset": f"{symbol}/*", "files_cleared": len(files)}
    else:
        # Reset all
        import glob
        files = glob.glob(os.path.join(STATE_DIR, "*.state.json"))
        for f in files:
            try:
                os.remove(f)
            except Exception:
                pass
        return {"ok": True, "reset": "ALL", "files_cleared": len(files)}


@app.get("/history")
def history(
    symbol: str = Query(...),
    interval: str = Query("1d"),
    lookback_days: int = Query(60),
    max_bars: int = Query(400),
    min_bars: int = Query(0),
    allow_stale: bool = Query(True),
    force_refresh: bool = Query(False),
):
    _safe_mkdir(DATA_DIR)
    _safe_mkdir(CACHE_DIR)
    _safe_mkdir(STATE_DIR)

    fetched_at = _utcnow().isoformat()
    sym_state = _load_symbol_state(symbol, interval)

    # Clamp lookback by interval capability
    cap = INTERVAL_MAX_LOOKBACK.get(interval, lookback_days)
    effective_lookback = min(int(lookback_days), int(cap))

    end_dt = _utcnow()
    start_dt = end_dt - timedelta(days=effective_lookback)

    cached_df, cached_meta = _read_cache(symbol, interval)
    cached_ok = cached_df is not None and not cached_df.empty
    cache_stale = _is_cache_stale(cached_df, interval) if cached_ok else True

    def _respond_from_df(df: pd.DataFrame, stale_flag: bool, err: Optional[str], source_override: str):
        if df is None or df.empty:
            dfw = pd.DataFrame(columns=["Datetime", "Open", "High", "Low", "Close", "Volume"])
        else:
            try:
                dfw = df[(df["Datetime"] >= start_dt) & (df["Datetime"] <= end_dt)]
            except Exception:
                dfw = df

        bars = _df_to_bars(dfw, max_bars)
        last = bars[-1] if bars else None
        return {
            "ok": True,
            "stale": bool(stale_flag),
            "symbol": symbol,
            "interval": interval,
            "lookback_days": effective_lookback,
            "source": source_override,
            "fetchedAt": fetched_at,
            "error": err,
            "count": len(bars),
            "dataAsOf": (last["t"] if last else None),
            "last": last,
            "bars": bars,
        }

    # === DECISION: Should we fetch fresh data? ===
    need_fetch = force_refresh or not cached_ok or cache_stale

    # If cache is fresh and we don't force refresh, return cache immediately
    if not need_fetch:
        print(f"[CACHE HIT] {symbol}/{interval}: fresh cache, serving directly", flush=True)
        return _respond_from_df(cached_df, False, None, "cache")

    # We need fresh data. Check per-symbol cooldown.
    if _is_in_cooldown(sym_state):
        until_ts = int(sym_state.get("cooldown_until_ts", 0) or 0)
        remaining = until_ts - int(time.time())
        print(f"[COOLDOWN] {symbol}/{interval}: in cooldown for {remaining}s more", flush=True)

        if cached_ok:
            return _respond_from_df(
                cached_df, True,
                f"COOLDOWN_SERVING_CACHE until_ts={until_ts} remaining={remaining}s",
                "cache_stale"
            )
        return {
            "ok": False,
            "stale": None,
            "symbol": symbol,
            "interval": interval,
            "lookback_days": effective_lookback,
            "source": "cooldown",
            "fetchedAt": fetched_at,
            "error": f"COOLDOWN_ACTIVE until_ts={until_ts} remaining={remaining}s reason={sym_state.get('cooldown_reason', '')}",
            "count": 0,
            "dataAsOf": None,
            "last": None,
            "bars": [],
        }

    # === FETCH from Yahoo Finance ===
    try:
        _global_rate_limit_sleep()

        # Incremental window: if we have cache, only fetch from last bar
        if cached_ok:
            last_dt = cached_df["Datetime"].max()
            overlap = max(0, int(YF_OVERLAP_BARS or 0))
            if interval in ("1d", "5d", "1wk", "1mo", "3mo"):
                start_fetch = max(start_dt, last_dt - timedelta(days=max(1, overlap)))
            else:
                start_fetch = max(start_dt, last_dt - timedelta(days=2))
        else:
            start_fetch = start_dt

        df_new = _download_incremental(symbol, interval, start_fetch, end_dt)

        # Merge with cache
        if cached_ok and df_new is not None and not df_new.empty:
            df_all = pd.concat([cached_df, df_new], ignore_index=True)
        elif df_new is not None and not df_new.empty:
            df_all = df_new
        elif cached_ok:
            # Fetch returned empty but we have cache - this is normal outside market hours
            # Don't trigger cooldown for empty responses
            print(f"[FETCH] {symbol}/{interval}: empty response, keeping cache", flush=True)
            # Update cache metadata to avoid re-fetching too soon
            meta = cached_meta or {}
            meta["lastCheckAt"] = _utcnow().isoformat()
            _, meta_path = _cache_paths(symbol, interval)
            _atomic_write(meta_path, json.dumps(meta, ensure_ascii=False))
            # Mark success (empty is not an error outside market hours)
            _clear_cooldown(sym_state, symbol, interval)
            return _respond_from_df(cached_df, True, "UPSTREAM_EMPTY_CACHE_OK", "cache_checked")
        else:
            raise RuntimeError("UPSTREAM_EMPTY_NO_CACHE")

        # Normalize final merged DataFrame
        if "Datetime" not in df_all.columns:
            raise RuntimeError(f"MISSING_DATETIME_COLUMN cols={list(df_all.columns)}")

        df_all["Datetime"] = pd.to_datetime(df_all["Datetime"], utc=True, errors="coerce")
        df_all = df_all.dropna(subset=["Datetime"])
        df_all = df_all.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")

        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c not in df_all.columns:
                df_all[c] = pd.NA
            df_all[c] = pd.to_numeric(df_all[c], errors="coerce")

        df_all = df_all.dropna(subset=["Open", "High", "Low", "Close"], how="all")

        if df_all.empty:
            if cached_ok:
                print(f"[FETCH] {symbol}/{interval}: all bars NaN after merge, keeping cache", flush=True)
                _clear_cooldown(sym_state, symbol, interval)
                return _respond_from_df(cached_df, True, "UPSTREAM_EMPTY_AFTER_CLEAN_CACHE_OK", "cache_checked")
            raise RuntimeError("UPSTREAM_EMPTY_AFTER_CLEAN")

        # Trim to keep only configured history depth
        keep_days = max(int(YF_INIT_LOOKBACK_DAYS or 400), int(effective_lookback))
        df_all = df_all[df_all["Datetime"] >= (end_dt - timedelta(days=keep_days + 5))]

        # Write updated cache
        last_ts = df_all["Datetime"].max()
        meta = {
            "symbol": symbol,
            "interval": interval,
            "updatedAt": _utcnow().isoformat(),
            "lookbackDaysStored": int(keep_days + 5),
            "rows": int(len(df_all)),
            "lastTs": (last_ts.isoformat().replace("+00:00", "Z") if pd.notna(last_ts) else None),
            "source": "yahoo_finance_yfinance",
        }
        _write_cache(symbol, interval, df_all, meta)

        # Build response
        resp = _respond_from_df(df_all, False, None, "yahoo_finance_yfinance")

        # min_bars check
        if min_bars and resp["count"] < int(min_bars):
            if allow_stale and cached_ok:
                return _respond_from_df(
                    cached_df, True,
                    f"NOT_ENOUGH_BARS_UPSTREAM count={resp['count']} min={min_bars}",
                    "cache"
                )
            return {
                "ok": False, "stale": None, "symbol": symbol, "interval": interval,
                "lookback_days": effective_lookback, "source": "yahoo_finance_yfinance",
                "fetchedAt": fetched_at,
                "error": f"NOT_ENOUGH_BARS count={resp['count']} min={min_bars}",
                "count": resp["count"], "dataAsOf": resp["dataAsOf"],
                "last": resp["last"], "bars": resp["bars"],
            }

        # Success: clear cooldown
        _clear_cooldown(sym_state, symbol, interval)
        print(f"[SUCCESS] {symbol}/{interval}: {resp['count']} bars, last={resp['dataAsOf']}", flush=True)
        return resp

    except Exception as e:
        msg = str(e)
        print(f"[ERROR] {symbol}/{interval}: {msg}", flush=True)

        error_type = _classify_error(msg)

        if error_type == "ratelimit":
            _set_cooldown(sym_state, symbol, interval, f"RATE_LIMIT: {msg}", is_ratelimit=True)
        elif error_type == "network":
            _set_cooldown(sym_state, symbol, interval, f"NETWORK: {msg}", is_ratelimit=False)
        elif error_type == "empty":
            # Empty data is not really an error - don't punish hard
            if not cached_ok:
                _set_cooldown(sym_state, symbol, interval, f"EMPTY_NO_CACHE: {msg}", is_ratelimit=False)
            # If we have cache, just return it without setting cooldown
        else:
            _set_cooldown(sym_state, symbol, interval, f"ERROR: {msg}", is_ratelimit=False)

        if cached_ok:
            return _respond_from_df(
                cached_df, True,
                f"{error_type.upper()}: {msg[:160]}",
                "cache_stale"
            )

        return {
            "ok": False, "stale": None, "symbol": symbol, "interval": interval,
            "lookback_days": effective_lookback, "source": "yahoo_finance_yfinance",
            "fetchedAt": fetched_at, "error": f"{error_type.upper()}: {msg}",
            "count": 0, "dataAsOf": None, "last": None, "bars": [],
        }


# --- Metadata endpoint ---
@app.get("/info")
def get_info(symbol: str = Query(...)):
    symbol = _safe_str(symbol, max_len=32).upper()
    try:
        _global_rate_limit_sleep()
        tick = yf.Ticker(symbol, session=yf_session)
        isin = _safe_str(getattr(tick, "isin", ""), max_len=64)
        info = tick.info or {}
        fast = dict(tick.fast_info or {})
        if not info and not fast:
            return _json_error_response(
                status_code=404,
                content={
                    "ok": False,
                    "symbol": symbol,
                    "error": "NO_MARKET_DATA_FOUND",
                    "sector": "",
                    "industry": "",
                    "isin": "",
                    "shortName": "",
                    "country": "",
                    "quoteType": "",
                    "quote": {},
                    "fetchedAt": _utcnow().isoformat(),
                    "source": "yahoo_finance_yfinance",
                },
            )
        quote = _extract_quote_snapshot(symbol, info, fast)
        return {
            "ok": True,
            "symbol": symbol,
            "sector": _safe_str(info.get("sector"), max_len=200),
            "industry": _safe_str(info.get("industry"), max_len=200),
            "isin": isin if isin and isin != "-" else _safe_str(info.get("isin"), max_len=64),
            "shortName": _safe_str(info.get("shortName"), max_len=200),
            "country": _safe_str(info.get("country"), max_len=100),
            "quoteType": _safe_str(info.get("quoteType"), max_len=64),
            "quote": quote,
            "fetchedAt": _utcnow().isoformat(),
            "source": "yahoo_finance_yfinance",
        }
    except Exception as e:
        err = str(e)
        print(f"[ERROR INFO] {symbol}: {err}")
        return _json_error_response(
            status_code=500,
            content={
                "ok": False,
                "symbol": symbol,
                "error": err,
                "sector": "",
                "industry": "",
                "isin": "",
                "shortName": "",
                "country": "",
                "quoteType": "",
                "quote": {},
                "fetchedAt": _utcnow().isoformat(),
                "source": "yahoo_finance_yfinance",
            },
        )


@app.get("/quote")
def get_quote(
    symbol: str = Query(None),
    symbols: str = Query(None),
    side: str = Query("BUY"),
    qty: float = Query(0.0, ge=0.0),
    max_age_seconds: int = Query(QUOTE_CACHE_TTL_SEC, ge=1, le=300),
    force_refresh: bool = Query(False),
):
    symbol_list = _normalize_symbol_list(symbol, symbols)
    if not symbol_list:
        return {
            "ok": False,
            "error": "NO_SYMBOL_PROVIDED",
            "quotes": [],
            "count": 0,
        }

    results: List[dict] = []
    success = 0
    side_u = _safe_str(side, max_len=10).upper()
    if side_u not in ("BUY", "SELL"):
        side_u = "BUY"

    for sym in symbol_list:
        cache_path = _json_cache_path("quote", sym)
        if not force_refresh:
            cached = _read_json_cache(cache_path, max_age_seconds=max_age_seconds)
            if cached is not None:
                row = dict(cached)
                row["source"] = "cache"
                results.append(row)
                if row.get("ok"):
                    success += 1
                continue

        try:
            _global_rate_limit_sleep()
            tick = yf.Ticker(sym, session=yf_session)
            info = tick.info or {}
            fast = dict(tick.fast_info or {})
            row = _extract_quote_snapshot(sym, info, fast, qty=qty, side=side_u)
            row["fetchedAt"] = _utcnow().isoformat()
            row["source"] = "yahoo_finance_yfinance"
            _write_json_cache(cache_path, row)
            results.append(row)
            success += 1
        except Exception as e:
            err = str(e)
            print(f"[ERROR QUOTE] {sym}: {err}", flush=True)

            stale = _read_json_cache(cache_path, max_age_seconds=86400)
            if stale is not None:
                row = dict(stale)
                row["source"] = "cache_stale"
                row["stale"] = True
                row["error"] = err
                results.append(row)
                if row.get("ok"):
                    success += 1
                continue

            results.append(
                {
                    "ok": False,
                    "symbol": sym,
                    "error": err,
                    "source": "yahoo_finance_yfinance",
                }
            )

    return {
        "ok": success > 0,
        "allOk": success == len(results),
        "partial": 0 < success < len(results),
        "count": len(results),
        "successCount": success,
        "failedCount": len(results) - success,
        "fetchedAt": _utcnow().isoformat(),
        "quotes": results,
    }


@app.get("/options")
def get_options(
    symbol: str = Query(...),
    expiration: str = Query(""),
    target_days: int = Query(30, ge=0, le=3650),
    max_rows_per_side: int = Query(250, ge=10, le=2000),
    max_age_seconds: int = Query(OPTIONS_CACHE_TTL_SEC, ge=10, le=3600),
    force_refresh: bool = Query(False),
):
    symbol = _safe_str(symbol, max_len=32).upper()
    expiration = _safe_str(expiration, max_len=32)
    cache_key = f"{symbol}|{expiration or 'AUTO'}|{target_days}|{max_rows_per_side}"
    cache_path = _json_cache_path("options", cache_key)

    if not force_refresh:
        cached = _read_json_cache(cache_path, max_age_seconds=max_age_seconds)
        if cached is not None:
            out = dict(cached)
            out["source"] = "cache"
            return out

    try:
        _global_rate_limit_sleep()
        tick = yf.Ticker(symbol, session=yf_session)
        expirations = list(tick.options or [])
        selected_exp, exp_warning = _choose_expiration(
            expirations=expirations,
            requested=expiration,
            target_days=target_days,
        )
        if not selected_exp:
            return {
                "ok": False,
                "symbol": symbol,
                "error": exp_warning or "NO_EXPIRATION_SELECTED",
                "expirations": expirations,
                "fetchedAt": _utcnow().isoformat(),
            }

        _global_rate_limit_sleep()
        chain = tick.option_chain(selected_exp)
        calls_df = getattr(chain, "calls", pd.DataFrame())
        puts_df = getattr(chain, "puts", pd.DataFrame())

        info = {}
        fast = {}
        try:
            info = tick.info or {}
        except Exception:
            info = {}
        try:
            fast = dict(tick.fast_info or {})
        except Exception:
            fast = {}

        quote = _extract_quote_snapshot(symbol, info, fast)
        spot = quote.get("regularMarketPrice")

        iv_call_atm = None
        iv_put_atm = None
        strike_call_atm = None
        strike_put_atm = None
        iv_call_otm_5 = None
        iv_put_otm_5 = None
        strike_call_otm_5 = None
        strike_put_otm_5 = None
        if spot is not None and spot > 0:
            iv_call_atm, strike_call_atm = _pick_nearest_iv(calls_df, target_strike=spot, prefer_above=None)
            iv_put_atm, strike_put_atm = _pick_nearest_iv(puts_df, target_strike=spot, prefer_above=None)
            iv_call_otm_5, strike_call_otm_5 = _pick_nearest_iv(calls_df, target_strike=spot * 1.05, prefer_above=True)
            iv_put_otm_5, strike_put_otm_5 = _pick_nearest_iv(puts_df, target_strike=spot * 0.95, prefer_above=False)

        iv_candidates = [x for x in [iv_call_atm, iv_put_atm] if x is not None]
        iv_atm = sum(iv_candidates) / len(iv_candidates) if iv_candidates else None
        skew_put_minus_call_5 = (
            iv_put_otm_5 - iv_call_otm_5
            if iv_put_otm_5 is not None and iv_call_otm_5 is not None
            else None
        )

        call_oi_total = None
        put_oi_total = None
        call_vol_total = None
        put_vol_total = None
        if calls_df is not None and not calls_df.empty and "openInterest" in calls_df.columns:
            call_oi_total = _safe_int(pd.to_numeric(calls_df["openInterest"], errors="coerce").fillna(0).sum())
        if puts_df is not None and not puts_df.empty and "openInterest" in puts_df.columns:
            put_oi_total = _safe_int(pd.to_numeric(puts_df["openInterest"], errors="coerce").fillna(0).sum())
        if calls_df is not None and not calls_df.empty and "volume" in calls_df.columns:
            call_vol_total = _safe_int(pd.to_numeric(calls_df["volume"], errors="coerce").fillna(0).sum())
        if puts_df is not None and not puts_df.empty and "volume" in puts_df.columns:
            put_vol_total = _safe_int(pd.to_numeric(puts_df["volume"], errors="coerce").fillna(0).sum())

        put_call_oi_ratio = None
        put_call_volume_ratio = None
        if call_oi_total is not None and call_oi_total > 0 and put_oi_total is not None:
            put_call_oi_ratio = float(put_oi_total) / float(call_oi_total)
        if call_vol_total is not None and call_vol_total > 0 and put_vol_total is not None:
            put_call_volume_ratio = float(put_vol_total) / float(call_vol_total)

        exp_dt = _parse_expiration_date(selected_exp)
        days_to_expiry = (exp_dt.date() - _utcnow().date()).days if exp_dt else None

        calls_rows = _normalize_option_rows(calls_df, max_rows=max_rows_per_side)
        puts_rows = _normalize_option_rows(puts_df, max_rows=max_rows_per_side)

        warnings: List[str] = []
        if exp_warning:
            warnings.append(exp_warning)
        if not calls_rows and not puts_rows:
            warnings.append("EMPTY_OPTION_CHAIN")
        if iv_atm is None:
            warnings.append("ATM_IV_NOT_AVAILABLE")

        out = {
            "ok": True,
            "symbol": symbol,
            "source": "yahoo_finance_yfinance",
            "fetchedAt": _utcnow().isoformat(),
            "expirationRequested": expiration or None,
            "expirationSelected": selected_exp,
            "targetDays": int(target_days),
            "daysToExpiration": days_to_expiry,
            "expirations": expirations,
            "underlying": {
                "regularMarketPrice": quote.get("regularMarketPrice"),
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "currency": quote.get("currency"),
                "marketState": quote.get("marketState"),
                "regularMarketTime": quote.get("regularMarketTime"),
            },
            "metrics": {
                "ivAtm": iv_atm,
                "ivAtmCall": iv_call_atm,
                "ivAtmPut": iv_put_atm,
                "strikeAtmCall": strike_call_atm,
                "strikeAtmPut": strike_put_atm,
                "ivOtmCall5Pct": iv_call_otm_5,
                "ivOtmPut5Pct": iv_put_otm_5,
                "strikeOtmCall5Pct": strike_call_otm_5,
                "strikeOtmPut5Pct": strike_put_otm_5,
                "skewPutMinusCall5Pct": skew_put_minus_call_5,
                "callOpenInterestTotal": call_oi_total,
                "putOpenInterestTotal": put_oi_total,
                "putCallOiRatio": put_call_oi_ratio,
                "callVolumeTotal": call_vol_total,
                "putVolumeTotal": put_vol_total,
                "putCallVolumeRatio": put_call_volume_ratio,
            },
            "chain": {
                "callsTotalRows": int(len(calls_df)) if calls_df is not None else 0,
                "putsTotalRows": int(len(puts_df)) if puts_df is not None else 0,
                "maxRowsPerSide": int(max_rows_per_side),
                "calls": calls_rows,
                "puts": puts_rows,
            },
            "warnings": warnings,
        }
        _write_json_cache(cache_path, out)
        return out
    except Exception as e:
        err = str(e)
        print(f"[ERROR OPTIONS] {symbol}: {err}", flush=True)
        stale = _read_json_cache(cache_path, max_age_seconds=86400)
        if stale is not None:
            out = dict(stale)
            out["source"] = "cache_stale"
            out["stale"] = True
            out["error"] = err
            return out
        return _json_error_response(
            status_code=500,
            content={
                "ok": False,
                "symbol": symbol,
                "source": "yahoo_finance_yfinance",
                "fetchedAt": _utcnow().isoformat(),
                "error": err,
                "expirationRequested": expiration or None,
            },
        )


@app.get("/calendar")
def get_calendar(
    symbol: str = Query(...),
    earnings_limit: int = Query(8, ge=1, le=32),
    max_age_seconds: int = Query(CALENDAR_CACHE_TTL_SEC, ge=30, le=86400),
    force_refresh: bool = Query(False),
):
    symbol = _safe_str(symbol, max_len=32).upper()
    cache_path = _json_cache_path("calendar", symbol)

    if not force_refresh:
        cached = _read_json_cache(cache_path, max_age_seconds=max_age_seconds)
        if cached is not None:
            out = dict(cached)
            out["source"] = "cache"
            return out

    try:
        _global_rate_limit_sleep()
        tick = yf.Ticker(symbol, session=yf_session)

        calendar_raw: Any = {}
        try:
            calendar_raw = tick.calendar
        except Exception:
            calendar_raw = {}

        calendar_payload: dict = {}
        if isinstance(calendar_raw, pd.DataFrame):
            if not calendar_raw.empty:
                if calendar_raw.shape[1] == 1:
                    col = calendar_raw.columns[0]
                    calendar_payload = {
                        _safe_str(idx, max_len=64): _json_scalar(v)
                        for idx, v in calendar_raw[col].items()
                    }
                else:
                    calendar_payload = _json_scalar(calendar_raw.to_dict()) or {}
        elif isinstance(calendar_raw, dict):
            calendar_payload = _json_scalar(calendar_raw) or {}

        earnings_rows: List[dict] = []
        earnings_df = None
        try:
            _global_rate_limit_sleep()
            earnings_df = tick.get_earnings_dates(limit=earnings_limit)
        except Exception:
            try:
                earnings_df = getattr(tick, "earnings_dates", None)
            except Exception:
                earnings_df = None

        if isinstance(earnings_df, pd.DataFrame) and not earnings_df.empty:
            wk = earnings_df.copy()
            wk = wk.reset_index()
            if len(wk) > int(earnings_limit):
                wk = wk.head(int(earnings_limit))
            for rec in wk.to_dict(orient="records"):
                row = {}
                for k, v in rec.items():
                    key = _safe_str(k, max_len=64)
                    if not key:
                        continue
                    row[key] = _json_scalar(v)
                earnings_rows.append(row)

        next_earnings_date = None
        candidate_dates: List[str] = []
        for key, value in calendar_payload.items():
            if "earnings" in key.lower() and "date" in key.lower():
                if isinstance(value, list):
                    candidate_dates.extend([_safe_str(v, max_len=64) for v in value if _safe_str(v, max_len=64)])
                else:
                    s = _safe_str(value, max_len=64)
                    if s:
                        candidate_dates.append(s)

        for row in earnings_rows:
            for k, v in row.items():
                if "date" in k.lower():
                    s = _safe_str(v, max_len=64)
                    if s:
                        candidate_dates.append(s)

        today_utc = _utcnow().date()
        parsed_candidates: List[datetime] = []
        for item in candidate_dates:
            dt = pd.to_datetime(item, errors="coerce", utc=True)
            if pd.isna(dt):
                continue
            parsed_candidates.append(dt.to_pydatetime())

        future_dates = [d for d in parsed_candidates if d.date() >= today_utc]
        if future_dates:
            next_earnings_date = min(future_dates).isoformat().replace("+00:00", "Z")
        elif parsed_candidates:
            next_earnings_date = max(parsed_candidates).isoformat().replace("+00:00", "Z")

        out = {
            "ok": True,
            "symbol": symbol,
            "source": "yahoo_finance_yfinance",
            "fetchedAt": _utcnow().isoformat(),
            "calendar": calendar_payload,
            "earningsDates": earnings_rows,
            "nextEarningsDate": next_earnings_date,
        }
        _write_json_cache(cache_path, out)
        return out
    except Exception as e:
        err = str(e)
        print(f"[ERROR CALENDAR] {symbol}: {err}", flush=True)
        stale = _read_json_cache(cache_path, max_age_seconds=86400)
        if stale is not None:
            out = dict(stale)
            out["source"] = "cache_stale"
            out["stale"] = True
            out["error"] = err
            return out
        return _json_error_response(
            status_code=500,
            content={
                "ok": False,
                "symbol": symbol,
                "source": "yahoo_finance_yfinance",
                "fetchedAt": _utcnow().isoformat(),
                "error": err,
                "calendar": {},
                "earningsDates": [],
                "nextEarningsDate": None,
            },
        )


@app.get("/fundamentals")
def get_fundamentals(symbol: str = Query(...)):
    """Structured fundamental snapshot for AG3 (quality/valuation/consensus inputs)."""
    fetched_at = _utcnow().isoformat()
    symbol = _safe_str(symbol, max_len=32).upper()

    profile = {}
    price = {}
    valuation = {}
    profitability = {}
    growth = {}
    financial_health = {}
    consensus = {}
    dividends = {}

    try:
        _global_rate_limit_sleep()
        tick = yf.Ticker(symbol, session=yf_session)

        info = {}
        try:
            info = tick.info or {}
        except Exception:
            info = {}

        fast = {}
        try:
            fast = dict(tick.fast_info or {})
        except Exception:
            fast = {}

        isin = ""
        try:
            isin = _safe_str(tick.isin, max_len=64)
        except Exception:
            isin = ""

        current_price = _pick_num(
            info.get("currentPrice"),
            info.get("regularMarketPrice"),
            info.get("previousClose"),
            fast.get("lastPrice"),
            fast.get("regularMarketPrice"),
            fast.get("previous_close"),
        )

        profile = {
            "symbol": symbol,
            "shortName": _pick_str(info.get("shortName"), info.get("longName"), max_len=200),
            "longName": _pick_str(info.get("longName"), info.get("shortName"), max_len=300),
            "sector": _safe_str(info.get("sector"), max_len=200),
            "industry": _safe_str(info.get("industry"), max_len=200),
            "country": _safe_str(info.get("country"), max_len=100),
            "currency": _pick_str(info.get("currency"), fast.get("currency"), max_len=16),
            "exchange": _pick_str(info.get("exchange"), fast.get("exchange"), max_len=64),
            "website": _safe_str(info.get("website"), max_len=300),
            "isin": _pick_str(isin, info.get("isin"), max_len=64),
            "quoteType": _safe_str(info.get("quoteType"), max_len=64),
            "businessSummary": _safe_str(info.get("longBusinessSummary"), max_len=4000),
        }

        price = {
            "currentPrice": current_price,
            "marketCap": _pick_num(info.get("marketCap"), fast.get("marketCap")),
            "sharesOutstanding": _pick_num(info.get("sharesOutstanding"), fast.get("shares")),
            "beta": _safe_num(info.get("beta")),
            "fiftyTwoWeekLow": _pick_num(info.get("fiftyTwoWeekLow"), fast.get("yearLow")),
            "fiftyTwoWeekHigh": _pick_num(info.get("fiftyTwoWeekHigh"), fast.get("yearHigh")),
        }

        valuation = {
            "trailingPE": _safe_num(info.get("trailingPE")),
            "forwardPE": _safe_num(info.get("forwardPE")),
            "pegRatio": _safe_num(info.get("pegRatio")),
            "priceToBook": _safe_num(info.get("priceToBook")),
            "enterpriseToEbitda": _safe_num(info.get("enterpriseToEbitda")),
            "enterpriseToRevenue": _safe_num(info.get("enterpriseToRevenue")),
            "trailingEps": _safe_num(info.get("trailingEps")),
            "forwardEps": _safe_num(info.get("forwardEps")),
            "bookValue": _safe_num(info.get("bookValue")),
        }

        profitability = {
            "grossMargins": _safe_num(info.get("grossMargins")),
            "operatingMargins": _safe_num(info.get("operatingMargins")),
            "profitMargins": _safe_num(info.get("profitMargins")),
            "returnOnEquity": _safe_num(info.get("returnOnEquity")),
            "returnOnAssets": _safe_num(info.get("returnOnAssets")),
            "ebitdaMargins": _safe_num(info.get("ebitdaMargins")),
            "freeCashflow": _safe_num(info.get("freeCashflow")),
            "operatingCashflow": _safe_num(info.get("operatingCashflow")),
        }

        growth = {
            "revenueGrowth": _safe_num(info.get("revenueGrowth")),
            "earningsGrowth": _safe_num(info.get("earningsGrowth")),
            "earningsQuarterlyGrowth": _safe_num(info.get("earningsQuarterlyGrowth")),
        }

        financial_health = {
            "debtToEquity": _safe_num(info.get("debtToEquity")),
            "totalCash": _safe_num(info.get("totalCash")),
            "totalDebt": _safe_num(info.get("totalDebt")),
            "currentRatio": _safe_num(info.get("currentRatio")),
            "quickRatio": _safe_num(info.get("quickRatio")),
        }

        target_mean = _safe_num(info.get("targetMeanPrice"))
        target_high = _safe_num(info.get("targetHighPrice"))
        target_low = _safe_num(info.get("targetLowPrice"))
        rec_mean = _safe_num(info.get("recommendationMean"))
        analyst_count = _safe_int(info.get("numberOfAnalystOpinions"))

        upside_pct = None
        if current_price and target_mean:
            upside_pct = ((target_mean / current_price) - 1.0) * 100.0

        consensus = {
            "recommendationKey": _safe_str(info.get("recommendationKey"), max_len=64),
            "recommendationMean": rec_mean,
            "numberOfAnalystOpinions": analyst_count,
            "targetMeanPrice": target_mean,
            "targetHighPrice": target_high,
            "targetLowPrice": target_low,
            "upsidePctToTargetMean": upside_pct,
        }

        dividends = {
            "dividendYield": _safe_num(info.get("dividendYield")),
            "payoutRatio": _safe_num(info.get("payoutRatio")),
            "fiveYearAvgDividendYield": _safe_num(info.get("fiveYearAvgDividendYield")),
        }

        blocks = [
            profile,
            price,
            valuation,
            profitability,
            growth,
            financial_health,
            consensus,
            dividends,
        ]
        data_points = 0
        for block in blocks:
            for v in block.values():
                if v is None:
                    continue
                if isinstance(v, str) and not v:
                    continue
                data_points += 1

        max_points = sum(len(b.keys()) for b in blocks)
        coverage_pct = 0.0
        if max_points > 0:
            coverage_pct = round((data_points / max_points) * 100.0, 1)
        coverage_pct = max(0.0, min(100.0, coverage_pct))

        return {
            "ok": True,
            "symbol": symbol,
            "source": "yahoo_finance_yfinance",
            "fetchedAt": fetched_at,
            "profile": profile,
            "price": price,
            "valuation": valuation,
            "profitability": profitability,
            "growth": growth,
            "financialHealth": financial_health,
            "consensus": consensus,
            "dividends": dividends,
            "meta": {
                "dataPoints": data_points,
                "dataCoveragePctApprox": coverage_pct,
            },
        }
    except Exception as e:
        err = str(e)
        print(f"[ERROR FUNDAMENTALS] {symbol}: {err}", flush=True)
        return _json_error_response(
            status_code=500,
            content={
                "ok": False,
                "symbol": symbol,
                "source": "yahoo_finance_yfinance",
                "fetchedAt": fetched_at,
                "error": err,
                "profile": profile,
                "price": price,
                "valuation": valuation,
                "profitability": profitability,
                "growth": growth,
                "financialHealth": financial_health,
                "consensus": consensus,
                "dividends": dividends,
                "meta": {"dataPoints": 0, "dataCoveragePctApprox": 0.0},
            },
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
