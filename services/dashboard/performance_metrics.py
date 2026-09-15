"""EUR benchmark observations and cash-flow-neutral measurement contracts."""
import pandas as pd

BENCHMARK_DEFAULTS = {
    "CAC 40": {"ticker": "^FCHI", "currency": "EUR", "variant": "PRICE", "timezone": "Europe/Paris", "close_time": "17:40"},
    "S&P 500": {"ticker": "^GSPC", "currency": "USD", "variant": "PRICE", "timezone": "America/New_York", "close_time": "16:05"},
    "EURO STOXX 50": {"ticker": "^STOXX50E", "currency": "EUR", "variant": "PRICE", "timezone": "Europe/Paris", "close_time": "18:05"},
}


def benchmark_contract(ticker, supplied=None):
    known = next((dict(v) for v in BENCHMARK_DEFAULTS.values() if v["ticker"] == ticker), {})
    known.update(supplied or {})
    known["ticker"] = ticker
    return known


def close_observations(raw, timezone, close_time):
    df = raw[["timestamp", "close"]].copy()
    df["day"] = pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%Y-%m-%d")
    df["timestamp"] = pd.to_datetime(df["day"] + " " + close_time).dt.tz_localize(timezone).dt.tz_convert("UTC").astype("datetime64[ns, UTC]")
    return df.sort_values("timestamp").drop_duplicates("day", keep="last")


def benchmark_eur(raw, contract, fx=None, cutoff=None):
    """No same-day close can be used before its conservative availability time."""
    empty = pd.DataFrame(columns=["timestamp", "close"])
    if raw is None or raw.empty or not all(contract.get(k) for k in ("currency", "timezone", "close_time", "variant")):
        return empty
    df = close_observations(raw, contract["timezone"], contract["close_time"])
    if contract["currency"] == "USD":
        if fx is None or fx.empty:
            return empty
        # Yahoo EURUSD is USD per EUR; daily FX close is 17:00 New York.
        f = close_observations(fx, "America/New_York", "17:05").rename(columns={"timestamp": "fx_as_of", "close": "usd_per_eur"})
        df = df.merge(f[["day", "fx_as_of", "usd_per_eur"]], on="day", how="inner")
        df = df[df["usd_per_eur"] > 0].copy()
        df["close"] = df["close"] / df["usd_per_eur"]
        df["timestamp"] = df[["timestamp", "fx_as_of"]].max(axis=1)
    elif contract["currency"] != "EUR":
        return empty
    end = pd.Timestamp(cutoff) if cutoff is not None else pd.Timestamp.now(tz="UTC")
    return df[df["timestamp"] <= end].sort_values("timestamp")


def benchmark_at_nav_times(benchmark, nav):
    if benchmark.empty or nav.empty:
        return pd.DataFrame(columns=["timestamp", "close"])
    points = nav[["timestamp"]].copy()
    points["timestamp"] = pd.to_datetime(points["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    benchmark = benchmark.copy()
    benchmark["timestamp"] = pd.to_datetime(benchmark["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    result = pd.merge_asof(points.sort_values("timestamp"),
        benchmark.rename(columns={"timestamp": "benchmark_as_of"}).sort_values("benchmark_as_of"),
        left_on="timestamp", right_on="benchmark_as_of", direction="backward", tolerance=pd.Timedelta(days=4))
    return result.dropna(subset=["close"])


def flow_neutral_index(nav, flows):
    """Daily linked return, external flows assumed at interval end (explicit estimate)."""
    df = nav[["timestamp", "value"]].copy().sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if df.empty:
        return df
    events = flows.copy()
    if not events.empty:
        events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)
    index = [100.0]
    for i in range(1, len(df)):
        previous, current = df.iloc[i-1], df.iloc[i]
        flow = 0.0 if events.empty else events.loc[(events.timestamp > previous.timestamp) & (events.timestamp <= current.timestamp), "amount"].sum()
        if previous.value <= 0:
            index.append(float("nan"))
        else:
            index.append(index[-1] * (current.value - flow) / previous.value)
    df["value"] = index
    return df
