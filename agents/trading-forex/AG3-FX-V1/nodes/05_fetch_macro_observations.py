import json
import math
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
fred_key = ctx.get("fred_api_key") or os.getenv("FRED_API_KEY") or ""
timeout = max(2, int(ctx.get("macro_fetch_timeout_seconds") or 8))
currencies = ctx.get("currencies") or ["USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD"]

FRED_SERIES = {
    "USD": {
        "policy_rate": ("FEDFUNDS", True, 1.0),
        "inflation": ("CPIAUCSL", False, 0.8),
        "labor": ("UNRATE", False, 0.8),
    },
}
CURRENCY_COUNTRY = {
    "USD": "USA",
    "EUR": "EMU",
    "JPY": "JPN",
    "GBP": "GBR",
    "CHF": "CHE",
    "AUD": "AUS",
    "CAD": "CAN",
    "NZD": "NZL",
}
CURRENCY_COUNTRY_FALLBACKS = {
    "EUR": ["EMU", "EUU"],
}
WORLDBANK_SERIES = {
    # World Bank does not expose central-bank policy rates consistently. This
    # proxy keeps AG3 from dropping the monetary component entirely when FRED or
    # official central-bank APIs are unavailable.
    "policy_rate": ("FR.INR.LEND", True, 0.7),
    "real_yield": ("FR.INR.RINR", True, 1.0),
    "inflation": ("FP.CPI.TOTL.ZG", False, 0.8),
    "growth": ("NY.GDP.MKTP.KD.ZG", True, 0.8),
    "labor": ("SL.UEM.TOTL.ZS", False, 0.6),
    "external_balance": ("BN.CAB.XOKA.GD.ZS", True, 0.5),
}

FACTOR_DEFAULTS = {
    "policy_rate": {"source": "FRED", "higher_is_bullish": True, "weight": 1.0},
    "real_yield": {"source": "FRED", "higher_is_bullish": True, "weight": 1.0},
    "inflation": {"source": "FRED", "higher_is_bullish": False, "weight": 0.8},
    "growth": {"source": "FRED", "higher_is_bullish": True, "weight": 0.8},
    "labor": {"source": "FRED", "higher_is_bullish": False, "weight": 0.6},
    "external_balance": {"source": "WorldBank", "higher_is_bullish": True, "weight": 0.5},
}


def clamp(v, lo=-1.0, hi=1.0):
    try:
        n = float(v)
        if not math.isfinite(n):
            return None
        return max(lo, min(hi, n))
    except Exception:
        return None


def fred_observations(series_id):
    if not fred_key:
        return []
    params = {
        "series_id": series_id,
        "api_key": fred_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 18,
    }
    url = "https://api.stlouisfed.org/fred/series/observations?" + urlencode(params)
    with urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    out = []
    for row in payload.get("observations") or []:
        value = row.get("value")
        if value in (None, "."):
            continue
        try:
            out.append({"date": row.get("date"), "value": float(value)})
        except Exception:
            continue
    return out


def worldbank_observations(country, indicator):
    params = {"format": "json", "per_page": 20}
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?" + urlencode(params)
    with urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
    out = []
    for row in rows:
        value = row.get("value")
        if value is None:
            continue
        try:
            out.append({"date": str(row.get("date") or ""), "value": float(value)})
        except Exception:
            continue
    return out


def zscore_normalized(obs, higher_is_bullish=True):
    values = [x["value"] for x in obs if x.get("value") is not None]
    if len(values) < 4:
        return None
    latest = values[0]
    sample = values[:10]
    mean = sum(sample) / len(sample)
    var = sum((x - mean) ** 2 for x in sample) / len(sample)
    sd = math.sqrt(var)
    if sd <= 0:
        return 0.0
    z = (latest - mean) / sd
    if not higher_is_bullish:
        z *= -1
    return clamp(z / 2.0)


def observation(currency, factor, source, series_id, value, previous, status, higher_is_bullish=True, weight=1.0, normalized_override=None):
    delta_1m = value - previous if value is not None and previous is not None else None
    raw = None
    if delta_1m is not None:
        scale = max(abs(previous or 0.0), 1.0)
        raw = delta_1m / scale
        if not higher_is_bullish:
            raw *= -1
    normalized = normalized_override if normalized_override is not None else (clamp((raw or 0.0) * 6.0) if status == "OK" else None)
    return {
        "observation_id": f"{run_id}_{currency}_{factor}",
        "run_id": run_id,
        "currency": currency,
        "factor": factor,
        "source": source,
        "series_id": series_id,
        "observation_date": None,
        "value": value,
        "previous_value": previous,
        "delta_1m": delta_1m,
        "delta_3m": None,
        "delta_12m": None,
        "zscore": None,
        "normalized_score": normalized,
        "weight": weight,
        "higher_is_bullish": bool(higher_is_bullish),
        "data_status": status,
        "fetched_at": as_of,
    }


def worldbank_record(currency, factor, indicator, higher_is_bullish, weight):
    countries = CURRENCY_COUNTRY_FALLBACKS.get(currency) or [CURRENCY_COUNTRY[currency]]
    obs = []
    country_used = countries[0]
    for country in countries:
        obs = worldbank_observations(country, indicator)
        country_used = country
        if obs:
            break
    latest = obs[0] if obs else None
    previous = obs[1] if len(obs) > 1 else None
    norm = zscore_normalized(obs, higher_is_bullish=higher_is_bullish)
    status = "MISSING"
    if latest:
        try:
            year = int(str(latest.get("date") or "")[:4])
            current_year = int(str(as_of)[:4])
            status = "STALE" if year < current_year - 3 else "OK"
        except Exception:
            status = "OK"
    rec = observation(
        currency,
        factor,
        "WorldBankProxy" if factor == "policy_rate" else "WorldBank",
        f"{country_used}:{indicator}",
        latest.get("value") if latest else None,
        previous.get("value") if previous else None,
        status,
        higher_is_bullish,
        weight,
        normalized_override=norm if status == "OK" else None,
    )
    rec["observation_date"] = f"{latest.get('date')}-12-31" if latest else None
    return rec


observations = []
fetch_errors = []

for ccy in currencies:
    configured = FRED_SERIES.get(ccy, {})
    for factor, meta in FACTOR_DEFAULTS.items():
        if factor in configured:
            series_id, higher_is_bullish, weight = configured[factor]
            try:
                obs = fred_observations(series_id)
                latest = obs[0] if obs else None
                previous = obs[1] if len(obs) > 1 else None
                if latest:
                    rec = observation(
                        ccy,
                        factor,
                        meta["source"],
                        series_id,
                        latest.get("value"),
                        previous.get("value") if previous else None,
                        "OK",
                        higher_is_bullish,
                        weight,
                    )
                    rec["observation_date"] = latest.get("date")
                elif factor in WORLDBANK_SERIES and ccy in CURRENCY_COUNTRY:
                    indicator, wb_higher_is_bullish, wb_weight = WORLDBANK_SERIES[factor]
                    rec = worldbank_record(ccy, factor, indicator, wb_higher_is_bullish, wb_weight)
                else:
                    rec = observation(ccy, factor, meta["source"], series_id, None, None, "MISSING", higher_is_bullish, weight)
                observations.append(rec)
            except Exception as exc:
                fetch_errors.append(f"{ccy}.{factor}.{series_id}: {exc}")
                observations.append(observation(ccy, factor, meta["source"], series_id, None, None, "ERROR", meta["higher_is_bullish"], meta["weight"]))
        elif factor in WORLDBANK_SERIES and ccy in CURRENCY_COUNTRY:
            indicator, higher_is_bullish, weight = WORLDBANK_SERIES[factor]
            try:
                observations.append(worldbank_record(ccy, factor, indicator, higher_is_bullish, weight))
            except Exception as exc:
                fetch_errors.append(f"{ccy}.{factor}.{indicator}: {exc}")
                observations.append(observation(ccy, factor, "WorldBank", indicator, None, None, "ERROR", higher_is_bullish, weight))
        else:
            observations.append(observation(ccy, factor, meta["source"], None, None, None, "NOT_MAPPED", meta["higher_is_bullish"], meta["weight"]))

ok_count = sum(1 for x in observations if x.get("data_status") == "OK")
macro_data_degraded = ok_count < max(3, len(currencies) * 3)

return [{
    "json": {
        **ctx,
        "macro_observations": observations,
        "macro_observation_ok_count": ok_count,
        "macro_fetch_errors": fetch_errors,
        "macro_data_degraded": macro_data_degraded,
    }
}]
