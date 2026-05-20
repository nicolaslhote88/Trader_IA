import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
fred_key = ctx.get("fred_api_key") or os.getenv("FRED_API_KEY") or ""
timeout = min(4, max(2, int(ctx.get("macro_fetch_timeout_seconds") or 4)))
fetch_budget_seconds = min(50, max(10, int(ctx.get("macro_fetch_budget_seconds") or os.getenv("AG3_FX_MACRO_FETCH_BUDGET_SECONDS") or 45)))
max_workers = min(16, max(4, int(ctx.get("macro_fetch_max_workers") or os.getenv("AG3_FX_MACRO_FETCH_MAX_WORKERS") or 12)))
deadline = time.monotonic() + fetch_budget_seconds
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
EUR_MEMBER_PROXY_COUNTRIES = ["DEU", "FRA", "ITA", "ESP", "NLD"]
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


def remaining_timeout():
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("macro fetch budget exhausted")
    return max(1, min(timeout, remaining))


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
    with urlopen(url, timeout=remaining_timeout()) as resp:
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
    last_exc = None
    for _ in range(1):
        try:
            with urlopen(url, timeout=remaining_timeout()) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            break
        except Exception as exc:
            last_exc = exc
    else:
        raise last_exc
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


def worldbank_member_proxy_observations(countries, indicator, min_members=3):
    by_year = {}
    for country in countries:
        if time.monotonic() >= deadline:
            break
        try:
            for row in worldbank_observations(country, indicator):
                year = str(row.get("date") or "")
                by_year.setdefault(year, []).append(row["value"])
        except Exception:
            continue
    out = []
    for year, values in by_year.items():
        if len(values) >= min_members:
            out.append({"date": year, "value": sum(values) / len(values)})
    return sorted(out, key=lambda x: str(x.get("date") or ""), reverse=True)


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
    if not obs and currency == "EUR":
        obs = worldbank_member_proxy_observations(EUR_MEMBER_PROXY_COUNTRIES, indicator)
        if obs:
            country_used = "EUR_PROXY_DEU_FRA_ITA_ESP_NLD"
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


def fetch_factor_record(ccy, factor, meta):
    configured = FRED_SERIES.get(ccy, {})
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
            return rec, None
        except Exception as exc:
            return (
                observation(ccy, factor, meta["source"], series_id, None, None, "ERROR", meta["higher_is_bullish"], meta["weight"]),
                f"{ccy}.{factor}.{series_id}: {exc}",
            )
    if factor in WORLDBANK_SERIES and ccy in CURRENCY_COUNTRY:
        indicator, higher_is_bullish, weight = WORLDBANK_SERIES[factor]
        try:
            return worldbank_record(ccy, factor, indicator, higher_is_bullish, weight), None
        except Exception as exc:
            source = "WorldBankProxy" if factor == "policy_rate" else "WorldBank"
            return (
                observation(ccy, factor, source, indicator, None, None, "ERROR", higher_is_bullish, weight),
                f"{ccy}.{factor}.{indicator}: {exc}",
            )
    return observation(ccy, factor, meta["source"], None, None, None, "NOT_MAPPED", meta["higher_is_bullish"], meta["weight"]), None


tasks = [(ccy, factor, meta) for ccy in currencies for factor, meta in FACTOR_DEFAULTS.items()]
results_by_key = {}
executor = ThreadPoolExecutor(max_workers=max_workers)
future_map = {executor.submit(fetch_factor_record, ccy, factor, meta): (ccy, factor, meta) for ccy, factor, meta in tasks}
try:
    for future in as_completed(future_map, timeout=fetch_budget_seconds):
        ccy, factor, meta = future_map[future]
        try:
            rec, err = future.result()
        except Exception as exc:
            rec = observation(ccy, factor, meta["source"], None, None, None, "ERROR", meta["higher_is_bullish"], meta["weight"])
            err = f"{ccy}.{factor}: {exc}"
        results_by_key[(ccy, factor)] = rec
        if err:
            fetch_errors.append(err)
except Exception as exc:
    fetch_errors.append(f"macro_fetch_budget_exhausted: {exc}")
finally:
    executor.shutdown(wait=False, cancel_futures=True)

for ccy, factor, meta in tasks:
    rec = results_by_key.get((ccy, factor))
    if rec is None:
        rec = observation(ccy, factor, meta["source"], None, None, None, "ERROR", meta["higher_is_bullish"], meta["weight"])
        fetch_errors.append(f"{ccy}.{factor}: macro fetch budget exhausted")
    observations.append(rec)

ok_count = sum(1 for x in observations if x.get("data_status") == "OK")
status_by_currency = {ccy: {} for ccy in currencies}
for row in observations:
    ccy = str(row.get("currency") or "").upper()
    factor = str(row.get("factor") or "")
    if ccy in status_by_currency and factor:
        status_by_currency[ccy][factor] = str(row.get("data_status") or "").upper()

critical_factors = {"policy_rate", "real_yield"}
critical_ok_count = sum(
    1
    for ccy in currencies
    for factor in critical_factors
    if status_by_currency.get(ccy, {}).get(factor) == "OK"
)
currency_ok_count = sum(
    1
    for ccy in currencies
    if sum(1 for status in status_by_currency.get(ccy, {}).values() if status == "OK") >= 4
)
critical_coverage = critical_ok_count / max(1, len(currencies) * len(critical_factors))
currency_coverage = currency_ok_count / max(1, len(currencies))
macro_data_degraded = (
    ok_count < max(3, len(currencies) * 3)
    or critical_coverage < 0.50
    or currency_coverage < 0.75
)
macro_quality = {
    "ok_count": ok_count,
    "total_observations": len(observations),
    "critical_ok_count": critical_ok_count,
    "critical_total": len(currencies) * len(critical_factors),
    "critical_coverage": critical_coverage,
    "currency_ok_count": currency_ok_count,
    "currency_total": len(currencies),
    "currency_coverage": currency_coverage,
    "degraded_reasons": [],
}
if ok_count < max(3, len(currencies) * 3):
    macro_quality["degraded_reasons"].append("LOW_TOTAL_MACRO_COVERAGE")
if critical_coverage < 0.50:
    macro_quality["degraded_reasons"].append("LOW_POLICY_RATE_REAL_YIELD_COVERAGE")
if currency_coverage < 0.75:
    macro_quality["degraded_reasons"].append("LOW_CURRENCY_FACTOR_COVERAGE")

return [{
    "json": {
        **ctx,
        "macro_observations": observations,
        "macro_observation_ok_count": ok_count,
        "macro_quality": macro_quality,
        "macro_fetch_errors": fetch_errors,
        "macro_data_degraded": macro_data_degraded,
    }
}]
