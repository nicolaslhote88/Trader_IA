"""Indicateurs macro comparables gratuits de la Banque mondiale.

Les séries annuelles complètent FRED sans être présentées comme des données
temps réel. Les transformations (PPP, REER, termes de l'échange et variations)
sont explicites dans le nom de source afin de rester auditables.
"""

from __future__ import annotations

import asyncio
import math
import os
from statistics import median
from typing import Any

import httpx


CURRENCY_COUNTRY = {
    "USD": "USA", "EUR": "EMU", "JPY": "JPN", "GBP": "GBR",
    "CHF": "CHE", "CAD": "CAN", "AUD": "AUS", "NZD": "NZL",
    "MXN": "MEX", "SEK": "SWE", "NOK": "NOR", "KRW": "KOR",
}

# Les agrégats EMU ne publient pas PPP/REER/termes de l'échange. L'Allemagne
# est utilisée uniquement pour ces trois métriques structurelles, avec un
# marquage proxy explicite dans la source.
COUNTRY_OVERRIDES = {
    "ppp_fair_value_usd": {"EUR": "DEU"},
    "reer_gap_pct": {"EUR": "DEU"},
    "terms_of_trade_score": {"EUR": "DEU"},
}

INDICATORS = {
    "current_account_pct_gdp": {"id": "BN.CAB.XOKA.GD.ZS", "unit": "pct_gdp"},
    "fiscal_balance_pct_gdp": {"id": "GC.NLD.TOTL.GD.ZS", "unit": "pct_gdp"},
    "gdp_growth_annual": {"id": "NY.GDP.MKTP.KD.ZG", "unit": "pct_yoy"},
    "cpi_yoy_annual": {"id": "FP.CPI.TOTL.ZG", "unit": "pct_yoy"},
    "unemployment_pct": {"id": "SL.UEM.TOTL.ZS", "unit": "pct_labor_force"},
    "ppp_fair_value_usd": {"id": "PA.NUS.PPP", "unit": "usd_per_lcu"},
    "reer_gap_pct": {"id": "PX.REX.REER", "unit": "pct_vs_recent_history"},
    "terms_of_trade_score": {"id": "TT.PRI.MRCH.XD.WD", "unit": "score_-1_1"},
}


def _as_of(year: Any) -> str:
    return f"{str(year)[:4]}-12-31"


def _latest_two(observations: list[dict]) -> tuple[dict | None, dict | None]:
    valid = [row for row in observations if row.get("value") is not None]
    valid.sort(key=lambda row: str(row.get("date") or ""), reverse=True)
    return (valid[0] if valid else None, valid[1] if len(valid) > 1 else None)


class WorldBankClient:
    def __init__(self) -> None:
        self.base_url = os.environ.get("WORLD_BANK_API_URL", "https://api.worldbank.org/v2").rstrip("/")
        self.timeout = float(os.environ.get("WORLD_BANK_TIMEOUT_SECONDS", "30"))
        self.max_retries = int(os.environ.get("WORLD_BANK_MAX_RETRIES", "3"))
        self.last_warnings: list[dict[str, Any]] = []

    async def _fetch(self, client: httpx.AsyncClient, indicator_id: str, countries: set[str]) -> list[dict]:
        url = f"{self.base_url}/country/{';'.join(sorted(countries))}/indicator/{indicator_id}"
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await client.get(url, params={"format": "json", "per_page": 1000, "mrv": 6})
                response.raise_for_status()
                payload = response.json()
                return payload[1] if isinstance(payload, list) and len(payload) > 1 and payload[1] else []
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt + 1 < self.max_retries:
                    await asyncio.sleep(0.4 * (attempt + 1))
        raise RuntimeError(f"WORLD_BANK_{indicator_id}_FAILED:{type(last_error).__name__}")

    @staticmethod
    def _transform(local_name: str, latest: dict, previous: dict | None) -> tuple[float | None, list[tuple[str, float, str]]]:
        value = float(latest["value"])
        derived: list[tuple[str, float, str]] = []
        if local_name == "ppp_fair_value_usd":
            return ((1.0 / value) if value > 0 else None), derived
        if local_name == "reer_gap_pct":
            # Hausse du REER = appréciation réelle. Un niveau sous sa récente
            # histoire est donc une sous-évaluation positive pour AG6.
            history = [float(row["value"]) for row in latest.get("_history", []) if row.get("value") is not None]
            benchmark = median(history[1:]) if len(history) > 1 else None
            return (((benchmark - value) / benchmark * 100.0) if benchmark else None), derived
        if local_name == "terms_of_trade_score":
            history = [float(row["value"]) for row in latest.get("_history", []) if row.get("value") is not None]
            benchmark = median(history[1:]) if len(history) > 1 else None
            change_pct = ((value / benchmark) - 1.0) * 100.0 if benchmark else None
            return (math.tanh(change_pct / 10.0) if change_pct is not None else None), derived
        if local_name == "gdp_growth_annual" and previous is not None:
            derived.append(("gdp_momentum_annual", value - float(previous["value"]), "pct_change"))
        if local_name == "unemployment_pct" and previous is not None:
            derived.append(("unemployment_change_pp_annual", value - float(previous["value"]), "pct_point"))
        return value, derived

    async def get_comparable_indicators(self) -> list[dict[str, Any]]:
        self.last_warnings = []
        rows: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            for local_name, spec in INDICATORS.items():
                currency_country = {
                    currency: COUNTRY_OVERRIDES.get(local_name, {}).get(currency, country)
                    for currency, country in CURRENCY_COUNTRY.items()
                }
                country_to_currency = {country: currency for currency, country in currency_country.items()}
                try:
                    observations = await self._fetch(client, spec["id"], set(country_to_currency))
                except RuntimeError as exc:
                    self.last_warnings.append({
                        "source": f"WORLD_BANK:{spec['id']}", "error_code": "SOURCE_UNAVAILABLE",
                        "detail": str(exc), "severity": "warning",
                    })
                    continue

                by_country: dict[str, list[dict]] = {}
                for observation in observations:
                    iso3 = str(observation.get("countryiso3code") or "").upper()
                    if iso3 in country_to_currency and observation.get("value") is not None:
                        by_country.setdefault(iso3, []).append(observation)

                for iso3, history in by_country.items():
                    history.sort(key=lambda row: str(row.get("date") or ""), reverse=True)
                    latest, previous = _latest_two(history)
                    if latest is None:
                        continue
                    enriched_latest = {**latest, "_history": history}
                    value, derived = self._transform(local_name, enriched_latest, previous)
                    if value is None or not math.isfinite(value):
                        continue
                    currency = country_to_currency[iso3]
                    is_proxy = iso3 != CURRENCY_COUNTRY[currency]
                    source = f"WORLD_BANK:{spec['id']}:DERIVED"
                    if is_proxy:
                        source += f":COUNTRY_PROXY_{iso3}"
                    base = {
                        "currency": currency, "as_of": _as_of(latest.get("date")),
                        "source": source, "country_code": iso3,
                    }
                    rows.append({**base, "indicator": local_name, "value": value, "unit": spec["unit"]})
                    for name, derived_value, unit in derived:
                        rows.append({**base, "indicator": name, "value": derived_value, "unit": unit})

                missing = sorted(set(currency_country) - {country_to_currency[c] for c in by_country})
                if missing:
                    self.last_warnings.append({
                        "source": f"WORLD_BANK:{spec['id']}", "error_code": "PARTIAL_COVERAGE",
                        "detail": f"missing_currencies={','.join(missing)}", "severity": "info",
                    })
        if not rows:
            raise RuntimeError("WORLD_BANK_ZERO_VALID_ROWS")
        return rows
