"""Indicateurs macro comparables de la Banque mondiale.

La balance courante et le solde budgetaire sont ingeres en pourcentage du PIB,
jamais reconstruits a partir de montants absolus incompatibles entre pays.
"""

from __future__ import annotations

import os
from typing import Any

import httpx


CURRENCY_COUNTRY = {
    "USD": "USA", "EUR": "EMU", "JPY": "JPN", "GBP": "GBR",
    "CHF": "CHE", "CAD": "CAN", "AUD": "AUS", "NZD": "NZL",
    "MXN": "MEX", "SEK": "SWE", "NOK": "NOR", "KRW": "KOR",
}

INDICATORS = {
    "current_account_pct_gdp": "BN.CAB.XOKA.GD.ZS",
    "fiscal_balance_pct_gdp": "GC.BAL.CASH.GD.ZS",
}


class WorldBankClient:
    def __init__(self) -> None:
        self.base_url = os.environ.get("WORLD_BANK_API_URL", "https://api.worldbank.org/v2").rstrip("/")
        self.timeout = float(os.environ.get("WORLD_BANK_TIMEOUT_SECONDS", "30"))

    async def get_comparable_indicators(self) -> list[dict[str, Any]]:
        country_to_currency = {country: currency for currency, country in CURRENCY_COUNTRY.items()}
        countries = ";".join(sorted(country_to_currency))
        rows: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            for local_name, indicator_id in INDICATORS.items():
                response = await client.get(
                    f"{self.base_url}/country/{countries}/indicator/{indicator_id}",
                    params={"format": "json", "per_page": 1000, "mrv": 5},
                )
                response.raise_for_status()
                payload = response.json()
                observations = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
                latest_by_country: dict[str, dict] = {}
                for observation in observations or []:
                    iso3 = str(observation.get("countryiso3code") or "").upper()
                    if iso3 not in country_to_currency or observation.get("value") is None:
                        continue
                    if iso3 not in latest_by_country or str(observation.get("date") or "") > str(latest_by_country[iso3].get("date") or ""):
                        latest_by_country[iso3] = observation
                for iso3, observation in latest_by_country.items():
                    rows.append({
                        "currency": country_to_currency[iso3],
                        "indicator": local_name,
                        "value": observation.get("value"),
                        "as_of": f"{observation.get('date')}-12-31",
                        "unit": "pct_gdp",
                        "source": f"WORLD_BANK:{indicator_id}",
                        "country_code": iso3,
                    })
        if not rows:
            raise RuntimeError("WORLD_BANK_ZERO_VALID_ROWS")
        return rows
