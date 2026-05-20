"""
Client FRED API (Federal Reserve Economic Data).
Toutes les séries macroéconomiques clés pour le framework 3 piliers.
"""

import os
import logging
from datetime import date, timedelta
from typing import Optional

import httpx

logger = logging.getLogger("fred_client")

FRED_BASE = "https://api.stlouisfed.org/fred"

# Séries FRED par pays / indicateur
FRED_SERIES = {
    # Taux directeurs
    "policy_rate": {
        "USD": "FEDFUNDS",        # Fed Funds Rate
        "EUR": "ECBDFR",          # ECB Deposit Facility Rate
        "JPY": "IRSTCI01JPM156N", # Japan Overnight Call Rate
        "GBP": "IUDSOIA",         # Bank of England SONIA
        "CHF": "IRSTCI01CHM156N", # Swiss National Bank
        "CAD": "IRSTCI01CAM156N", # Bank of Canada
        "AUD": "IRSTCI01AUM156N", # Reserve Bank of Australia
        "NZD": "IRSTCI01NZM156N", # RBNZ
    },
    # PIB réel (croissance QoQ annualisée)
    "gdp_growth": {
        "USD": "A191RL1Q225SBEA",   # US Real GDP QoQ SAAR
        "EUR": "CLVMNACSCAB1GQEA19", # Euro Area Real GDP
        "JPY": "JPNRGDPEXP",         # Japan Real GDP
        "GBP": "UKNGDP",             # UK Real GDP
        "CAD": "NAEXKP01CAQ189S",    # Canada Real GDP
        "AUD": "NGDPRNSAXDCAUQ",     # Australia Real GDP
    },
    # CPI (Inflation YoY %)
    "cpi_yoy": {
        "USD": "CPIAUCSL",            # US CPI All Urban
        "EUR": "CP0000EZ19M086NEST",  # Euro Area HICP
        "JPY": "JPNCPIALLMINMEI",     # Japan CPI
        "GBP": "GBRCPIALLMINMEI",     # UK CPI
        "CHF": "CHECPIALLMINMEI",     # Switzerland CPI
        "CAD": "CANCPIALLMINMEI",     # Canada CPI
        "AUD": "AUSCPIALLQINMEI",     # Australia CPI
        "NZD": "NZLCPIALLQINMEI",     # New Zealand CPI
    },
    # Balance du compte courant (Milliards USD, trimestriel)
    "current_account": {
        "USD": "BOPCRNT",     # US Current Account Balance
        "EUR": "BPCA01EZQ02S", # Euro Area CA Balance
        "JPY": "JPNB6BLTT02STSAQ", # Japan CA Balance
        "GBP": "BPCA01GBQ02S",    # UK CA Balance
        "CAD": "BPCA01CAQ02S",    # Canada CA Balance
        "AUD": "BPCA01AUQ02S",    # Australia CA Balance
    },
    # Yields souverains (US Treasury via FRED)
    "yield_2y": {"USD": "DGS2"},
    "yield_5y": {"USD": "DGS5"},
    "yield_10y": {"USD": "DGS10"},
    "yield_30y": {"USD": "DGS30"},
    # Yields EU/autres via FRED
    "yield_10y_eur": {"EUR": "IRLTLT01EZM156N"},
    "yield_10y_gbp": {"GBP": "IRLTLT01GBM156N"},
    "yield_10y_jpy": {"JPY": "IRLTLT01JPM156N"},
    "yield_10y_cad": {"CAD": "IRLTLT01CAM156N"},
    "yield_10y_aud": {"AUD": "IRLTLT01AUM156N"},
    "yield_10y_chf": {"CHF": "IRLTLT01CHM156N"},
    "yield_2y_eur": {"EUR": "IRLTST01EZM156N"},
}


class FREDClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("FRED_API_KEY", "")
        if not self.api_key:
            logger.warning("FRED_API_KEY not set — FRED requests will fail")

    async def get_series(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Récupère les observations d'une série FRED."""
        if not self.api_key:
            return []
        if observation_start is None:
            observation_start = (date.today() - timedelta(days=365 * 5)).isoformat()
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end

        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"{FRED_BASE}/series/observations", params=params)
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as exc:
                detail = exc.response.text[:300].replace(self.api_key, "***")
                raise RuntimeError(f"FRED series {series_id} failed with HTTP {exc.response.status_code}: {detail}") from None
            data = r.json()
            obs = data.get("observations", [])
            result = []
            for o in obs:
                if o.get("value") not in (".", None, ""):
                    try:
                        result.append({"date": o["date"], "value": float(o["value"])})
                    except (ValueError, KeyError):
                        pass
            return result

    async def get_latest(self, series_id: str) -> Optional[dict]:
        """Retourne la dernière observation disponible."""
        try:
            obs = await self.get_series(series_id, limit=5)
        except RuntimeError as exc:
            logger.warning("%s", exc)
            return None
        return obs[0] if obs else None

    async def get_policy_rates(self) -> dict[str, dict]:
        """Taux directeurs pour toutes les grandes banques centrales."""
        results = {}
        for currency, series_id in FRED_SERIES["policy_rate"].items():
            latest = await self.get_latest(series_id)
            if latest:
                results[currency] = {
                    "rate_pct": latest["value"],
                    "as_of": latest["date"],
                    "series_id": series_id,
                }
        return results

    async def get_gdp_growth(self) -> dict[str, dict]:
        """Croissance PIB réel pour les principales économies."""
        results = {}
        for currency, series_id in FRED_SERIES["gdp_growth"].items():
            try:
                obs = await self.get_series(series_id, limit=8)
            except RuntimeError as exc:
                logger.warning("%s", exc)
                continue
            if len(obs) >= 2:
                latest = obs[0]["value"]
                prev = obs[1]["value"]
                results[currency] = {
                    "latest_qoq": latest,
                    "prev_qoq": prev,
                    "momentum": latest - prev,
                    "as_of": obs[0]["date"],
                    "series_id": series_id,
                }
        return results

    async def get_cpi(self) -> dict[str, dict]:
        """Inflation CPI YoY pour les principales devises."""
        results = {}
        for currency, series_id in FRED_SERIES["cpi_yoy"].items():
            try:
                obs = await self.get_series(series_id, limit=14)
            except RuntimeError as exc:
                logger.warning("%s", exc)
                continue
            if len(obs) >= 13:
                latest = obs[0]["value"]
                year_ago = obs[12]["value"] if len(obs) > 12 else None
                yoy = round((latest / year_ago - 1) * 100, 2) if year_ago else None
                results[currency] = {
                    "cpi_index": latest,
                    "yoy_pct": yoy,
                    "as_of": obs[0]["date"],
                    "series_id": series_id,
                }
        return results

    async def get_current_account(self) -> dict[str, dict]:
        """Balance du compte courant (déficit/excédent) en Mds USD."""
        results = {}
        for currency, series_id in FRED_SERIES["current_account"].items():
            try:
                obs = await self.get_series(series_id, limit=8)
            except RuntimeError as exc:
                logger.warning("%s", exc)
                continue
            if obs:
                latest = obs[0]["value"]
                results[currency] = {
                    "balance_bn_usd": latest,
                    "surplus": latest > 0,
                    "as_of": obs[0]["date"],
                    "series_id": series_id,
                }
        return results

    async def get_us_yield_curve(self) -> dict[str, dict]:
        """Courbe des taux US Treasuries (2Y, 5Y, 10Y, 30Y)."""
        results = {}
        for tenor, key in [("2Y", "yield_2y"), ("5Y", "yield_5y"), ("10Y", "yield_10y"), ("30Y", "yield_30y")]:
            series_id = FRED_SERIES[key].get("USD")
            if series_id:
                latest = await self.get_latest(series_id)
                if latest:
                    results[tenor] = {"yield_pct": latest["value"], "as_of": latest["date"]}
        return results

    async def get_g10_yields_10y(self) -> dict[str, dict]:
        """Yields 10Y souverains pour les devises G10."""
        mapping = {
            "USD": ("yield_10y", "USD"),
            "EUR": ("yield_10y_eur", "EUR"),
            "GBP": ("yield_10y_gbp", "GBP"),
            "JPY": ("yield_10y_jpy", "JPY"),
            "CAD": ("yield_10y_cad", "CAD"),
            "AUD": ("yield_10y_aud", "AUD"),
            "CHF": ("yield_10y_chf", "CHF"),
        }
        results = {}
        for currency, (key, ccy) in mapping.items():
            series_id = FRED_SERIES[key].get(ccy)
            if series_id:
                latest = await self.get_latest(series_id)
                if latest:
                    results[currency] = {"yield_10y_pct": latest["value"], "as_of": latest["date"]}
        return results

    async def get_g10_yields_2y(self) -> dict[str, dict]:
        """Yields 2Y souverains (proxy carry court terme)."""
        mapping = {
            "USD": ("yield_2y", "USD"),
            "EUR": ("yield_2y_eur", "EUR"),
        }
        results = {}
        for currency, (key, ccy) in mapping.items():
            series_id = FRED_SERIES[key].get(ccy)
            if series_id:
                latest = await self.get_latest(series_id)
                if latest:
                    results[currency] = {"yield_2y_pct": latest["value"], "as_of": latest["date"]}
        return results

    async def get_historical_cpi(self, currency: str, years: int = 5) -> list[dict]:
        """CPI historique pour calcul PPP."""
        series_id = FRED_SERIES["cpi_yoy"].get(currency)
        if not series_id:
            return []
        start = (date.today() - timedelta(days=365 * years)).isoformat()
        return await self.get_series(series_id, observation_start=start, limit=years * 13)

    async def get_historical_policy_rates(self, currency: str, years: int = 5) -> list[dict]:
        """Taux directeurs historiques pour calcul carry."""
        series_id = FRED_SERIES["policy_rate"].get(currency)
        if not series_id:
            return []
        start = (date.today() - timedelta(days=365 * years)).isoformat()
        return await self.get_series(series_id, observation_start=start, limit=years * 13)
