"""
Client FRED API (Federal Reserve Economic Data).
Toutes les séries macroéconomiques clés pour le framework 3 piliers.
"""

import os
import logging
from datetime import date, datetime, timedelta
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
        "MXN": "IRSTCI01MXM156N", # Mexico overnight/interbank rate
        "SEK": "IRSTCI01SEM156N", # Sweden overnight/interbank rate
        "NOK": "IRSTCI01NOM156N", # Norway overnight/interbank rate
        "KRW": "INTDSRKRM193N",    # Bank of Korea
    },
    # PIB réel (croissance QoQ annualisée)
    "gdp_growth": {
        "USD": "A191RL1Q225SBEA",   # US Real GDP QoQ SAAR
        "EUR": "CLVMNACSCAB1GQEA19", # Euro Area Real GDP
        "JPY": "JPNRGDPEXP",         # Japan Real GDP
        "GBP": "UKNGDP",             # UK Real GDP
        "CAD": "NAEXKP01CAQ189S",    # Canada Real GDP
        "AUD": "NGDPRNSAXDCAUQ",      # Australia Real GDP
        "CHF": "NAEXKP01CHQ657S",     # Switzerland GDP QoQ growth rate
        "NZD": "NAEXKP01NZQ657S",     # New Zealand GDP QoQ growth rate
        "MXN": "NAEXKP01MXQ657S",     # Mexico GDP QoQ growth rate
        "SEK": "NAEXKP01SEQ657S",     # Sweden GDP QoQ growth rate
        "NOK": "NAEXKP01NOQ657S",     # Norway GDP QoQ growth rate
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
        "MXN": "MEXCPIALLMINMEI",     # Mexico CPI all items
        "SEK": "SWECPIALLMINMEI",     # Sweden CPI all items
        "NOK": "NORCPIALLMINMEI",     # Norway CPI all items
        "KRW": "KORCPIALLMINMEI",     # Korea CPI all items
    },
    # Chômage (macro contextuel, pas encore pondéré dans le score pilier 1)
    "unemployment": {
        "MXN": "LRHUTTTTMXM156S",
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
    "yield_10y_nzd": {"NZD": "IRLTLT01NZM156N"},
    "yield_10y_chf": {"CHF": "IRLTLT01CHM156N"},
    "yield_10y_mxn": {"MXN": "IRLTLT01MXM156N"},
    "yield_10y_sek": {"SEK": "IRLTLT01SEM156N"},
    "yield_10y_nok": {"NOK": "IRLTLT01NOM156N"},
    "yield_10y_krw": {"KRW": "IRLTLT01KRM156N"},
    "yield_2y_eur": {"EUR": "IRLTST01EZM156N"},
}

# Official static fallback used only when FRED exposes no usable observations.
# Keep these dated and auditable; environment variables can override them in
# production if the central-bank source changes before the next code release.
OFFICIAL_POLICY_RATE_FALLBACKS = {
    "SEK": {
        "rate_pct": 1.75,
        "as_of": "2026-05-13",
        "series_id": "RIKSBANK_POLICY_RATE",
        "source": "Riksbank_official_static",
    },
}


def _year_ago_index(observations: list[dict]) -> Optional[int]:
    """
    Return the observation index that is roughly one year before latest.

    FRED returns mixed frequencies here: most CPI series are monthly, while
    AUD/NZD are quarterly. A fixed 12-row lookback makes quarterly CPI compare
    against three years ago and materially overstates inflation.
    """
    if len(observations) < 2:
        return None
    try:
        latest = datetime.fromisoformat(str(observations[0]["date"]))
        previous = datetime.fromisoformat(str(observations[1]["date"]))
    except (KeyError, TypeError, ValueError):
        return 12 if len(observations) > 12 else None
    months_gap = max(1, abs((latest.year - previous.year) * 12 + latest.month - previous.month))
    idx = max(1, round(12 / months_gap))
    return idx if len(observations) > idx else None


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
                    "source": "FRED",
                }
            env_value = os.environ.get(f"{currency}_POLICY_RATE_PCT")
            if env_value not in (None, ""):
                try:
                    results[currency] = {
                        "rate_pct": float(env_value),
                        "as_of": os.environ.get(f"{currency}_POLICY_RATE_AS_OF", date.today().isoformat()),
                        "series_id": f"{currency}_POLICY_RATE_PCT",
                        "source": os.environ.get(f"{currency}_POLICY_RATE_SOURCE", "manual_override"),
                    }
                except ValueError:
                    logger.warning("Invalid %s_POLICY_RATE_PCT override", currency)
        for currency, fallback in OFFICIAL_POLICY_RATE_FALLBACKS.items():
            if currency not in results:
                results[currency] = dict(fallback)
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
                    "source": "FRED",
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
            year_idx = _year_ago_index(obs)
            if year_idx is not None:
                latest = obs[0]["value"]
                year_ago = obs[year_idx]["value"]
                yoy = round((latest / year_ago - 1) * 100, 2) if year_ago else None
                results[currency] = {
                    "cpi_index": latest,
                    "yoy_pct": yoy,
                    "as_of": obs[0]["date"],
                    "series_id": series_id,
                    "source": "FRED",
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
                    "source": "FRED",
                }
        return results

    async def get_unemployment(self) -> dict[str, dict]:
        """Taux de chômage pour les devises hors G8 suivies en contexte macro."""
        results = {}
        for currency, series_id in FRED_SERIES.get("unemployment", {}).items():
            try:
                obs = await self.get_series(series_id, limit=5)
            except RuntimeError as exc:
                logger.warning("%s", exc)
                continue
            if obs:
                results[currency] = {
                    "unemployment_pct": obs[0]["value"],
                    "as_of": obs[0]["date"],
                    "series_id": series_id,
                    "source": "FRED",
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
            "NZD": ("yield_10y_nzd", "NZD"),
            "CHF": ("yield_10y_chf", "CHF"),
            "MXN": ("yield_10y_mxn", "MXN"),
            "SEK": ("yield_10y_sek", "SEK"),
            "NOK": ("yield_10y_nok", "NOK"),
            "KRW": ("yield_10y_krw", "KRW"),
        }
        results = {}
        for currency, (key, ccy) in mapping.items():
            series_id = FRED_SERIES[key].get(ccy)
            if series_id:
                latest = await self.get_latest(series_id)
                if latest:
                    results[currency] = {"yield_10y_pct": latest["value"], "as_of": latest["date"], "source": "FRED"}
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
                    results[currency] = {"yield_2y_pct": latest["value"], "as_of": latest["date"], "source": "FRED"}
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
