"""
Client pour les données de courbe des taux souverains.
Sources : FRED (primaire), IBKR (backup pour données intraday/temps réel).
Calcule le slope 10Y-2Y et détecte la pentification/aplatissement.
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional

import httpx

logger = logging.getLogger("rates_client")

# Conids IBKR pour les contrats de taux (ETF proxies)
IBKR_BOND_CONIDS = {
    # US Treasuries futures
    "US_2Y_FUTURE": 23408745,   # ZT1 2-Year Treasury Note future
    "US_10Y_FUTURE": 258,        # ZN1 10-Year Treasury Note future
    # Bond ETFs (proxy pour yields via prix)
    "SHY": 23408,   # iShares 1-3 Year Treasury Bond ETF (proxy 2Y)
    "IEF": 23409,   # iShares 7-10 Year Treasury Bond ETF (proxy 10Y)
    "TLT": 23410,   # iShares 20+ Year Treasury Bond ETF (proxy long)
}

# Yields 2Y par pays (proxy : politique monétaire actuelle, banques centrales)
# En l'absence de yields souverains directs IBKR, on utilise les taux directeurs +
# spread vs. policy rate (estimé empiriquement)
POLICY_RATE_TO_2Y_SPREAD = 0.25  # spread moyen historique (bps → %)
POLICY_RATE_TO_10Y_SPREAD = 0.75  # fallback defensif quand la jambe 10Y manque

# G10 pairs pour la stratégie de pentification
G10_COUNTRIES = {
    "USD": {"country": "United States", "cb": "Federal Reserve"},
    "EUR": {"country": "Euro Area",     "cb": "ECB"},
    "JPY": {"country": "Japan",         "cb": "Bank of Japan"},
    "GBP": {"country": "United Kingdom","cb": "Bank of England"},
    "CHF": {"country": "Switzerland",   "cb": "SNB"},
    "CAD": {"country": "Canada",        "cb": "Bank of Canada"},
    "AUD": {"country": "Australia",     "cb": "RBA"},
    "NZD": {"country": "New Zealand",   "cb": "RBNZ"},
    "MXN": {"country": "Mexico",        "cb": "Banxico"},
    "SEK": {"country": "Sweden",        "cb": "Riksbank"},
    "NOK": {"country": "Norway",        "cb": "Norges Bank"},
    "KRW": {"country": "South Korea",   "cb": "Bank of Korea"},
}


class RatesClient:
    def __init__(self, ibkr_broker_url: Optional[str] = None):
        self.ibkr_url = ibkr_broker_url or os.environ.get("IBKR_BROKER_URL", "http://ibkr-broker:8080")
        self.banxico_token = os.environ.get("BANXICO_API_TOKEN", "")

    async def get_ibkr_market_data(self, conid: int, period: str = "1y", bar: str = "1d") -> list[dict]:
        """Récupère les données historiques IBKR pour un contrat."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get(
                    f"{self.ibkr_url}/marketdata/history",
                    params={"conid": conid, "period": period, "bar": bar},
                )
                if r.status_code == 200:
                    return r.json().get("data", [])
        except Exception as exc:
            logger.warning("IBKR market data failed for conid %s: %s", conid, exc)
        return []

    async def get_banxico_yields(self) -> tuple[dict[str, dict], dict[str, dict]]:
        """
        Fetch MXN sovereign yields from Banxico SIE when series IDs are configured.

        Required env vars:
        - BANXICO_API_TOKEN
        - BANXICO_MXN_YIELD_2Y_SERIES_ID
        - BANXICO_MXN_YIELD_10Y_SERIES_ID

        The series IDs stay configurable because Banxico can expose several
        benchmark variants; operations can choose the exact curve convention.
        """
        series = {
            "2Y": os.environ.get("BANXICO_MXN_YIELD_2Y_SERIES_ID", ""),
            "10Y": os.environ.get("BANXICO_MXN_YIELD_10Y_SERIES_ID", ""),
        }
        if not self.banxico_token or not all(series.values()):
            return {}, {}

        async def fetch_one(tenor: str, series_id: str) -> Optional[dict]:
            url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{series_id}/datos/oportuno"
            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.get(url, headers={"Bmx-Token": self.banxico_token})
                    resp.raise_for_status()
                data = resp.json()
                rows = (((data.get("bmx") or {}).get("series") or [{}])[0].get("datos") or [])
                if not rows:
                    return None
                latest = rows[-1]
                value = str(latest.get("dato") or "").replace(",", "")
                return {"yield_pct": float(value), "as_of": latest.get("fecha"), "source": "Banxico"}
            except Exception as exc:
                logger.warning("Banxico MXN %s yield failed: %s", tenor, exc)
                return None

        y2 = await fetch_one("2Y", series["2Y"])
        y10 = await fetch_one("10Y", series["10Y"])
        yields_2y = {"MXN": {"yield_2y_pct": y2["yield_pct"], "as_of": y2["as_of"], "source": y2["source"]}} if y2 else {}
        yields_10y = {"MXN": {"yield_10y_pct": y10["yield_pct"], "as_of": y10["as_of"], "source": y10["source"]}} if y10 else {}
        return yields_10y, yields_2y

    def build_yield_curve(
        self,
        policy_rates: dict[str, dict],
        yields_10y: dict[str, dict],
        yields_2y: dict[str, dict],
    ) -> dict[str, dict]:
        """
        Construit la courbe des taux par pays avec:
        - 2Y yield (si dispo FRED, sinon policy_rate + spread estimé)
        - 10Y yield (FRED)
        - slope = 10Y - 2Y
        - slope_signal : steepening / flattening / flat
        """
        result = {}
        for currency in G10_COUNTRIES:
            y10 = yields_10y.get(currency, {}).get("yield_10y_pct")
            y2 = yields_2y.get(currency, {}).get("yield_2y_pct")
            policy = policy_rates.get(currency, {}).get("rate_pct")
            source = yields_10y.get(currency, {}).get("source") or "FRED"
            observation_as_of = yields_10y.get(currency, {}).get("as_of")

            # Operational override for non-FRED curves such as Banxico 2Y/10Y.
            # Example: MXN_YIELD_10Y_PCT=9.55 MXN_YIELD_2Y_PCT=9.85.
            env_y10 = os.environ.get(f"{currency}_YIELD_10Y_PCT")
            env_y2 = os.environ.get(f"{currency}_YIELD_2Y_PCT")
            try:
                if env_y10 not in (None, ""):
                    y10 = float(env_y10)
                    source = os.environ.get(f"{currency}_YIELD_SOURCE", "manual_override")
                if env_y2 not in (None, ""):
                    y2 = float(env_y2)
            except ValueError:
                logger.warning("Invalid manual yield override for %s", currency)

            if y2 is None and policy is not None:
                # Proxy : taux directeur + spread moyen historique
                y2 = round(policy + POLICY_RATE_TO_2Y_SPREAD, 3)
                if source == "FRED":
                    source = "FRED+policy_2y_proxy"

            if y10 is None and policy is not None:
                # Proxy de dernier recours : preserve le cube avec une confiance
                # degradee au lieu de supprimer totalement la devise.
                y10 = round(policy + POLICY_RATE_TO_10Y_SPREAD, 3)
                source = "policy_curve_proxy" if source == "FRED" else f"{source}+policy_curve_proxy"
                observation_as_of = policy_rates.get(currency, {}).get("as_of")

            if y10 is None or y2 is None:
                continue

            slope = round(y10 - y2, 3)
            result[currency] = {
                "yield_2y": y2,
                "yield_10y": y10,
                "slope_10y2y": slope,
                "as_of": observation_as_of or policy_rates.get(currency, {}).get("as_of") or date.today().isoformat(),
                "source": source,
            }
        return result

    def compute_steepening_signal(
        self,
        current_curves: dict[str, dict],
        historical_curves: list[dict],
        lookback_days: int = 30,
    ) -> dict[str, dict]:
        """
        Détecte la pentification (steepening) vs. l'aplatissement (flattening).
        Retourne un signal par pays.

        Strategy signal :
        - steepener : long 2Y bonds, short 10Y bonds (si CBs vont baisser les taux courts)
        - flattener : short 2Y, long 10Y (si courbe s'aplatit)
        """
        # Calcul du changement de slope sur la période
        if not historical_curves:
            return {ccy: {**d, "slope_change_30d": None, "steepening": None, "rates_signal": "neutral"}
                    for ccy, d in current_curves.items()}

        # Filtrer l'historique pour trouver la valeur d'il y a ~lookback_days
        from datetime import datetime
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        hist_by_ccy: dict[str, list] = {}
        for rec in historical_curves:
            ccy = rec.get("currency")
            if ccy not in hist_by_ccy:
                hist_by_ccy[ccy] = []
            hist_by_ccy[ccy].append(rec)

        result = {}
        for currency, cur in current_curves.items():
            hist = hist_by_ccy.get(currency, [])
            past_slope = None
            for rec in sorted(hist, key=lambda x: x.get("as_of", ""), reverse=False):
                if rec.get("as_of", "") <= cutoff:
                    past_slope = rec.get("slope_10y2y")
            slope_change = None
            steepening = None
            signal = "neutral"
            if past_slope is not None:
                slope_change = round(cur["slope_10y2y"] - past_slope, 3)
                steepening = slope_change > 0.10  # +10bps de pentification sur 30j
                if steepening:
                    signal = "steepener"  # long 2Y, short 10Y
                elif slope_change < -0.10:
                    signal = "flattener"  # short 2Y, long 10Y
            result[currency] = {
                **cur,
                "slope_change_30d": slope_change,
                "steepening": steepening,
                "rates_signal": signal,
            }
        return result
