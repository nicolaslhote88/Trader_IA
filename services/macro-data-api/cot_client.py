"""
Client CFTC Commitment of Traders (COT).
Données de positionnement spéculatif sur les futures FX — publiées chaque mardi.
Utilise le rapport Disaggregated Futures Only (format legacy compatible).
"""

import io
import logging
import zipfile
from datetime import date, timedelta
from typing import Optional

import httpx
import pandas as pd

logger = logging.getLogger("cot_client")

# URL des données COT CFTC (Disaggregated, Futures Only, année courante + historique)
COT_CURRENT_YEAR_URL = "https://www.cftc.gov/files/dea/newcot/f_disagg.zip"
COT_HISTORICAL_URLS = [
    "https://www.cftc.gov/files/dea/newcot/disaggregated_futonly_2024_2025.zip",
    "https://www.cftc.gov/files/dea/newcot/disaggregated_futonly_2022_2023.zip",
    "https://www.cftc.gov/files/dea/newcot/disaggregated_futonly_2020_2021.zip",
]

# Mapping nom de marché CFTC → devise ISO
CFTC_MARKET_TO_CURRENCY = {
    "EURO FX":               "EUR",
    "JAPANESE YEN":          "JPY",
    "BRITISH POUND":         "GBP",
    "SWISS FRANC":           "CHF",
    "CANADIAN DOLLAR":       "CAD",
    "AUSTRALIAN DOLLAR":     "AUD",
    "NEW ZEALAND DOLLAR":    "NZD",
    "MEXICAN PESO":          "MXN",
    "SOUTH KOREAN WON":      "KRW",
    "CHINESE RENMINBI":      "CNH",
}

# Colonnes importantes dans le rapport
COT_COLUMNS = {
    "Market and Exchange Names":         "market_name",
    "As of Date in Form YYYY-MM-DD":     "report_date",
    "Open Interest (All)":               "open_interest",
    "Asset Mgr. Positions-Long (All)":   "asset_mgr_long",
    "Asset Mgr. Positions-Short (All)":  "asset_mgr_short",
    "Lev Money Positions-Long (All)":    "lev_money_long",
    "Lev Money Positions-Short (All)":   "lev_money_short",
    "Other Rept. Positions-Long (All)":  "other_long",
    "Other Rept. Positions-Short (All)": "other_short",
    "Nonrept. Positions-Long (All)":     "nonrept_long",
    "Nonrept. Positions-Short (All)":    "nonrept_short",
}


class COTClient:
    def __init__(self):
        pass

    async def _download_zip(self, url: str) -> Optional[pd.DataFrame]:
        """Télécharge et parse un zip COT CFTC."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(url)
                r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                if not csv_files:
                    logger.warning("No CSV in COT zip: %s", url)
                    return None
                with z.open(csv_files[0]) as f:
                    df = pd.read_csv(f, low_memory=False)
            return df
        except Exception as exc:
            logger.error("COT download failed %s: %s", url, exc)
            return None

    def _parse_df(self, df: pd.DataFrame) -> list[dict]:
        """Parse le dataframe COT en liste de dicts par devise."""
        results = []
        rename = {k: v for k, v in COT_COLUMNS.items() if k in df.columns}
        df = df.rename(columns=rename)
        required = ["market_name", "report_date", "open_interest"]
        if not all(c in df.columns for c in required):
            return results
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
        df = df.dropna(subset=["report_date"])

        for _, row in df.iterrows():
            mkt = str(row.get("market_name", "")).strip().upper()
            currency = CFTC_MARKET_TO_CURRENCY.get(mkt)
            if not currency:
                continue

            def safe_int(v):
                try:
                    return int(float(v)) if pd.notna(v) else 0
                except (ValueError, TypeError):
                    return 0

            # On utilise "Leveraged Money" comme proxy des spéculatifs purs
            lev_long = safe_int(row.get("lev_money_long", 0))
            lev_short = safe_int(row.get("lev_money_short", 0))
            # Asset managers comme signal de positionnement institutionnel
            am_long = safe_int(row.get("asset_mgr_long", 0))
            am_short = safe_int(row.get("asset_mgr_short", 0))
            # Combiné = total non-commercial "spéculatif"
            total_spec_long = lev_long + am_long
            total_spec_short = lev_short + am_short
            net_spec = total_spec_long - total_spec_short

            results.append({
                "report_date": row["report_date"].date().isoformat(),
                "currency": currency,
                "net_spec": net_spec,
                "lev_money_long": lev_long,
                "lev_money_short": lev_short,
                "asset_mgr_long": am_long,
                "asset_mgr_short": am_short,
                "open_interest": safe_int(row.get("open_interest", 0)),
            })
        return results

    async def get_current_cot(self) -> list[dict]:
        """Récupère le dernier rapport COT (semaine courante)."""
        df = await self._download_zip(COT_CURRENT_YEAR_URL)
        if df is None:
            return []
        return self._parse_df(df)

    async def get_historical_cot(self, years_back: int = 2) -> list[dict]:
        """Récupère l'historique COT pour calculer les z-scores."""
        all_records = []
        urls = [COT_CURRENT_YEAR_URL] + COT_HISTORICAL_URLS[:years_back]
        for url in urls:
            df = await self._download_zip(url)
            if df is not None:
                all_records.extend(self._parse_df(df))
        # Déduplique par (report_date, currency)
        seen = set()
        unique = []
        for rec in all_records:
            key = (rec["report_date"], rec["currency"])
            if key not in seen:
                seen.add(key)
                unique.append(rec)
        return sorted(unique, key=lambda x: x["report_date"], reverse=True)

    def compute_z_scores(self, records: list[dict], lookback_weeks: int = 52) -> list[dict]:
        """
        Calcule le z-score de positionnement net vs. les 52 dernières semaines.
        z > +1.5 : crowded long (dangereux)
        z < -1.5 : crowded short (opportunité contrarian)
        positioning_score = clamp(-z / 2, -1, +1)  — inversé : hated = positif
        """
        import statistics

        df = pd.DataFrame(records)
        if df.empty:
            return records

        result = []
        for currency in df["currency"].unique():
            cdf = df[df["currency"] == currency].sort_values("report_date", ascending=False)
            net_series = cdf["net_spec"].tolist()
            for i, row in enumerate(cdf.to_dict("records")):
                window = net_series[i: i + lookback_weeks]
                if len(window) >= 4:
                    mu = statistics.mean(window)
                    sigma = statistics.stdev(window) if len(window) > 1 else 1.0
                    sigma = max(sigma, 1.0)
                    z = (row["net_spec"] - mu) / sigma
                else:
                    z = 0.0
                crowded = abs(z) > 1.5
                # Inversé : position "haïe" (z très négatif) = opportunité = score positif
                positioning_score = max(-1.0, min(1.0, -z / 2.0))
                result.append({
                    **row,
                    "net_z_score": round(z, 3),
                    "crowded_flag": crowded,
                    "crowded_direction": ("long" if z > 1.5 else "short" if z < -1.5 else "neutral"),
                    "positioning_score": round(positioning_score, 3),
                })
        return result
