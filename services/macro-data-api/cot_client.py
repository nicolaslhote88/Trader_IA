"""
Client CFTC Commitment of Traders (COT).
Données de positionnement spéculatif sur les futures FX — publiées chaque mardi.
Utilise le rapport Disaggregated Futures Only (format legacy compatible).
"""

import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import date
from typing import Literal, Optional

import httpx
import pandas as pd

logger = logging.getLogger("cot_client")

# FX futures live in the CFTC "Traders in Financial Futures" report, not in the
# commodity-focused disaggregated report. Annual files include the current year
# to date and have stable headers.
COT_FINANCIAL_FUTURES_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"

PositioningSource = Literal["CFTC_COT", "OPTION_RR_25D", "ETF_FLOWS", "CME_OI"]
PositioningConfidence = Literal["high", "medium", "low"]


@dataclass(frozen=True)
class PositioningRecord:
    currency: str
    net_specs: float
    timestamp: date
    source: PositioningSource
    confidence: PositioningConfidence


SOURCE_CONFIDENCE: dict[str, PositioningConfidence] = {
    "CFTC_COT": "high",
    "OPTION_RR_25D": "medium",
    "CME_OI": "medium",
    "ETF_FLOWS": "low",
}


def confidence_for_source(source: str) -> PositioningConfidence:
    return SOURCE_CONFIDENCE.get(str(source or "").upper(), "low")

# Mapping nom de marché CFTC → devise ISO
CFTC_MARKET_TO_CURRENCY = {
    "EURO FX":               "EUR",
    "JAPANESE YEN":          "JPY",
    "BRITISH POUND":         "GBP",
    "SWISS FRANC":           "CHF",
    "CANADIAN DOLLAR":       "CAD",
    "AUSTRALIAN DOLLAR":     "AUD",
    "NEW ZEALAND DOLLAR":    "NZD",
    "NZ DOLLAR":             "NZD",
    "MEXICAN PESO":          "MXN",
    "SOUTH KOREAN WON":      "KRW",
    "CHINESE RENMINBI":      "CNH",
}

CFTC_MARKET_CODE_TO_CURRENCY = {
    "095741": "MXN",
}

# Colonnes importantes dans les rapports CFTC. Keep legacy names as fallback,
# but prefer the underscored TFF headers from fut_fin_txt_YYYY.zip.
COT_COLUMNS = {
    "Market and Exchange Names":         "market_name",
    "Market_and_Exchange_Names":         "market_name",
    "CFTC Contract Market Code":         "market_code",
    "CFTC_Contract_Market_Code":         "market_code",
    "As of Date in Form YYYY-MM-DD":     "report_date",
    "Report_Date_as_YYYY-MM-DD":         "report_date",
    "Open Interest (All)":               "open_interest",
    "Open_Interest_All":                 "open_interest",
    "Asset Mgr. Positions-Long (All)":   "asset_mgr_long",
    "Asset_Mgr_Positions_Long_All":      "asset_mgr_long",
    "Asset Mgr. Positions-Short (All)":  "asset_mgr_short",
    "Asset_Mgr_Positions_Short_All":     "asset_mgr_short",
    "Lev Money Positions-Long (All)":    "lev_money_long",
    "Lev_Money_Positions_Long_All":      "lev_money_long",
    "Lev Money Positions-Short (All)":   "lev_money_short",
    "Lev_Money_Positions_Short_All":     "lev_money_short",
    "Other Rept. Positions-Long (All)":  "other_long",
    "Other_Rept_Positions_Long_All":     "other_long",
    "Other Rept. Positions-Short (All)": "other_short",
    "Other_Rept_Positions_Short_All":    "other_short",
    "Nonrept. Positions-Long (All)":     "nonrept_long",
    "NonRept_Positions_Long_All":        "nonrept_long",
    "Nonrept. Positions-Short (All)":    "nonrept_short",
    "NonRept_Positions_Short_All":       "nonrept_short",
}


class COTClient:
    def __init__(self):
        pass

    def build_proxy_positioning_record(
        self,
        currency: str,
        net_spec: float,
        report_date: str,
        source: PositioningSource,
        open_interest: int = 0,
    ) -> dict:
        """
        Normalise un proxy de positionnement non-CFTC.

        Les fetchers provider-specific (option risk reversal, CME OI, ETF flows)
        doivent produire ce format avant persistence, afin que le scoring puisse
        appliquer le gating via source/confidence.
        """
        source_value = str(source or "").upper()
        return {
            "report_date": report_date,
            "currency": str(currency or "").upper(),
            "net_spec": float(net_spec),
            "lev_money_long": 0,
            "lev_money_short": 0,
            "asset_mgr_long": 0,
            "asset_mgr_short": 0,
            "open_interest": int(open_interest or 0),
            "source": source_value,
            "confidence": confidence_for_source(source_value),
        }

    async def _download_file(self, url: str) -> Optional[pd.DataFrame]:
        """Télécharge et parse un fichier COT CFTC zip/csv/txt."""
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                r = await client.get(url)
                r.raise_for_status()
            content = io.BytesIO(r.content)
            if zipfile.is_zipfile(content):
                content.seek(0)
                with zipfile.ZipFile(content) as z:
                    data_files = [
                        f for f in z.namelist()
                        if f.lower().endswith((".csv", ".txt"))
                    ]
                    if not data_files:
                        logger.warning("No data file in COT zip: %s", url)
                        return None
                    with z.open(data_files[0]) as f:
                        df = pd.read_csv(f, low_memory=False)
            else:
                content.seek(0)
                df = pd.read_csv(content, low_memory=False)
            return df
        except Exception as exc:
            logger.error("COT download failed %s: %s", url, exc)
            return None

    @staticmethod
    def _normalize_market_name(value: object) -> str:
        text = str(value or "").strip().upper()
        # CFTC rows look like "EURO FX - CHICAGO MERCANTILE EXCHANGE".
        return text.split(" - ", 1)[0].strip()

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
            mkt = self._normalize_market_name(row.get("market_name", ""))
            market_code = str(row.get("market_code") or "").strip()
            currency = CFTC_MARKET_TO_CURRENCY.get(mkt) or CFTC_MARKET_CODE_TO_CURRENCY.get(market_code)
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
                "source": "CFTC_COT",
                "confidence": confidence_for_source("CFTC_COT"),
            })
        return results

    async def get_current_cot(self) -> list[dict]:
        """Récupère le dernier rapport COT (semaine courante)."""
        year = date.today().year
        df = await self._download_file(COT_FINANCIAL_FUTURES_URL.format(year=year))
        if df is None:
            return []
        return self._parse_df(df)

    async def get_historical_cot(self, years_back: int = 2) -> list[dict]:
        """Récupère l'historique COT pour calculer les z-scores."""
        all_records = []
        current_year = date.today().year
        years = range(current_year, current_year - max(0, years_back) - 1, -1)
        for year in years:
            url = COT_FINANCIAL_FUTURES_URL.format(year=year)
            df = await self._download_file(url)
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
                    "source": row.get("source", "CFTC_COT"),
                    "confidence": row.get("confidence", confidence_for_source(row.get("source", "CFTC_COT"))),
                })
        return result
