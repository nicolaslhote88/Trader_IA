"""Client minimal du service yfinance interne pour les références FX.

Les cotations ne deviennent jamais une valeur de remplacement : une erreur ou
une ligne vide reste absente dans le contrat AG6.
"""

from __future__ import annotations

import os
from typing import Any

import httpx


FX_SYMBOLS = {
    "EUR": ("EURUSD=X", False),
    "GBP": ("GBPUSD=X", False),
    "AUD": ("AUDUSD=X", False),
    "NZD": ("NZDUSD=X", False),
    "JPY": ("JPY=X", True),
    "CHF": ("CHF=X", True),
    "CAD": ("CAD=X", True),
    "MXN": ("MXN=X", True),
    "SEK": ("SEK=X", True),
    "NOK": ("NOK=X", True),
    "KRW": ("KRW=X", True),
}


class MarketClient:
    def __init__(self) -> None:
        self.base_url = os.environ.get("YFINANCE_API_URL", "http://yfinance-api:8080").rstrip("/")
        self.timeout = float(os.environ.get("YFINANCE_API_TIMEOUT_SECONDS", "30"))

    async def get_fx_spots(self) -> dict[str, dict[str, Any]]:
        symbols = [row[0] for row in FX_SYMBOLS.values()]
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            response = await client.get(
                f"{self.base_url}/quote",
                params={"symbols": ",".join(symbols), "max_age_seconds": 300},
            )
            response.raise_for_status()
            payload = response.json()
        rows_by_symbol = {str(row.get("symbol") or "").upper(): row for row in payload.get("quotes", [])}
        output: dict[str, dict[str, Any]] = {
            "USD": {
                "value": 1.0,
                "symbol": "USD",
                "observation_time": payload.get("fetchedAt"),
                "source": "IDENTITY_REFERENCE",
                "status": "calculated_value",
                "confidence": 1.0,
            }
        }
        for currency, (symbol, invert) in FX_SYMBOLS.items():
            row = rows_by_symbol.get(symbol.upper()) or {}
            raw = row.get("mid") if row.get("mid") is not None else row.get("regularMarketPrice")
            try:
                raw_value = float(raw)
            except (TypeError, ValueError):
                continue
            if raw_value <= 0:
                continue
            output[currency] = {
                "value": 1.0 / raw_value if invert else raw_value,
                "raw_value": raw_value,
                "symbol": symbol,
                "inverted": invert,
                "observation_time": row.get("regularMarketTime") or payload.get("fetchedAt"),
                "ingestion_time": payload.get("fetchedAt"),
                "source": row.get("source") or "yahoo_finance_yfinance",
                "status": "calculated_value" if invert else "direct_observation",
                "confidence": 0.85 if row.get("stale") else 0.95,
            }
        return output
