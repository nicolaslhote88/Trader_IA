"""
AG8-FX-Rates — Fetch et analyse de la courbe des taux souverains G10.
Source primaire : FRED API via macro-data-api.
Calcule les slopes 10Y-2Y et détecte les signaux de pentification.
"""
import os
import urllib.request
import json
import duckdb
from datetime import date, timedelta

ctx = (_items or [{"json": {}}])[0].get("json", {})
api_url = ctx.get("macro_api_url", os.environ.get("MACRO_DATA_API_URL", "http://macro-data-api:8081"))
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))

error = None
yield_curves = []

try:
    with urllib.request.urlopen(f"{api_url}/macro/rates", timeout=15) as resp:
        yield_curves = json.loads(resp.read())
except Exception as exc:
    error = str(exc)

# Enrichir avec l'historique pour calculer le changement de slope
historical_by_ccy = {}
if os.path.exists(db_path):
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            rows = con.execute(
                """SELECT currency, as_of, slope_10y2y
                   FROM rates.yield_curve
                   WHERE as_of >= ?
                   ORDER BY currency, as_of DESC""",
                [(date.today() - timedelta(days=45)).isoformat()],
            ).fetchall()
            for currency, as_of, slope in rows:
                if currency not in historical_by_ccy:
                    historical_by_ccy[currency] = []
                historical_by_ccy[currency].append({"as_of": as_of, "slope_10y2y": slope})
    except Exception as exc:
        pass

# Enrichir chaque courbe avec le changement de slope sur 30j
enriched_curves = []
for curve in yield_curves:
    ccy = curve.get("currency", "")
    current_slope = curve.get("slope_10y2y")
    hist = historical_by_ccy.get(ccy, [])
    past_slope = None
    if hist and len(hist) > 1:
        # Prendre la valeur d'il y a ~30j
        cutoff = (date.today() - timedelta(days=28)).isoformat()
        for h in sorted(hist, key=lambda x: x["as_of"]):
            if h["as_of"] <= cutoff:
                past_slope = h["slope_10y2y"]
    slope_change = None
    if current_slope is not None and past_slope is not None:
        slope_change = round(current_slope - past_slope, 3)
    enriched_curves.append({**curve, "slope_change_30d": slope_change})

# Identifier les opportunités de pentification (steepener)
steepening_ops = []
for c in enriched_curves:
    if c.get("rates_signal") == "steepener":
        steepening_ops.append({
            "currency": c.get("currency"),
            "slope": c.get("slope_10y2y"),
            "slope_change_30d": c.get("slope_change_30d"),
            "signal": "steepener: long 2Y bonds, short 10Y bonds",
        })

return [{"json": {
    **ctx,
    "yield_curves": enriched_curves,
    "steepening_opportunities": steepening_ops,
    "rates_error": error,
}}]
