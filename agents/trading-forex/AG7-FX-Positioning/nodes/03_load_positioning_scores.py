"""
AG7-FX-Positioning — Pilier 3 : Charge les scores de positionnement depuis COT.
Récupère depuis macro-data-api les derniers scores COT et les formate pour le brief.
"""
import os
import urllib.request
import json

ctx = (_items or [{"json": {}}])[0].get("json", {})
api_url = ctx.get("macro_api_url", os.environ.get("MACRO_DATA_API_URL", "http://macro-data-api:8081"))

error = None
cot_latest = []

try:
    with urllib.request.urlopen(f"{api_url}/macro/cot", timeout=15) as resp:
        cot_latest = json.loads(resp.read())
except Exception as exc:
    error = str(exc)

# Formatter pour le brief
G10 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
positioning_by_ccy = {}
for rec in cot_latest:
    ccy = rec.get("currency", "")
    if ccy in G10:
        positioning_by_ccy[ccy] = {
            "currency": ccy,
            "net_spec": rec.get("net_spec"),
            "cot_z_score": rec.get("net_z_score"),
            "crowded_flag": rec.get("crowded_flag", False),
            "crowded_direction": rec.get("crowded_direction", "neutral"),
            "positioning_score": rec.get("positioning_score"),
            "report_date": rec.get("report_date"),
        }

# Identifier les devises "détestées" (crowded short) = opportunités contrarian
hated_currencies = [
    ccy for ccy, d in positioning_by_ccy.items()
    if d.get("crowded_direction") == "short" and d.get("crowded_flag")
]
# Identifier les devises crowded long (à éviter)
crowded_longs = [
    ccy for ccy, d in positioning_by_ccy.items()
    if d.get("crowded_direction") == "long" and d.get("crowded_flag")
]

return [{"json": {
    **ctx,
    "positioning_scores": list(positioning_by_ccy.values()),
    "hated_currencies": hated_currencies,
    "crowded_long_currencies": crowded_longs,
    "cot_error": error,
}}]
