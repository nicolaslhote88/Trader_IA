"""
AG7-FX-Positioning — Pilier 3 : Refresh des données COT CFTC.
Télécharge les données hebdomadaires de positionnement spéculatif.
"""
import os
import urllib.request
import json

ctx = (_items or [{"json": {}}])[0].get("json", {})
api_url = ctx.get("macro_api_url", os.environ.get("MACRO_DATA_API_URL", "http://macro-data-api:8081"))

error = None
refresh_result = {}

try:
    req = urllib.request.Request(
        f"{api_url}/macro/cot/refresh",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        refresh_result = json.loads(resp.read())
    if int(refresh_result.get("records_total") or 0) <= 0 or int(refresh_result.get("currencies_updated") or 0) <= 0:
        error = f"COT_REFRESH_EMPTY:{refresh_result}"
except Exception as exc:
    error = str(exc)

return [{"json": {**ctx, "cot_refresh": refresh_result, "cot_error": error}}]
