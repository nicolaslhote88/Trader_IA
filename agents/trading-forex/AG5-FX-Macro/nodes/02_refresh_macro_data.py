"""
AG5-FX-Macro — Pilier 1 : Macro/Flows
Déclenche le refresh complet des données macro depuis FRED via macro-data-api.
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
        f"{api_url}/macro/refresh_all",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        refresh_result = json.loads(resp.read())
except Exception as exc:
    error = str(exc)

return [{"json": {**ctx, "refresh_result": refresh_result, "refresh_error": error}}]
