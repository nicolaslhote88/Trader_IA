"""
AG6-FX-Valuation — Pilier 2 : Fetch des données de valorisation.
Récupère les taux directeurs et CPI historiques depuis macro-data-api.
"""
import os
import urllib.request
import json

ctx = (_items or [{"json": {}}])[0].get("json", {})
api_url = ctx.get("macro_api_url", os.environ.get("MACRO_DATA_API_URL", "http://macro-data-api:8081"))

error = None
policy_rates = []
cpi_history = []

try:
    # Taux directeurs
    with urllib.request.urlopen(f"{api_url}/macro/policy_rates", timeout=15) as resp:
        policy_rates = json.loads(resp.read())
except Exception as exc:
    error = f"policy_rates: {exc}"

try:
    # Indicateurs macro (inclut CPI)
    with urllib.request.urlopen(f"{api_url}/pillars/scores", timeout=15) as resp:
        existing_scores = json.loads(resp.read())
except Exception as exc:
    existing_scores = []

return [{"json": {
    **ctx,
    "policy_rates": policy_rates,
    "existing_pillar_scores": existing_scores,
    "fetch_error": error,
}}]
