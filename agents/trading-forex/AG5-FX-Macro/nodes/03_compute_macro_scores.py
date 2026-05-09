"""
AG5-FX-Macro — Pilier 1 : Calcul des scores macro par devise.
Appelle /pillars/compute sur macro-data-api pour calculer et sauvegarder les scores.
"""
import os
import urllib.request
import json

ctx = (_items or [{"json": {}}])[0].get("json", {})
api_url = ctx.get("macro_api_url", os.environ.get("MACRO_DATA_API_URL", "http://macro-data-api:8081"))

error = None
scores_result = {}

try:
    req = urllib.request.Request(
        f"{api_url}/pillars/compute",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=b"{}",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        scores_result = json.loads(resp.read())
except Exception as exc:
    error = str(exc)

scores = scores_result.get("scores", [])
aligned_count = scores_result.get("aligned_count", 0)

return [{"json": {**ctx, "pillar_scores": scores, "aligned_count": aligned_count, "scores_error": error}}]
