"""
AG6-FX-Valuation — Pilier 2 : Calcul des scores de valorisation.
Carry (différentiel de taux) + PPP (déviation de parité de pouvoir d'achat).
Sauvegarde dans macro_data.duckdb.pillars.currency_scores.
"""
import os
import math
import duckdb
from datetime import date

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
policy_rates = ctx.get("policy_rates", [])

G10 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
today = date.today().isoformat()

def clamp(v, lo=-1.0, hi=1.0):
    return max(lo, min(hi, v))

# Carry score : normalisation des taux directeurs vs. moyenne G10
rates_dict = {r["currency"]: float(r.get("rate_pct") or 0) for r in policy_rates if r.get("currency") in G10}
avg = sum(rates_dict.values()) / len(rates_dict) if rates_dict else 0
std = max(math.sqrt(sum((v - avg) ** 2 for v in rates_dict.values()) / len(rates_dict)) if rates_dict else 0, 0.5)
carry_scores = {ccy: clamp((r - avg) / std / 2) for ccy, r in rates_dict.items()}

# Récupérer PPP depuis la DB (déjà calculé dans scoring.py via AG5)
ppp_scores = {}
try:
    if os.path.exists(db_path):
        with duckdb.connect(db_path, read_only=True) as con:
            rows = con.execute(
                """SELECT DISTINCT ON (currency) currency, ppp_deviation
                   FROM pillars.currency_scores
                   WHERE ppp_deviation IS NOT NULL
                   ORDER BY currency, as_of DESC"""
            ).fetchall()
            ppp_scores = {r[0]: float(r[1]) for r in rows}
except Exception:
    pass

# Calculer scores de valorisation
valuation_scores = []
for ccy in G10:
    carry = carry_scores.get(ccy, 0.0)
    ppp = ppp_scores.get(ccy, 0.0)
    valuation = clamp(carry * 0.60 + ppp * 0.40)
    valuation_scores.append({
        "currency": ccy,
        "carry_score": round(carry, 3),
        "ppp_deviation": round(ppp, 3),
        "valuation_score": round(valuation, 3),
    })

# Mettre à jour les scores de valorisation dans la DB
try:
    if os.path.exists(db_path):
        with duckdb.connect(db_path) as con:
            for s in valuation_scores:
                con.execute(
                    """INSERT INTO pillars.currency_scores
                       (as_of, currency, carry_score, ppp_deviation, valuation_score)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (as_of, currency) DO UPDATE SET
                         carry_score = EXCLUDED.carry_score,
                         ppp_deviation = EXCLUDED.ppp_deviation,
                         valuation_score = EXCLUDED.valuation_score,
                         updated_at = CURRENT_TIMESTAMP""",
                    [today, s["currency"], s["carry_score"], s["ppp_deviation"], s["valuation_score"]],
                )
    written = len(valuation_scores)
except Exception as exc:
    written = 0
    ctx["write_error"] = str(exc)

return [{"json": {**ctx, "valuation_scores": valuation_scores, "records_written": written}}]
