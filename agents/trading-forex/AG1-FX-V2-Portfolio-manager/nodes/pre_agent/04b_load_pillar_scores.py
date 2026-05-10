"""
AG1-FX-V2 — Charge les scores des 3 piliers depuis macro_data.duckdb.
Construit le contexte three_pillars pour le brief LLM.
"""
import os
import duckdb
from datetime import date, timedelta

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
threshold = ctx.get("three_pillars_threshold", 0.20)

G10 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
pillar_scores_by_ccy = {}
yield_curves = {}
cot_by_ccy = {}
rates_signals = {}

if os.path.exists(db_path):
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            # Scores piliers
            rows = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM pillars.currency_scores
                   WHERE as_of >= ?
                   ORDER BY currency, as_of DESC""",
                [(date.today() - timedelta(days=7)).isoformat()],
            ).fetchdf()
            for _, r in rows.iterrows():
                ccy = r["currency"]
                pillar_scores_by_ccy[ccy] = {
                    "macro_score": r.get("macro_score"),
                    "valuation_score": r.get("valuation_score"),
                    "positioning_score": r.get("positioning_score"),
                    "composite_score": r.get("composite_score"),
                    "crowded_flag": bool(r.get("crowded_flag", False)),
                    "all_pillars_aligned": bool(r.get("all_pillars_aligned", False)),
                    "as_of": str(r.get("as_of", "")),
                }

            # Courbe des taux
            rate_rows = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM rates.yield_curve
                   WHERE as_of >= ?
                   ORDER BY currency, as_of DESC""",
                [(date.today() - timedelta(days=7)).isoformat()],
            ).fetchdf()
            for _, r in rate_rows.iterrows():
                ccy = r["currency"]
                yield_curves[ccy] = {
                    "yield_2y_pct": r.get("yield_2y_pct"),
                    "yield_10y_pct": r.get("yield_10y_pct"),
                    "slope_10y2y": r.get("slope_10y2y"),
                    "slope_change_30d": r.get("slope_change_30d"),
                    "steepening": bool(r.get("steepening", False)),
                    "rates_signal": r.get("rates_signal", "neutral"),
                }

            # COT latest
            cot_rows = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM cot.speculative_positions
                   ORDER BY currency, report_date DESC"""
            ).fetchdf()
            for _, r in cot_rows.iterrows():
                ccy = r["currency"]
                cot_by_ccy[ccy] = {
                    "net_spec": int(r.get("net_spec", 0)),
                    "net_z_score": float(r.get("net_z_score", 0)) if r.get("net_z_score") is not None else None,
                    "crowded_flag": bool(r.get("crowded_flag", False)),
                    "crowded_direction": r.get("crowded_direction", "neutral"),
                    "positioning_score": float(r.get("positioning_score", 0)) if r.get("positioning_score") is not None else None,
                    "report_date": str(r.get("report_date", "")),
                }
    except Exception as exc:
        ctx["pillar_load_error"] = str(exc)

# Construire le contexte three_pillars pour le brief
three_pillars = {
    "as_of": date.today().isoformat(),
    "threshold": threshold,
    "by_currency": {},
    "opportunities": [],
    "crowded_alerts": [],
    "data_available": bool(pillar_scores_by_ccy),
}

for ccy in G10:
    p = pillar_scores_by_ccy.get(ccy, {})
    y = yield_curves.get(ccy, {})
    c = cot_by_ccy.get(ccy, {})

    macro_s = p.get("macro_score")
    val_s = p.get("valuation_score")
    pos_s = p.get("positioning_score") or c.get("positioning_score")
    composite = p.get("composite_score")
    aligned = p.get("all_pillars_aligned", False)

    three_pillars["by_currency"][ccy] = {
        "macro_score": macro_s,
        "valuation_score": val_s,
        "positioning_score": pos_s,
        "composite_score": composite,
        "all_pillars_aligned": aligned,
        "crowded_flag": c.get("crowded_flag", False),
        "crowded_direction": c.get("crowded_direction", "neutral"),
        "cot_z_score": c.get("net_z_score"),
        "yield_slope": y.get("slope_10y2y"),
        "rates_signal": y.get("rates_signal", "neutral"),
    }
    if aligned and composite is not None:
        direction = "bullish" if composite > 0 else "bearish"
        three_pillars["opportunities"].append({
            "currency": ccy,
            "direction": direction,
            "composite_score": composite,
        })
    if c.get("crowded_flag"):
        three_pillars["crowded_alerts"].append({
            "currency": ccy,
            "direction": c.get("crowded_direction"),
            "z_score": c.get("net_z_score"),
            "warning": f"{ccy} crowded {c.get('crowded_direction', '')} — éviter ou anticiper retournement",
        })

# Trier les opportunités par force de signal
three_pillars["opportunities"].sort(key=lambda x: abs(x.get("composite_score") or 0), reverse=True)

return [{"json": {**ctx, "three_pillars": three_pillars, "yield_curves": yield_curves}}]
