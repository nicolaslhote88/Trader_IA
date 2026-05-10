"""
AG8-FX-Rates — Calcul des signaux de courbe des taux.

Stratégie de pentification (steepener) :
- Signal : CBs vont baisser les taux courts (récession) + dette long terme maintient les longs élevés
- Trade : Long obligations 2Y (ZT/ZF), Short obligations 10Y (ZN)
- Conviction renforcée si : courbe inversée (slope < 0) + momentum de pentification

Stratégie d'aplatissement (flattener) :
- CBs hawkish sur toute la courbe
- Trade : Short 2Y, Long 10Y
"""
import os
import duckdb
from datetime import date

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
yield_curves = ctx.get("yield_curves", [])

today = date.today().isoformat()
rates_signals = []

for curve in yield_curves:
    ccy = curve.get("currency", "")
    slope = curve.get("slope_10y2y")
    slope_change = curve.get("slope_change_30d")
    yield_2y = curve.get("yield_2y_pct")
    yield_10y = curve.get("yield_10y_pct")

    if slope is None:
        continue

    # Détecter le régime
    inverted = slope < 0  # courbe inversée = récession probable
    steepening = slope_change is not None and slope_change > 0.10
    flattening = slope_change is not None and slope_change < -0.10

    # Signal principal
    if steepening or (inverted and slope_change is not None and slope_change > 0):
        signal = "steepener"
        action = "long_2y_short_10y"
        conviction = min(1.0, abs(slope_change or 0) / 0.5)
    elif flattening:
        signal = "flattener"
        action = "short_2y_long_10y"
        conviction = min(1.0, abs(slope_change or 0) / 0.5)
    elif inverted:
        signal = "watch_steepener"  # pas encore de momentum mais courbe inversée
        action = "monitor"
        conviction = 0.3
    else:
        signal = "neutral"
        action = "no_trade"
        conviction = 0.0

    # Contexte spécifique USD : thèse "fin de l'exceptionnalisme américain"
    notes = ""
    if ccy == "USD":
        if slope < -0.5:
            notes = "Courbe fortement inversée US — récession imminente, steepener fort anticipé"
        elif slope < 0:
            notes = "Courbe US légèrement inversée — surveiller pentification"

    rates_signals.append({
        "currency": ccy,
        "yield_2y_pct": yield_2y,
        "yield_10y_pct": yield_10y,
        "slope_10y2y": slope,
        "slope_change_30d": slope_change,
        "inverted": inverted,
        "steepening": steepening,
        "flattening": flattening,
        "rates_signal": signal,
        "trade_action": action,
        "conviction": round(conviction, 2),
        "notes": notes,
    })

# Mettre à jour les signaux dans la DB
try:
    if os.path.exists(db_path):
        with duckdb.connect(db_path) as con:
            for s in rates_signals:
                con.execute(
                    """INSERT INTO rates.yield_curve
                       (as_of, currency, yield_2y_pct, yield_10y_pct, slope_10y2y,
                        slope_change_30d, steepening, rates_signal)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (as_of, currency) DO UPDATE SET
                         yield_2y_pct = EXCLUDED.yield_2y_pct,
                         yield_10y_pct = EXCLUDED.yield_10y_pct,
                         slope_10y2y = EXCLUDED.slope_10y2y,
                         slope_change_30d = EXCLUDED.slope_change_30d,
                         steepening = EXCLUDED.steepening,
                         rates_signal = EXCLUDED.rates_signal,
                         updated_at = CURRENT_TIMESTAMP""",
                    [today, s["currency"], s["yield_2y_pct"], s["yield_10y_pct"],
                     s["slope_10y2y"], s["slope_change_30d"], s["steepening"], s["rates_signal"]],
                )
except Exception as exc:
    ctx["db_error"] = str(exc)

# Résumé pour le brief AG1-FX-V2
steepener_ops = [s for s in rates_signals if s["rates_signal"] in ("steepener", "watch_steepener")]
us_curve = next((s for s in rates_signals if s["currency"] == "USD"), {})

return [{"json": {
    **ctx,
    "rates_signals": rates_signals,
    "steepener_opportunities": steepener_ops,
    "us_yield_curve": us_curve,
    "us_slope": us_curve.get("slope_10y2y"),
    "us_steepening": us_curve.get("steepening", False),
}}]
