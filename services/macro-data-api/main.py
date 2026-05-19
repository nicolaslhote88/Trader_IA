"""
Macro Data API — Service central pour le framework 3 piliers.
Expose les données macroéconomiques, COT et courbe des taux
nécessaires aux agents AG5/AG6/AG7/AG8.
"""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from cot_client import COTClient
from fred_client import FREDClient
from macro_db import MacroDB
from rates_client import RatesClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("macro-data-api")
logging.getLogger("httpx").setLevel(logging.WARNING)

fred = FREDClient()
cot = COTClient()
db = MacroDB()
rates = RatesClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Macro Data API started. DB: %s", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
    yield


app = FastAPI(title="Macro Data API", version="1.0.0", lifespan=lifespan)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "macro-data-api", "as_of": date.today().isoformat()}


# ── Policy Rates ──────────────────────────────────────────────────────────────

@app.get("/macro/policy_rates")
async def get_policy_rates(refresh: bool = Query(False)):
    """Taux directeurs de toutes les banques centrales G10."""
    if refresh:
        data = await fred.get_policy_rates()
        db.upsert_policy_rates(data)
    return db.get_latest_policy_rates()


# ── Country Indicators ────────────────────────────────────────────────────────

@app.get("/macro/country/{currency}")
async def get_country_macro(currency: str, refresh: bool = Query(False)):
    """Indicateurs macro pour une devise (PIB, CPI, balance courante)."""
    currency = currency.upper()
    if refresh:
        gdp = await fred.get_gdp_growth()
        cpi = await fred.get_cpi()
        ca = await fred.get_current_account()
        db.upsert_gdp_data(gdp)
        db.upsert_cpi_data(cpi)
        db.upsert_current_account_data(ca)
    rows = db.get_indicators(currency=currency)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No macro data for {currency}")
    return {"currency": currency, "indicators": rows}


@app.post("/macro/refresh_all")
async def refresh_all_macro():
    """Rafraîchit tous les indicateurs macro depuis FRED. À appeler 1x/jour."""
    run_id = str(uuid.uuid4())[:8]
    try:
        policy_rates = await fred.get_policy_rates()
        gdp = await fred.get_gdp_growth()
        cpi_data = await fred.get_cpi()
        ca = await fred.get_current_account()
        yields_10y = await fred.get_g10_yields_10y()
        yields_2y = await fred.get_g10_yields_2y()

        db.upsert_policy_rates(policy_rates)
        db.upsert_gdp_data(gdp)
        db.upsert_cpi_data(cpi_data)
        db.upsert_current_account_data(ca)

        # Courbe des taux
        curves = rates.build_yield_curve(policy_rates, yields_10y, yields_2y)
        history = db.get_latest_yield_curve()
        enriched = rates.compute_steepening_signal(curves, history)
        db.upsert_yield_curve(enriched)

        return {
            "run_id": run_id,
            "status": "ok",
            "policy_rates_updated": len(policy_rates),
            "gdp_updated": len(gdp),
            "cpi_updated": len(cpi_data),
            "ca_updated": len(ca),
            "yield_curves_updated": len(enriched),
        }
    except Exception as exc:
        logger.error("refresh_all_macro failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── COT / Positionnement ──────────────────────────────────────────────────────

@app.get("/macro/cot/{currency}")
async def get_cot_currency(currency: str):
    """Données COT CFTC pour une devise (positionnement spéculatif)."""
    rows = db.get_cot_history(currency.upper(), limit=52)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No COT data for {currency}")
    return {"currency": currency.upper(), "history": rows}


@app.get("/macro/cot")
async def get_all_cot():
    """Dernières positions COT pour toutes les devises."""
    return db.get_latest_cot()


@app.post("/macro/cot/refresh")
async def refresh_cot():
    """Télécharge et rafraîchit les données COT CFTC (hebdomadaire)."""
    run_id = str(uuid.uuid4())[:8]
    try:
        # Récupérer l'historique complet (2 ans) pour calculer les z-scores
        records = await cot.get_historical_cot(years_back=2)
        if not records:
            raise RuntimeError("COT refresh returned zero records from CFTC")
        # Calculer les z-scores sur 52 semaines
        records_with_z = cot.compute_z_scores(records, lookback_weeks=52)
        db.upsert_cot_positions(records_with_z)
        latest = db.get_latest_cot()
        if not latest:
            raise RuntimeError("COT refresh wrote zero latest currency rows")
        return {
            "run_id": run_id,
            "status": "ok",
            "records_total": len(records_with_z),
            "currencies_updated": len(latest),
            "latest_report": latest[:3] if latest else [],
        }
    except Exception as exc:
        logger.error("COT refresh failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Yield Curve / Taux ────────────────────────────────────────────────────────

@app.get("/macro/rates/{currency}")
async def get_rates_currency(currency: str):
    """Courbe des taux souverains pour une devise."""
    rows = db.get_yield_curve_history(currency.upper(), limit=90)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No rates data for {currency}")
    return {"currency": currency.upper(), "yield_curve": rows}


@app.get("/macro/rates")
async def get_all_rates():
    """Dernière snapshot de toutes les courbes des taux G10."""
    return db.get_latest_yield_curve()


# ── Pillar Scores ─────────────────────────────────────────────────────────────

@app.get("/pillars/scores")
async def get_pillar_scores():
    """Scores des 3 piliers par devise (dernière valeur)."""
    return db.get_latest_pillar_scores()


@app.get("/pillars/scores/{currency}")
async def get_pillar_scores_currency(currency: str):
    """Historique des scores piliers pour une devise."""
    rows = db.get_pillar_history(currency.upper(), limit=30)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No pillar scores for {currency}")
    return {"currency": currency.upper(), "history": rows}


@app.post("/pillars/compute")
async def compute_pillar_scores():
    """
    Calcule et sauvegarde les scores des 3 piliers pour toutes les devises.
    Combine : macro_indicators + COT + carry + yield_curve.
    """
    from scoring import compute_all_pillar_scores
    try:
        scores = compute_all_pillar_scores(db)
        db.upsert_pillar_scores(scores)
        return {
            "status": "ok",
            "currencies_scored": len(scores),
            "aligned_count": sum(1 for s in scores if s.get("all_pillars_aligned")),
            "scores": scores,
        }
    except Exception as exc:
        logger.error("compute_pillar_scores failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ── Résumé complet ────────────────────────────────────────────────────────────

@app.get("/macro/summary")
async def get_macro_summary():
    """
    Retourne un résumé complet pour le brief LLM des agents AG1-FX-V2.
    Combine : pillar_scores, yield_curve, COT, policy_rates.
    """
    pillar_scores = db.get_latest_pillar_scores()
    yield_curves = db.get_latest_yield_curve()
    cot_latest = db.get_latest_cot()
    policy_rates = db.get_latest_policy_rates()

    # Indexation par devise
    pillars_by_ccy = {r["currency"]: r for r in pillar_scores}
    yields_by_ccy = {r["currency"]: r for r in yield_curves}
    cot_by_ccy = {r["currency"]: r for r in cot_latest}
    rates_by_ccy = {r["currency"]: r for r in policy_rates}

    summary = {
        "as_of": date.today().isoformat(),
        "currencies": {},
        "steepening_opportunities": [],
        "crowded_positions": [],
        "three_pillar_opportunities": [],
        "us_yield_curve": yields_by_ccy.get("USD"),
    }

    all_ccys = set(list(pillars_by_ccy.keys()) + list(yields_by_ccy.keys()) + list(rates_by_ccy.keys()))
    for ccy in all_ccys:
        p = pillars_by_ccy.get(ccy, {})
        y = yields_by_ccy.get(ccy, {})
        c = cot_by_ccy.get(ccy, {})
        r = rates_by_ccy.get(ccy, {})
        summary["currencies"][ccy] = {
            "policy_rate_pct": r.get("rate_pct"),
            "yield_10y_pct": y.get("yield_10y_pct"),
            "yield_2y_pct": y.get("yield_2y_pct"),
            "slope_10y2y": y.get("slope_10y2y"),
            "rates_signal": y.get("rates_signal", "neutral"),
            "cot_net_spec": c.get("net_spec"),
            "cot_z_score": c.get("net_z_score"),
            "crowded_flag": c.get("crowded_flag", False),
            "crowded_direction": c.get("crowded_direction", "neutral"),
            "positioning_score": c.get("positioning_score"),
            "macro_score": p.get("macro_score"),
            "valuation_score": p.get("valuation_score"),
            "composite_score": p.get("composite_score"),
            "all_pillars_aligned": p.get("all_pillars_aligned", False),
        }
        if y.get("rates_signal") == "steepener":
            summary["steepening_opportunities"].append(ccy)
        if c.get("crowded_flag"):
            summary["crowded_positions"].append({"currency": ccy, "direction": c.get("crowded_direction"), "z_score": c.get("net_z_score")})
        if p.get("all_pillars_aligned"):
            summary["three_pillar_opportunities"].append({
                "currency": ccy,
                "composite_score": p.get("composite_score"),
                "direction": "bullish" if (p.get("composite_score") or 0) > 0 else "bearish",
            })

    return summary


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
