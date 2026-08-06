"""
Macro Data API — Service central pour le framework 3 piliers.
Expose les données macroéconomiques, COT et courbe des taux
nécessaires aux agents AG5/AG6/AG7/AG8.
"""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from cot_client import COTClient
from fred_client import FREDClient
from macro_db import MacroDB
from market_client import MarketClient
from rates_client import RatesClient
from world_bank_client import WorldBankClient
from components import build_ag5_rows, build_ag6_rows, build_ag7_rows, build_ag8_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("macro-data-api")
logging.getLogger("httpx").setLevel(logging.WARNING)

fred = FREDClient()
cot = COTClient()
db = MacroDB()
rates = RatesClient()
market = MarketClient()
world_bank = WorldBankClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Macro Data API started. DB: %s", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
    yield


app = FastAPI(title="Macro Data API", version="2.1.0", lifespan=lifespan)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "macro-data-api",
        "version": "2.1.0",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "writer": "macro-data-api",
    }


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
        unemployment = await fred.get_unemployment()
        db.upsert_gdp_data(gdp)
        db.upsert_cpi_data(cpi)
        db.upsert_current_account_data(ca)
        db.upsert_unemployment_data(unemployment)
    rows = db.get_indicators(currency=currency)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No macro data for {currency}")
    return {"currency": currency, "indicators": rows}


async def _refresh_macro_sources() -> dict:
    """Collecte les sources indépendantes et rend toute dégradation visible."""

    run_id = f"MACRO_{uuid.uuid4().hex[:12]}"
    source_errors: list[dict] = []
    policy_rates: dict = {}
    gdp: dict = {}
    cpi_data: dict = {}
    unemployment: dict = {}
    enriched: dict = {}
    world_bank_rows: list[dict] = []
    financial_conditions: Optional[dict] = None
    fred.reset_warnings()
    try:
        policy_rates = await fred.get_policy_rates()
        gdp = await fred.get_gdp_growth()
        cpi_data = await fred.get_cpi()
        unemployment = await fred.get_unemployment()
        financial_conditions = await fred.get_financial_conditions()
        yields_10y = await fred.get_g10_yields_10y()
        yields_2y = await fred.get_g10_yields_2y()
        banxico_10y, banxico_2y = await rates.get_banxico_yields()
        yields_10y.update(banxico_10y)
        yields_2y.update(banxico_2y)
        db.upsert_policy_rates(policy_rates)
        db.upsert_gdp_data(gdp)
        db.upsert_cpi_data(cpi_data)
        db.upsert_unemployment_data(unemployment)
        if financial_conditions:
            db.upsert_country_indicator(
                financial_conditions["currency"], financial_conditions["indicator"],
                financial_conditions["value"], financial_conditions["as_of"],
                financial_conditions["unit"], financial_conditions["source"],
            )
        curves = rates.build_yield_curve(policy_rates, yields_10y, yields_2y)
        history = db.get_latest_yield_curve()
        enriched = rates.compute_steepening_signal(curves, history)
        db.upsert_yield_curve(enriched)
    except Exception as exc:
        logger.exception("FRED/rates refresh failed")
        source_errors.append({"source": "FRED_OR_BANXICO", "error_code": type(exc).__name__, "detail": str(exc)[:500], "severity": "error"})
    source_errors.extend(fred.last_warnings)

    try:
        world_bank_rows = await world_bank.get_comparable_indicators()
        for row in world_bank_rows:
            db.upsert_country_indicator(
                row["currency"], row["indicator"], row["value"], row["as_of"], row["unit"], row["source"]
            )
        source_errors.extend(world_bank.last_warnings)
    except Exception as exc:
        logger.exception("World Bank refresh failed")
        source_errors.append({"source": "WORLD_BANK", "error_code": type(exc).__name__, "detail": str(exc)[:500], "severity": "error"})

    rows_written = sum((len(policy_rates), len(gdp), len(cpi_data), len(unemployment), len(enriched), len(world_bank_rows), 1 if financial_conditions else 0))
    if rows_written == 0:
        raise HTTPException(status_code=502, detail={"error_code": "MACRO_REFRESH_ZERO_ROWS", "source_errors": source_errors})
    return {
        "run_id": run_id,
        "status": "DEGRADED" if any(row.get("severity") == "error" for row in source_errors) else "OK",
        "policy_rates_updated": len(policy_rates),
        "gdp_updated": len(gdp),
        "cpi_updated": len(cpi_data),
        "current_account_absolute_audit_rows": 0,
        "unemployment_updated": len(unemployment),
        "yield_curves_updated": len(enriched),
        "financial_conditions_updated": 1 if financial_conditions else 0,
        "world_bank_comparable_rows": len(world_bank_rows),
        "source_errors": source_errors,
    }


@app.post("/macro/refresh_all")
async def refresh_all_macro():
    """Rafraîchit tous les indicateurs macro depuis FRED. À appeler 1x/jour."""
    return await _refresh_macro_sources()


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

COMPONENT_WRITERS = {
    "ag5": db.upsert_ag5_macro,
    "ag6": db.upsert_ag6_fx_valuation,
    "ag7": db.upsert_ag7_positioning,
    "ag8": db.upsert_ag8_rates_liquidity,
}

COMPONENT_QUALITY_FLOORS = {
    "ag5": {"coverage": 0.55, "confidence": 0.35, "usable_rows": 0.60},
    "ag6": {"coverage": 0.55, "confidence": 0.40, "usable_rows": 0.60},
    "ag7": {"coverage": 0.75, "confidence": 0.50, "usable_rows": 0.60},
    "ag8": {"coverage": 0.65, "confidence": 0.40, "usable_rows": 0.60},
}


def _write_component(component: str, rows: list[dict], started_at: datetime, warnings: Optional[list[dict]] = None) -> dict:
    run_id = f"{component.upper()}_{uuid.uuid4().hex[:12]}"
    warnings = warnings or []
    try:
        if not rows:
            raise RuntimeError(f"{component.upper()}_ZERO_ROWS")
        COMPONENT_WRITERS[component](rows)
        coverage = sum(float(row.get("coverage_ratio", 1.0)) for row in rows) / len(rows)
        confidence = sum(float(row.get("confidence") or 0.0) for row in rows) / len(rows)
        usable_ratio = sum(row.get("freshness_status") in {"fresh", "aging"} for row in rows) / len(rows)
        finished_at = datetime.now(timezone.utc)
        floors = COMPONENT_QUALITY_FLOORS[component]
        blocking_warning = any(row.get("severity") == "error" for row in warnings)
        quality_ok = (
            coverage >= floors["coverage"] and confidence >= floors["confidence"]
            and usable_ratio >= floors["usable_rows"] and not blocking_warning
        )
        status = "OK" if quality_ok else "DEGRADED"
        snapshot_ids = sorted({row["component_snapshot_id"] for row in rows})
        db.log_component_run({
            "run_id": run_id, "component": component.upper(), "started_at": started_at,
            "finished_at": finished_at, "status": status, "rows_read": len(rows),
            "rows_written": len(rows), "coverage_ratio": coverage,
            "payload": {"warnings": warnings, "component_snapshot_ids": snapshot_ids, "quality": {
                "coverage_ratio": coverage, "confidence": confidence, "usable_row_ratio": usable_ratio,
                "floors": floors,
            }},
        })
        return {
            "run_id": run_id, "status": status, "component": component.upper(),
            "component_snapshot_id": snapshot_ids[0] if len(snapshot_ids) == 1 else snapshot_ids,
            "rows_written": len(rows), "coverage_ratio": round(coverage, 6),
            "confidence": round(confidence, 6), "usable_row_ratio": round(usable_ratio, 6),
            "warnings": warnings, "rows": rows,
        }
    except Exception as exc:
        logger.exception("%s component failed", component)
        try:
            db.log_component_run({
                "run_id": run_id, "component": component.upper(), "started_at": started_at,
                "finished_at": datetime.now(timezone.utc), "status": "ERROR", "rows_read": len(rows),
                "rows_written": 0, "error_code": type(exc).__name__, "error_detail": str(exc)[:1000],
                "payload": {"warnings": warnings},
            })
        except Exception:
            logger.exception("Unable to persist component error log")
        raise HTTPException(status_code=500, detail={"error_code": type(exc).__name__, "detail": str(exc)[:1000]})


@app.get("/components/health")
async def get_components_health(limit: int = Query(40, ge=1, le=500)):
    return {"status": "OK", "runs": db.get_component_health(limit=limit)}


@app.get("/components/{component}")
async def get_component(component: str):
    try:
        rows = db.get_latest_component(component)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"component": component.lower(), "rows": rows, "count": len(rows)}


@app.post("/components/ag5/refresh")
async def refresh_ag5(refresh_sources: bool = Query(True)):
    started_at = datetime.now(timezone.utc)
    warnings = []
    if refresh_sources:
        try:
            source_result = await _refresh_macro_sources()
            warnings.extend(source_result.get("source_errors") or [])
        except HTTPException as exc:
            # Une panne totale de collecte ne remplace jamais les observations
            # existantes. Elles seront recalculées avec leur âge réel et donc
            # explicitement vieillissantes/périmées.
            if not db.get_latest_policy_rates() and not db.get_indicators():
                raise
            logger.exception("AG5 source refresh failed; using stale-on-error observations")
            warnings.append({
                "source": "MACRO_SOURCES", "error_code": "MACRO_REFRESH_FAILED",
                "detail": str(exc.detail)[:500], "fallback": "STALE_ON_ERROR",
            })
    return _write_component("ag5", build_ag5_rows(db, now=datetime.now(timezone.utc)), started_at, warnings)


@app.post("/components/ag6/compute")
async def compute_ag6():
    started_at = datetime.now(timezone.utc)
    warnings = []
    spots = {}
    try:
        spots = await market.get_fx_spots()
        if not spots:
            raise RuntimeError("FX_SPOT_ZERO_ROWS")
    except Exception as exc:
        logger.exception("AG6 market spot refresh failed; computing explicit degraded output")
        warnings.append({"source": "YFINANCE_API", "error_code": type(exc).__name__, "detail": str(exc)[:500]})
    return _write_component("ag6", build_ag6_rows(db, spots, now=datetime.now(timezone.utc)), started_at, warnings)


@app.post("/components/ag7/refresh")
async def refresh_ag7(refresh_source: bool = Query(True)):
    started_at = datetime.now(timezone.utc)
    warnings = []
    if refresh_source:
        try:
            records = await cot.get_historical_cot(years_back=2)
            if not records:
                raise RuntimeError("COT_REFRESH_ZERO_ROWS")
            records_with_z = cot.compute_z_scores(records, lookback_weeks=52)
            if not records_with_z:
                raise RuntimeError("COT_ZSCORE_ZERO_ROWS")
            db.upsert_cot_positions(records_with_z)
        except Exception as exc:
            if not db.get_latest_cot():
                raise HTTPException(status_code=502, detail={"error_code": type(exc).__name__, "detail": str(exc)[:1000]})
            logger.exception("COT refresh failed; using stale-on-error snapshot")
            warnings.append({"source": "CFTC_COT", "error_code": type(exc).__name__, "detail": str(exc)[:500], "fallback": "STALE_ON_ERROR"})
    return _write_component("ag7", build_ag7_rows(db, now=datetime.now(timezone.utc)), started_at, warnings)


@app.post("/components/ag8/compute")
async def compute_ag8():
    started_at = datetime.now(timezone.utc)
    return _write_component("ag8", build_ag8_rows(db, now=datetime.now(timezone.utc)), started_at)

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
    raise HTTPException(
        status_code=410,
        detail={
            "error_code": "LEGACY_PREMATURE_COMPOSITE_DISABLED",
            "replacement": [
                "/components/ag5/refresh", "/components/ag6/compute",
                "/components/ag7/refresh", "/components/ag8/compute",
            ],
        },
    )


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
            "data_completeness": p.get("data_completeness"),
            "score_status": p.get("score_status"),
            "confidence_floor": p.get("confidence_floor"),
            "missing_inputs": p.get("missing_inputs"),
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
