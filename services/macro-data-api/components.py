"""Orchestration canonique des composants analytiques AG5 a AG8.

Ce module lit les observations normalisees par ``MacroDB`` et produit des
snapshots autonomes. Il ne declenche aucun ordre et ne calcule pas la synthese
AG5-AG9, qui appartient au service Global-Context-Synthesizer.
"""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Optional

from macro_db import MacroDB
from scoring import (
    clamp,
    compute_ag5_macro,
    compute_fx_valuation,
    cot_positioning_score,
    effective_age_hours,
    finite_or_none,
    freshness_status,
    rates_regime,
    synthetic_usd_positioning,
)


CURRENCIES = ("USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD", "MXN", "SEK", "NOK", "KRW")
CONFIDENCE_MAP = {"high": 0.90, "medium": 0.60, "low": 0.35, "missing": 0.0}
FRESHNESS_RANK = {"fresh": 0, "aging": 1, "stale": 2, "missing": 3}
AG5_THRESHOLDS = {
    "growth": (24 * 120, 24 * 190),
    "growth_momentum": (24 * 120, 24 * 190),
    "inflation": (24 * 45, 24 * 75),
    "policy_rate": (24 * 45, 24 * 120),
    "current_account_pct_gdp": (24 * 400, 24 * 550),
    "fiscal_balance_pct_gdp": (24 * 400, 24 * 550),
    "unemployment_change_pp": (24 * 45, 24 * 75),
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def snapshot_id(prefix: str, now: datetime) -> str:
    return f"{prefix}_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def parse_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if len(text) == 10:
        text += "T00:00:00+00:00"
    elif text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value: Any) -> Optional[str]:
    parsed = parse_time(value)
    return parsed.isoformat() if parsed else None


def worst_freshness(statuses: Iterable[str]) -> str:
    valid = [str(status) for status in statuses if status]
    return max(valid, key=lambda value: FRESHNESS_RANK.get(value, 3)) if valid else "missing"


def _metric(
    row: Optional[dict],
    *,
    name: str,
    now: datetime,
    fresh_hours: float,
    stale_hours: float,
    value: Any = None,
    status: str = "direct_observation",
    confidence: float = 0.90,
) -> dict:
    if not row:
        return {
            "value": None,
            "status": "unavailable",
            "freshness_status": "missing",
            "confidence": 0.0,
            "source": None,
            "observation_time": None,
            "publication_time": None,
            "ingestion_time": None,
            "age_hours": None,
        }
    observed = iso(row.get("as_of") or row.get("observation_time"))
    age = effective_age_hours(observed, row.get("age_hours"), now=now)
    fresh = freshness_status(age, fresh_hours=fresh_hours, stale_hours=stale_hours)
    freshness_factor = {"fresh": 1.0, "aging": 0.70, "stale": 0.25, "missing": 0.0}[fresh]
    raw_value = row.get("value") if value is None else value
    return {
        "name": name,
        "value": finite_or_none(raw_value),
        "unit": row.get("unit"),
        "status": status,
        "freshness_status": fresh,
        "confidence": round(clamp(confidence * freshness_factor, 0.0, 1.0), 6),
        "source": row.get("source"),
        "observation_time": observed,
        "publication_time": iso(row.get("publication_time")),
        "ingestion_time": iso(row.get("updated_at") or row.get("ingestion_time")),
        "age_hours": age,
    }


def _latest_by_indicator(rows: list[dict]) -> dict[tuple[str, str], dict]:
    latest: dict[tuple[str, str], dict] = {}
    for row in rows:
        key = (str(row.get("currency") or "").upper(), str(row.get("indicator") or ""))
        if not key[0] or not key[1]:
            continue
        if key not in latest or str(row.get("as_of") or "") > str(latest[key].get("as_of") or ""):
            latest[key] = row
    return latest


def _previous_indicator(rows: list[dict], currency: str, indicator: str) -> Optional[dict]:
    matches = sorted(
        [row for row in rows if row.get("currency") == currency and row.get("indicator") == indicator],
        key=lambda row: str(row.get("as_of") or ""),
        reverse=True,
    )
    return matches[1] if len(matches) > 1 else None


def build_ag5_rows(db: MacroDB, *, now: Optional[datetime] = None) -> list[dict]:
    now = now or utcnow()
    component_id = snapshot_id("AG5", now)
    indicator_rows = db.get_indicators()
    latest = _latest_by_indicator(indicator_rows)
    policies = {str(row["currency"]): row for row in db.get_latest_policy_rates()}
    neutral_rates = db.get_neutral_rates()
    output = []
    for currency in CURRENCIES:
        metric_rows = {
            "growth": latest.get((currency, "gdp_growth_qoq")),
            "growth_momentum": latest.get((currency, "gdp_momentum")),
            "inflation": latest.get((currency, "cpi_yoy")),
            "policy_rate": policies.get(currency),
            "current_account_pct_gdp": latest.get((currency, "current_account_pct_gdp")),
            "fiscal_balance_pct_gdp": latest.get((currency, "fiscal_balance_pct_gdp")),
        }
        unemployment = latest.get((currency, "unemployment_pct"))
        previous_unemployment = _previous_indicator(indicator_rows, currency, "unemployment_pct")
        unemployment_change = None
        if unemployment and previous_unemployment:
            current_value = finite_or_none(unemployment.get("value"))
            previous_value = finite_or_none(previous_unemployment.get("value"))
            if current_value is not None and previous_value is not None:
                unemployment_change = current_value - previous_value
        metric_rows["unemployment_change_pp"] = unemployment

        metrics = {}
        for name, source_row in metric_rows.items():
            fresh_hours, stale_hours = AG5_THRESHOLDS[name]
            value = unemployment_change if name == "unemployment_change_pp" else None
            metrics[name] = _metric(
                source_row,
                name=name,
                now=now,
                fresh_hours=fresh_hours,
                stale_hours=stale_hours,
                value=value,
                status="calculated_value" if name == "unemployment_change_pp" and value is not None else "direct_observation",
            )
            if name == "unemployment_change_pp" and value is not None:
                metrics[name]["previous_observation_time"] = iso(previous_unemployment.get("as_of"))
                metrics[name]["previous_value"] = finite_or_none(previous_unemployment.get("value"))

        computed = compute_ag5_macro(currency, metrics, neutral_rate=neutral_rates.get(currency))
        observations = [parse_time(row.get("observation_time")) for row in metrics.values() if row.get("value") is not None]
        ingestions = [parse_time(row.get("ingestion_time")) for row in metrics.values() if row.get("ingestion_time")]
        proxy_inputs = []
        if latest.get((currency, "current_account_bn_usd")) and not metric_rows["current_account_pct_gdp"]:
            proxy_inputs.append("current_account_bn_usd_excluded_not_comparable")
        output.append({
            **computed,
            "component_snapshot_id": component_id,
            "entity_type": "country_or_currency",
            "observation_time": min(observations).isoformat() if observations else None,
            "publication_time": None,
            "ingestion_time": max(ingestions).isoformat() if ingestions else now.isoformat(),
            "calculation_time": now.isoformat(),
            "freshness_status": worst_freshness(row["freshness_status"] for row in metrics.values() if row.get("value") is not None),
            "proxy_inputs": proxy_inputs,
            "lineage": {"metrics": metrics, "neutral_rate": neutral_rates.get(currency), "excluded_inputs": proxy_inputs},
            "source": "MACRO_DATA_API",
        })
    return output


def build_ag6_rows(
    db: MacroDB,
    spots: dict[str, dict],
    *,
    now: Optional[datetime] = None,
) -> list[dict]:
    now = now or utcnow()
    component_id = snapshot_id("AG6", now)
    indicators = db.get_indicators()
    latest = _latest_by_indicator(indicators)
    policy_rows = {str(row["currency"]): row for row in db.get_latest_policy_rates()}
    policy_values = {currency: finite_or_none(row.get("rate_pct")) for currency, row in policy_rows.items()}
    inflation_values = {
        currency: finite_or_none((latest.get((currency, "cpi_yoy")) or {}).get("value"))
        for currency in CURRENCIES
    }
    nominal_population = [value for value in policy_values.values() if value is not None]
    real_values = {
        currency: policy_values.get(currency) - inflation_values[currency]
        for currency in CURRENCIES
        if policy_values.get(currency) is not None and inflation_values.get(currency) is not None
    }
    real_population = list(real_values.values())
    nominal_anchor = median(nominal_population) if nominal_population else None
    real_anchor = median(real_population) if real_population else None
    output = []
    for currency in CURRENCIES:
        policy = _metric(policy_rows.get(currency), name="policy_rate", now=now, fresh_hours=24 * 45, stale_hours=24 * 120, value=policy_values.get(currency))
        inflation = _metric(latest.get((currency, "cpi_yoy")), name="inflation", now=now, fresh_hours=24 * 45, stale_hours=24 * 75)
        spot_raw = spots.get(currency)
        spot = _metric(spot_raw, name="spot_reference", now=now, fresh_hours=24, stale_hours=72, value=(spot_raw or {}).get("value"), status=(spot_raw or {}).get("status", "direct_observation"), confidence=float((spot_raw or {}).get("confidence", 0.0)))
        ppp = _metric(latest.get((currency, "ppp_fair_value_usd")), name="ppp_fair_value", now=now, fresh_hours=24 * 400, stale_hours=24 * 550)
        reer = _metric(latest.get((currency, "reer_gap_pct")), name="reer_gap", now=now, fresh_hours=24 * 60, stale_hours=24 * 100)
        terms = _metric(latest.get((currency, "terms_of_trade_score")), name="terms_of_trade", now=now, fresh_hours=24 * 120, stale_hours=24 * 190)
        nominal_carry = None if policy["value"] is None or nominal_anchor is None else policy["value"] - nominal_anchor
        real_carry = None if currency not in real_values or real_anchor is None else real_values[currency] - real_anchor
        statuses = {
            "carry": "calculated_value" if nominal_carry is not None else "unavailable",
            "real_carry": "calculated_value" if real_carry is not None else "unavailable",
            "spot_reference": spot["status"] if spot["value"] is not None else "unavailable",
            "ppp": "calculated_value" if ppp["value"] is not None and spot["value"] is not None else "unavailable",
            "reer": reer["status"] if reer["value"] is not None else "unavailable",
            "terms_of_trade": terms["status"] if terms["value"] is not None else "unavailable",
        }
        stale = set()
        if policy["freshness_status"] == "stale":
            stale.add("carry")
        if policy["freshness_status"] == "stale" or inflation["freshness_status"] == "stale":
            stale.add("real_carry")
        if ppp["freshness_status"] == "stale" or spot["freshness_status"] == "stale":
            stale.add("ppp")
        if reer["freshness_status"] == "stale":
            stale.add("reer")
        if terms["freshness_status"] == "stale":
            stale.add("terms_of_trade")
        confidences = {
            "carry": policy["confidence"],
            "real_carry": min(policy["confidence"], inflation["confidence"]),
            "ppp": min(ppp["confidence"], spot["confidence"]),
            "reer": reer["confidence"],
            "terms_of_trade": terms["confidence"],
        }
        computed = compute_fx_valuation(
            currency,
            nominal_carry_pct=nominal_carry,
            real_carry_pct=real_carry,
            spot_reference=spot["value"],
            ppp_fair_value=ppp["value"],
            reer_gap_pct=reer["value"],
            terms_of_trade_score=terms["value"],
            input_confidence=confidences,
            stale_inputs=stale,
        )
        lineage_metrics = {"policy_rate": policy, "inflation": inflation, "spot_reference": spot, "ppp_fair_value": ppp, "reer_gap": reer, "terms_of_trade": terms}
        observations = [parse_time(row.get("observation_time")) for row in lineage_metrics.values() if row.get("value") is not None]
        ingestions = [parse_time(row.get("ingestion_time")) for row in lineage_metrics.values() if row.get("ingestion_time")]
        output.append({
            **computed,
            "component_snapshot_id": component_id,
            "observation_time": min(observations).isoformat() if observations else None,
            "publication_time": None,
            "ingestion_time": max(ingestions).isoformat() if ingestions else now.isoformat(),
            "calculation_time": now.isoformat(),
            "freshness_status": worst_freshness(row["freshness_status"] for row in lineage_metrics.values() if row.get("value") is not None),
            "input_status": statuses,
            "lineage": {"metrics": lineage_metrics, "nominal_anchor": nominal_anchor, "real_anchor": real_anchor, "anchor_method": "cross_sectional_median_available_currencies"},
            "source": "MACRO_DATA_API+YFINANCE_API",
        })
    return output


def _load_positioning_config() -> dict:
    path = Path(__file__).resolve().parent / "config" / "positioning.json"
    return json.loads(path.read_text(encoding="utf-8"))


def build_ag7_rows(db: MacroDB, *, now: Optional[datetime] = None) -> list[dict]:
    now = now or utcnow()
    config = _load_positioning_config()
    component_id = snapshot_id("AG7", now)
    threshold = float(config["crowded_z_threshold"])
    fresh_hours = float(config["fresh_hours"])
    stale_hours = float(config["stale_hours"])
    direct_rows = db.get_latest_cot()
    if not direct_rows:
        raise ValueError("AG7_COT_ZERO_ROWS")
    output = []
    usable_for_usd = {}
    for source_row in direct_rows:
        currency = str(source_row.get("currency") or "").upper()
        if not currency:
            continue
        z_score = finite_or_none(source_row.get("net_z_score"))
        report_date = source_row.get("report_date")
        age = effective_age_hours(report_date, now=now)
        fresh = freshness_status(age, fresh_hours=fresh_hours, stale_hours=stale_hours)
        base_confidence = CONFIDENCE_MAP.get(str(source_row.get("confidence") or "high").lower(), 0.60)
        confidence = base_confidence * {"fresh": 1.0, "aging": 0.70, "stale": 0.25, "missing": 0.0}[fresh]
        direction = "unknown" if z_score is None else ("long" if z_score >= threshold else "short" if z_score <= -threshold else "neutral")
        row = {
            "component": "AG7_POSITIONING",
            "schema_version": "AG7_POSITIONING_V2",
            "method_version": "AG7_POSITIONING_V2",
            "component_snapshot_id": component_id,
            "entity_id": currency,
            "report_date": str(report_date)[:10] if report_date else None,
            "observation_time": iso(report_date),
            "publication_time": None,
            "ingestion_time": iso(source_row.get("updated_at")) or now.isoformat(),
            "calculation_time": now.isoformat(),
            "net_position": finite_or_none(source_row.get("net_spec")),
            "z_score": z_score,
            "positioning_score": cot_positioning_score(z_score),
            "crowded_flag": bool(z_score is not None and abs(z_score) >= threshold),
            "crowded_direction": direction,
            "crowded_threshold": threshold,
            "is_proxy": False,
            "contributors": [],
            "weights": {},
            "confidence": round(clamp(confidence, 0.0, 1.0), 6),
            "freshness_status": fresh,
            "missing_inputs": [] if z_score is not None else ["z_score"],
            "lineage": {"source_row": source_row, "config_version": config["config_version"], "score_direction": "contrarian_negative_z_is_positive"},
            "source": source_row.get("source") or "CFTC_COT",
        }
        output.append(row)
        if z_score is not None and currency != "USD":
            usable_for_usd[currency] = {"z_score": z_score}
    if not any(row["entity_id"] == "USD" and not row["is_proxy"] for row in output):
        synthetic = synthetic_usd_positioning(usable_for_usd, config["usd_synthetic_weights"])
        if synthetic:
            contributor_rows = [row for row in output if row["entity_id"] in synthetic["contributors"]]
            usd_fresh = worst_freshness(row["freshness_status"] for row in contributor_rows)
            usd_observed = [parse_time(row["observation_time"]) for row in contributor_rows if row.get("observation_time")]
            usd_report = max((row["report_date"] for row in contributor_rows if row.get("report_date")), default=None)
            z_score = synthetic["z_score"]
            output.append({
                "component": "AG7_POSITIONING", "schema_version": "AG7_POSITIONING_V2", "method_version": "AG7_POSITIONING_V2",
                "component_snapshot_id": component_id, "entity_id": "USD", "report_date": usd_report,
                "observation_time": min(usd_observed).isoformat() if usd_observed else None,
                "publication_time": None, "ingestion_time": now.isoformat(), "calculation_time": now.isoformat(),
                "net_position": None, "z_score": z_score, "positioning_score": synthetic["positioning_score"],
                "crowded_flag": abs(z_score) >= threshold,
                "crowded_direction": "long" if z_score >= threshold else "short" if z_score <= -threshold else "neutral",
                "crowded_threshold": threshold, "is_proxy": True, "contributors": synthetic["contributors"],
                "weights": synthetic["weights"], "confidence": min(0.60, synthetic["confidence"]),
                "freshness_status": usd_fresh, "missing_inputs": ["direct_usd_cot_contract"],
                "lineage": {"derived_from": [row["entity_id"] for row in contributor_rows], "config_version": config["config_version"], "score_direction": "inverse_weighted_basket_then_contrarian"},
                "source": "CFTC_SYNTHETIC_USD_BASKET",
            })
    if not output:
        raise ValueError("AG7_ZERO_VALID_ROWS")
    return output


def _nearest_curve_change(history: list[dict], latest: dict) -> dict[str, Optional[float]]:
    latest_time = parse_time(latest.get("as_of"))
    if not latest_time:
        return {"slope_change": None, "yield_2y_change": None, "yield_10y_change": None, "baseline_as_of": None}
    candidates = []
    for row in history:
        row_time = parse_time(row.get("as_of"))
        if not row_time or row_time >= latest_time:
            continue
        age_days = (latest_time - row_time).total_seconds() / 86400.0
        if 20 <= age_days <= 45:
            candidates.append((abs(age_days - 30.0), row_time, row))
    if not candidates:
        return {"slope_change": None, "yield_2y_change": None, "yield_10y_change": None, "baseline_as_of": None}
    baseline = sorted(candidates, key=lambda value: (value[0], -value[1].timestamp()))[0][2]
    latest_y2, latest_y10 = finite_or_none(latest.get("yield_2y_pct")), finite_or_none(latest.get("yield_10y_pct"))
    base_y2, base_y10 = finite_or_none(baseline.get("yield_2y_pct")), finite_or_none(baseline.get("yield_10y_pct"))
    y2_change = None if latest_y2 is None or base_y2 is None else latest_y2 - base_y2
    y10_change = None if latest_y10 is None or base_y10 is None else latest_y10 - base_y10
    slope_change = None if y2_change is None or y10_change is None else y10_change - y2_change
    return {"slope_change": slope_change, "yield_2y_change": y2_change, "yield_10y_change": y10_change, "baseline_as_of": baseline.get("as_of")}


def _ag8_overlays(regime: dict, real_rate: Optional[float]) -> dict:
    duration = finite_or_none(regime.get("duration_pressure"))
    slope = finite_or_none(regime.get("slope_10y2y"))
    pressure = None if duration is None else max(0.0, duration)
    financials = None if slope is None else (clamp(max(0.0, slope) / 2.0) if regime.get("curve_regime") in {"bear_steepening", "normal"} else 0.0)
    carry = None if real_rate is None else clamp(math.tanh(real_rate / 3.0))
    return {
        "growth_equities_pressure": {"value": pressure, "inputs": ["duration_pressure"]},
        "financials_tailwind": {"value": financials, "inputs": ["slope_10y2y", "curve_regime"]},
        "real_estate_pressure": {"value": pressure, "inputs": ["duration_pressure"]},
        "utilities_pressure": {"value": pressure, "inputs": ["duration_pressure"]},
        "cyclicals_tailwind": {"value": financials, "inputs": ["slope_10y2y", "curve_regime"]},
        "currency_carry_support": {"value": carry, "inputs": ["policy_rate", "inflation"]},
        "duration_asset_pressure": {"value": pressure, "inputs": ["yield_10y", "neutral_rate_estimate"]},
    }


def build_ag8_rows(db: MacroDB, *, now: Optional[datetime] = None) -> list[dict]:
    now = now or utcnow()
    component_id = snapshot_id("AG8", now)
    curves = {str(row["currency"]): row for row in db.get_latest_yield_curve()}
    policies = {str(row["currency"]): row for row in db.get_latest_policy_rates()}
    neutral_rates = db.get_neutral_rates()
    latest_indicators = _latest_by_indicator(db.get_indicators())
    if not curves:
        raise ValueError("AG8_RATES_ZERO_ROWS")
    output = []
    for currency, curve_row in sorted(curves.items()):
        history = db.get_yield_curve_history(currency, limit=120)
        changes = _nearest_curve_change(history, curve_row)
        curve_metric = _metric(curve_row, name="yield_curve", now=now, fresh_hours=72, stale_hours=168, value=curve_row.get("yield_10y_pct"))
        policy_metric = _metric(policies.get(currency), name="policy_rate", now=now, fresh_hours=24 * 45, stale_hours=24 * 120, value=(policies.get(currency) or {}).get("rate_pct"))
        inflation_metric = _metric(latest_indicators.get((currency, "cpi_yoy")), name="inflation", now=now, fresh_hours=24 * 45, stale_hours=24 * 75)
        neutral = neutral_rates.get(currency) or {}
        regime = rates_regime(
            yield_2y=curve_row.get("yield_2y_pct"), yield_10y=curve_row.get("yield_10y_pct"),
            slope_change=changes["slope_change"], policy_rate=policy_metric["value"], neutral_rate=neutral.get("rate_pct"),
            yield_2y_change=changes["yield_2y_change"], yield_10y_change=changes["yield_10y_change"],
        )
        real_rate = None if policy_metric["value"] is None or inflation_metric["value"] is None else policy_metric["value"] - inflation_metric["value"]
        available_weights = {
            "yield_2y": 0.20 if finite_or_none(curve_row.get("yield_2y_pct")) is not None else 0.0,
            "yield_10y": 0.20 if finite_or_none(curve_row.get("yield_10y_pct")) is not None else 0.0,
            "slope_change": 0.20 if changes["slope_change"] is not None else 0.0,
            "policy": 0.15 if policy_metric["value"] is not None else 0.0,
            "real_rate": 0.15 if real_rate is not None else 0.0,
            "liquidity": 0.0,
        }
        coverage = sum(available_weights.values())
        confidence_inputs = [curve_metric["confidence"]]
        if policy_metric["value"] is not None:
            confidence_inputs.append(policy_metric["confidence"])
        if real_rate is not None:
            confidence_inputs.append(min(policy_metric["confidence"], inflation_metric["confidence"]))
        confidence = (sum(confidence_inputs) / len(confidence_inputs)) * coverage if confidence_inputs else 0.0
        freshness = worst_freshness([curve_metric["freshness_status"], policy_metric["freshness_status"], inflation_metric["freshness_status"]])
        missing = [name for name, weight in available_weights.items() if weight == 0.0]
        stale = [name for name, metric in {"yield_curve": curve_metric, "policy_rate": policy_metric, "inflation": inflation_metric}.items() if metric["freshness_status"] == "stale"]
        output.append({
            "component": "AG8_RATES_LIQUIDITY", "schema_version": "AG8_RATES_V2", "method_version": "AG8_RATES_V2",
            "component_snapshot_id": component_id, "currency": currency,
            "observation_time": curve_metric["observation_time"], "publication_time": None,
            "ingestion_time": curve_metric["ingestion_time"] or now.isoformat(), "calculation_time": now.isoformat(),
            **regime, "real_rate": real_rate, "liquidity_score": None,
            "overlays": _ag8_overlays(regime, real_rate), "coverage_ratio": round(coverage, 6),
            "confidence": round(clamp(confidence, 0.0, 1.0), 6), "freshness_status": freshness,
            "missing_inputs": missing, "stale_inputs": stale, "proxy_inputs": ["neutral_rate_estimate"],
            "lineage": {"yield_curve": curve_metric, "policy_rate": policy_metric, "inflation": inflation_metric, "neutral_rate": neutral, "change_baseline_as_of": changes["baseline_as_of"], "coverage_weights": {"yield_2y": 0.20, "yield_10y": 0.20, "slope_change": 0.20, "policy": 0.15, "real_rate": 0.15, "liquidity": 0.10}},
            "source": "MACRO_DATA_API",
        })
    return output
