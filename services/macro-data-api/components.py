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


def aggregate_freshness(statuses: Iterable[str]) -> str:
    valid = [str(status or "missing") for status in statuses]
    if not valid:
        return "missing"
    fresh_ratio = sum(status == "fresh" for status in valid) / len(valid)
    usable_ratio = sum(status in {"fresh", "aging"} for status in valid) / len(valid)
    if fresh_ratio >= 0.75:
        return "fresh"
    if usable_ratio >= 0.60:
        return "aging"
    return "stale" if any(status != "missing" for status in valid) else "missing"


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


def _latest_candidate(latest: dict[tuple[str, str], dict], currency: str, indicators: tuple[str, ...]) -> Optional[dict]:
    candidates = [latest.get((currency, indicator)) for indicator in indicators]
    candidates = [row for row in candidates if row]
    return max(candidates, key=lambda row: str(row.get("as_of") or "")) if candidates else None


def _freshest_candidate(
    latest: dict[tuple[str, str], dict], currency: str, indicators: tuple[str, ...], *, now: datetime,
    direct_fresh_hours: float, direct_stale_hours: float,
) -> Optional[dict]:
    candidates = [latest.get((currency, indicator)) for indicator in indicators]
    candidates = [row for row in candidates if row]
    if not candidates:
        return None

    def rank(row: dict) -> tuple[int, float]:
        annual = "WORLD_BANK" in str(row.get("source") or "").upper()
        fresh_hours = 24 * 400 if annual else direct_fresh_hours
        stale_hours = 24 * 550 if annual else direct_stale_hours
        status = freshness_status(effective_age_hours(row.get("as_of"), now=now), fresh_hours=fresh_hours, stale_hours=stale_hours)
        observed = parse_time(row.get("as_of"))
        return FRESHNESS_RANK[status], -(observed.timestamp() if observed else 0.0)

    return min(candidates, key=rank)


def _source_confidence(row: Optional[dict], *, direct: float = 0.90, annual: float = 0.70) -> float:
    if not row:
        return 0.0
    source = str(row.get("source") or "").upper()
    confidence = annual if "WORLD_BANK" in source else direct
    if "COUNTRY_PROXY" in source:
        confidence = min(confidence, 0.50)
    if "POLICY_CURVE_PROXY" in source:
        confidence = min(confidence, 0.45)
    return confidence


def build_ag5_rows(db: MacroDB, *, now: Optional[datetime] = None) -> list[dict]:
    now = now or utcnow()
    component_id = snapshot_id("AG5", now)
    indicator_rows = db.get_indicators()
    latest = _latest_by_indicator(indicator_rows)
    policies = {str(row["currency"]): row for row in db.get_latest_policy_rates()}
    neutral_rates = db.get_neutral_rates()
    output = []
    for currency in CURRENCIES:
        growth_row = _freshest_candidate(
            latest, currency, ("gdp_growth_qoq", "gdp_growth_annual"), now=now,
            direct_fresh_hours=24 * 120, direct_stale_hours=24 * 190,
        )
        growth_indicator = str((growth_row or {}).get("indicator") or "")
        growth_momentum_name = "gdp_momentum_annual" if growth_indicator == "gdp_growth_annual" else "gdp_momentum"
        inflation_row = _freshest_candidate(
            latest, currency, ("cpi_yoy", "cpi_yoy_annual"), now=now,
            direct_fresh_hours=24 * 45, direct_stale_hours=24 * 75,
        )
        unemployment = _latest_candidate(latest, currency, ("unemployment_pct",))
        direct_unemployment_change = latest.get((currency, "unemployment_change_pp_annual"))
        metric_rows = {
            "growth": growth_row,
            "growth_momentum": latest.get((currency, growth_momentum_name)),
            "inflation": inflation_row,
            "policy_rate": policies.get(currency),
            "current_account_pct_gdp": latest.get((currency, "current_account_pct_gdp")),
            "fiscal_balance_pct_gdp": latest.get((currency, "fiscal_balance_pct_gdp")),
        }
        previous_unemployment = _previous_indicator(indicator_rows, currency, "unemployment_pct")
        unemployment_change = finite_or_none((direct_unemployment_change or {}).get("value"))
        unemployment_source = direct_unemployment_change or unemployment
        if unemployment_change is None and unemployment and previous_unemployment:
            current_value = finite_or_none(unemployment.get("value"))
            previous_value = finite_or_none(previous_unemployment.get("value"))
            if current_value is not None and previous_value is not None:
                unemployment_change = current_value - previous_value
        metric_rows["unemployment_change_pp"] = unemployment_source

        metrics = {}
        for name, source_row in metric_rows.items():
            fresh_hours, stale_hours = AG5_THRESHOLDS[name]
            is_annual = "WORLD_BANK" in str((source_row or {}).get("source") or "").upper()
            if is_annual and name in {"growth", "growth_momentum", "inflation", "unemployment_change_pp"}:
                fresh_hours, stale_hours = 24 * 400, 24 * 550
            elif is_annual and name in {"current_account_pct_gdp", "fiscal_balance_pct_gdp"}:
                fresh_hours, stale_hours = 24 * 550, 24 * 730
            value = unemployment_change if name == "unemployment_change_pp" else None
            if name == "policy_rate":
                value = (source_row or {}).get("rate_pct")
            metric_status = "direct_observation"
            if is_annual and name in {"growth", "growth_momentum", "inflation", "unemployment_change_pp"}:
                metric_status = "proxy_observation"
            elif is_annual:
                metric_status = "structural_annual_observation"
            elif name == "unemployment_change_pp" and value is not None:
                metric_status = "calculated_value"
            metrics[name] = _metric(
                source_row,
                name=name,
                now=now,
                fresh_hours=fresh_hours,
                stale_hours=stale_hours,
                value=value,
                status=metric_status,
                confidence=_source_confidence(source_row),
            )
            if name == "unemployment_change_pp" and value is not None:
                metrics[name]["previous_observation_time"] = iso((previous_unemployment or {}).get("as_of"))
                metrics[name]["previous_value"] = finite_or_none((previous_unemployment or {}).get("value"))

        computed = compute_ag5_macro(currency, metrics, neutral_rate=neutral_rates.get(currency))
        observations = [parse_time(row.get("observation_time")) for row in metrics.values() if row.get("value") is not None]
        ingestions = [parse_time(row.get("ingestion_time")) for row in metrics.values() if row.get("ingestion_time")]
        proxy_inputs = []
        proxy_inputs.extend(name for name, row in metrics.items() if row.get("status") == "proxy_observation")
        excluded_inputs = []
        if latest.get((currency, "current_account_bn_usd")) and not metric_rows["current_account_pct_gdp"]:
            excluded_inputs.append("current_account_bn_usd_excluded_not_comparable")
        output.append({
            **computed,
            "component_snapshot_id": component_id,
            "entity_type": "country_or_currency",
            "observation_time": min(observations).isoformat() if observations else None,
            "publication_time": None,
            "ingestion_time": max(ingestions).isoformat() if ingestions else now.isoformat(),
            "calculation_time": now.isoformat(),
            "freshness_status": aggregate_freshness(row["freshness_status"] for row in metrics.values() if row.get("value") is not None),
            "proxy_inputs": proxy_inputs,
            "lineage": {"metrics": metrics, "neutral_rate": neutral_rates.get(currency), "proxy_inputs": proxy_inputs, "excluded_inputs": excluded_inputs},
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
    inflation_rows = {
        currency: _freshest_candidate(
            latest, currency, ("cpi_yoy", "cpi_yoy_annual"), now=now,
            direct_fresh_hours=24 * 45, direct_stale_hours=24 * 75,
        )
        for currency in CURRENCIES
    }
    inflation_values = {currency: finite_or_none((row or {}).get("value")) for currency, row in inflation_rows.items()}
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
        policy = _metric(policy_rows.get(currency), name="policy_rate", now=now, fresh_hours=24 * 45, stale_hours=24 * 120, value=policy_values.get(currency), confidence=_source_confidence(policy_rows.get(currency)))
        inflation_row = inflation_rows[currency]
        inflation_is_annual = "WORLD_BANK" in str((inflation_row or {}).get("source") or "").upper()
        inflation = _metric(
            inflation_row, name="inflation", now=now,
            fresh_hours=24 * (400 if inflation_is_annual else 45), stale_hours=24 * (550 if inflation_is_annual else 75),
            confidence=_source_confidence(inflation_row), status="proxy_observation" if inflation_is_annual else "direct_observation",
        )
        spot_raw = spots.get(currency)
        spot = _metric(spot_raw, name="spot_reference", now=now, fresh_hours=24, stale_hours=72, value=(spot_raw or {}).get("value"), status=(spot_raw or {}).get("status", "direct_observation"), confidence=float((spot_raw or {}).get("confidence", 0.0)))
        ppp_row = latest.get((currency, "ppp_fair_value_usd"))
        reer_row = latest.get((currency, "reer_gap_pct"))
        terms_row = latest.get((currency, "terms_of_trade_score"))
        ppp = _metric(ppp_row, name="ppp_fair_value", now=now, fresh_hours=24 * 400, stale_hours=24 * 550, confidence=_source_confidence(ppp_row), status="structural_annual_observation")
        reer = _metric(reer_row, name="reer_gap", now=now, fresh_hours=24 * 400, stale_hours=24 * 550, confidence=_source_confidence(reer_row, annual=0.60), status="recent_history_derived")
        terms = _metric(terms_row, name="terms_of_trade", now=now, fresh_hours=24 * 500, stale_hours=24 * 730, confidence=_source_confidence(terms_row, annual=0.55), status="recent_history_derived")
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
            "freshness_status": aggregate_freshness(row["freshness_status"] for row in lineage_metrics.values() if row.get("value") is not None),
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
    liquidity_row = latest_indicators.get(("USD", "global_financial_conditions_score"))
    if not curves:
        raise ValueError("AG8_RATES_ZERO_ROWS")
    output = []
    for currency, curve_row in sorted(curves.items()):
        history = db.get_yield_curve_history(currency, limit=120)
        changes = _nearest_curve_change(history, curve_row)
        curve_source = str(curve_row.get("source") or "").lower()
        curve_is_proxy = "policy_curve_proxy" in curve_source
        curve_is_monthly = currency != "USD" and not curve_is_proxy
        curve_metric = _metric(
            curve_row, name="yield_curve", now=now,
            fresh_hours=24 * (45 if curve_is_proxy else 75 if curve_is_monthly else 3),
            stale_hours=24 * (120 if curve_is_proxy or curve_is_monthly else 7),
            value=curve_row.get("yield_10y_pct"),
            confidence=0.45 if curve_is_proxy else 0.75 if curve_is_monthly else 0.90,
            status="proxy_curve" if curve_is_proxy else "monthly_observation" if curve_is_monthly else "direct_observation",
        )
        policy_metric = _metric(policies.get(currency), name="policy_rate", now=now, fresh_hours=24 * 45, stale_hours=24 * 120, value=(policies.get(currency) or {}).get("rate_pct"), confidence=_source_confidence(policies.get(currency)))
        inflation_row = _freshest_candidate(
            latest_indicators, currency, ("cpi_yoy", "cpi_yoy_annual"), now=now,
            direct_fresh_hours=24 * 45, direct_stale_hours=24 * 75,
        )
        inflation_is_annual = "WORLD_BANK" in str((inflation_row or {}).get("source") or "").upper()
        inflation_metric = _metric(
            inflation_row, name="inflation", now=now,
            fresh_hours=24 * (400 if inflation_is_annual else 45), stale_hours=24 * (550 if inflation_is_annual else 75),
            confidence=_source_confidence(inflation_row), status="proxy_observation" if inflation_is_annual else "direct_observation",
        )
        liquidity_metric = _metric(
            liquidity_row, name="global_financial_conditions", now=now,
            fresh_hours=24 * 14, stale_hours=24 * 35, confidence=0.75,
            status="global_liquidity_proxy",
        )
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
            "liquidity": 0.10 if liquidity_metric["value"] is not None else 0.0,
        }
        coverage = sum(available_weights.values())
        confidence_inputs = [curve_metric["confidence"]]
        if policy_metric["value"] is not None:
            confidence_inputs.append(policy_metric["confidence"])
        if real_rate is not None:
            confidence_inputs.append(min(policy_metric["confidence"], inflation_metric["confidence"]))
        if liquidity_metric["value"] is not None:
            confidence_inputs.append(liquidity_metric["confidence"])
        confidence = (sum(confidence_inputs) / len(confidence_inputs)) * coverage if confidence_inputs else 0.0
        used_metrics = [curve_metric, policy_metric, inflation_metric, liquidity_metric]
        freshness = aggregate_freshness(metric["freshness_status"] for metric in used_metrics if metric.get("value") is not None)
        missing = [name for name, weight in available_weights.items() if weight == 0.0]
        stale = [name for name, metric in {"yield_curve": curve_metric, "policy_rate": policy_metric, "inflation": inflation_metric, "liquidity": liquidity_metric}.items() if metric.get("value") is not None and metric["freshness_status"] == "stale"]
        proxy_inputs = ["neutral_rate_estimate", "global_financial_conditions_proxy"]
        if curve_is_proxy:
            proxy_inputs.append("policy_curve_proxy")
        if inflation_is_annual:
            proxy_inputs.append("annual_inflation_proxy")
        output.append({
            "component": "AG8_RATES_LIQUIDITY", "schema_version": "AG8_RATES_V2", "method_version": "AG8_RATES_V2",
            "component_snapshot_id": component_id, "currency": currency,
            "observation_time": curve_metric["observation_time"], "publication_time": None,
            "ingestion_time": curve_metric["ingestion_time"] or now.isoformat(), "calculation_time": now.isoformat(),
            **regime, "real_rate": real_rate, "liquidity_score": liquidity_metric["value"],
            "overlays": _ag8_overlays(regime, real_rate), "coverage_ratio": round(coverage, 6),
            "confidence": round(clamp(confidence, 0.0, 1.0), 6), "freshness_status": freshness,
            "missing_inputs": missing, "stale_inputs": stale, "proxy_inputs": proxy_inputs,
            "lineage": {"yield_curve": curve_metric, "policy_rate": policy_metric, "inflation": inflation_metric, "liquidity": liquidity_metric, "neutral_rate": neutral, "change_baseline_as_of": changes["baseline_as_of"], "coverage_weights": {"yield_2y": 0.20, "yield_10y": 0.20, "slope_change": 0.20, "policy": 0.15, "real_rate": 0.15, "liquidity": 0.10}},
            "source": "MACRO_DATA_API",
        })
    return output
