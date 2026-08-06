"""Méthodes canoniques AG5–AG8.

Les fonctions de ce module sont pures : aucune lecture réseau et aucune écriture
DuckDB. Une valeur absente reste ``None`` et n'est jamais convertie en signal
neutre. Les scores sont bornés dans [-1, 1].
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

METHOD_VERSIONS = {
    "freshness": "EFFECTIVE_AGE_V1",
    "composite": "AVAILABLE_WEIGHT_RENORM_V1",
    "ag5": "AG5_MACRO_V3",
    "ag6": "AG6_FX_VALUATION_V3",
    "ag7": "AG7_POSITIONING_V2",
    "ag8": "AG8_RATES_V3",
}


def finite_or_none(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _parse_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def effective_age_hours(
    observation_time: Any,
    recorded_age_hours: Any = None,
    *,
    now: Optional[datetime] = None,
) -> Optional[float]:
    """Âge effectif = max(âge enregistré, maintenant - observation réelle)."""

    recorded = finite_or_none(recorded_age_hours)
    observed = _parse_time(observation_time)
    actual = None
    if observed is not None:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        actual = max(0.0, (current.astimezone(timezone.utc) - observed).total_seconds() / 3600.0)
    candidates = [x for x in (recorded, actual) if x is not None]
    return max(candidates) if candidates else None


def freshness_status(age_hours: Any, *, fresh_hours: float, stale_hours: float) -> str:
    age = finite_or_none(age_hours)
    if age is None:
        return "missing"
    if age <= fresh_hours:
        return "fresh"
    if age <= stale_hours:
        return "aging"
    return "stale"


def bounded_weighted_composite(
    components: Mapping[str, Any],
    weights: Mapping[str, float],
    *,
    confidences: Optional[Mapping[str, Any]] = None,
    stale: Optional[set[str]] = None,
    alignment_threshold: float = 0.20,
) -> dict[str, Any]:
    """Composite borné et renormalisé sur les seules composantes valides."""

    stale = stale or set()
    configured_total = sum(max(0.0, float(weights.get(name, 0.0))) for name in weights)
    available: list[tuple[str, float, float]] = []
    missing: list[str] = []
    for name, configured_weight in weights.items():
        value = finite_or_none(components.get(name))
        weight = max(0.0, float(configured_weight))
        if value is None or name in stale or weight <= 0:
            missing.append(name)
            continue
        available.append((name, clamp(value), weight))

    used_total = sum(row[2] for row in available)
    coverage = used_total / configured_total if configured_total > 0 else 0.0
    normalized = {
        name: weight / used_total
        for name, _, weight in available
    } if used_total > 0 else {}
    contributions = {
        name: value * normalized[name]
        for name, value, _ in available
    }
    score = clamp(sum(contributions.values())) if contributions else None

    input_confidences = []
    for name, _, _ in available:
        conf = finite_or_none((confidences or {}).get(name, 1.0))
        input_confidences.append(clamp(conf if conf is not None else 0.0, 0.0, 1.0))
    mean_confidence = (
        sum(input_confidences) / len(input_confidences)
        if input_confidences else 0.0
    )
    # Une couverture partielle ne peut pas conserver une confiance complète.
    confidence = clamp(mean_confidence * coverage, 0.0, 1.0)

    signs = [1 if value > 0 else -1 for _, value, _ in available if abs(value) >= alignment_threshold]
    all_aligned = bool(
        score is not None
        and coverage == 1.0
        and len(signs) == len(weights)
        and len(set(signs)) == 1
        and not stale
    )
    return {
        "score": score,
        "coverage_ratio": round(coverage, 6),
        "confidence": round(confidence, 6),
        "configured_weights": {k: float(v) for k, v in weights.items()},
        "normalized_weights": normalized,
        "contributions": contributions,
        "missing_components": missing,
        "stale_components": sorted(stale),
        "all_aligned": all_aligned,
        "method_version": METHOD_VERSIONS["composite"],
    }


def score_growth(growth_pct: Any, momentum_pct: Any = None) -> Optional[float]:
    growth = finite_or_none(growth_pct)
    momentum = finite_or_none(momentum_pct)
    if growth is None:
        return None
    level = math.tanh(growth / 3.0)
    if momentum is None:
        return clamp(level)
    return clamp(0.70 * level + 0.30 * math.tanh(momentum / 2.0))


def score_inflation_response(
    inflation_pct: Any,
    policy_rate_pct: Any,
    *,
    target_pct: float = 2.0,
) -> Optional[float]:
    """Évalue l'écart à la cible conjointement à la réponse monétaire."""

    inflation = finite_or_none(inflation_pct)
    policy = finite_or_none(policy_rate_pct)
    if inflation is None:
        return None
    gap = inflation - float(target_pct)
    stability = -math.tanh(abs(gap) / 3.0)
    if policy is None:
        return clamp(stability)
    real_policy = policy - inflation
    response = math.tanh(real_policy / 3.0)
    return clamp(0.60 * stability + 0.40 * response)


def score_policy_stance(
    policy_rate_pct: Any,
    neutral_rate_pct: Any,
    *,
    uncertainty_pct: Any = 1.0,
) -> Optional[float]:
    policy = finite_or_none(policy_rate_pct)
    neutral = finite_or_none(neutral_rate_pct)
    uncertainty = finite_or_none(uncertainty_pct)
    if policy is None or neutral is None:
        return None
    scale = max(0.5, uncertainty or 1.0)
    return clamp(math.tanh((policy - neutral) / (2.0 * scale)))


def score_current_account_pct_gdp(value: Any) -> Optional[float]:
    ratio = finite_or_none(value)
    return None if ratio is None else clamp(math.tanh(ratio / 5.0))


def score_fiscal_balance_pct_gdp(value: Any) -> Optional[float]:
    balance = finite_or_none(value)
    return None if balance is None else clamp(math.tanh(balance / 5.0))


def score_labor_momentum(unemployment_change_pp: Any) -> Optional[float]:
    change = finite_or_none(unemployment_change_pp)
    return None if change is None else clamp(-math.tanh(change / 1.5))


def compute_ag5_macro(
    entity_id: str,
    metrics: Mapping[str, Any],
    *,
    neutral_rate: Optional[Mapping[str, Any]] = None,
    weights: Optional[Mapping[str, float]] = None,
) -> dict[str, Any]:
    """Construit le contrat AG5 sans préjugé normatif sur une devise."""

    def value(name: str) -> Any:
        row = metrics.get(name)
        return row.get("value") if isinstance(row, Mapping) else row

    neutral_rate = neutral_rate or {}
    growth = score_growth(value("growth"), value("growth_momentum"))
    inflation = score_inflation_response(value("inflation"), value("policy_rate"))
    policy = score_policy_stance(
        value("policy_rate"),
        neutral_rate.get("rate_pct"),
        uncertainty_pct=neutral_rate.get("uncertainty_pct", 1.0),
    )
    real_rate_value = None
    if finite_or_none(value("policy_rate")) is not None and finite_or_none(value("inflation")) is not None:
        real_rate_value = float(value("policy_rate")) - float(value("inflation"))
    real_rate = None if real_rate_value is None else clamp(math.tanh(real_rate_value / 3.0))
    current_account = score_current_account_pct_gdp(value("current_account_pct_gdp"))
    fiscal = score_fiscal_balance_pct_gdp(value("fiscal_balance_pct_gdp"))
    labor = score_labor_momentum(value("unemployment_change_pp"))

    subscores = {
        "growth": growth,
        "inflation": inflation,
        "monetary_policy": policy,
        "real_rate": real_rate,
        "current_account": current_account,
        "fiscal": fiscal,
        "labor": labor,
    }
    configured_weights = weights or {
        "growth": 0.24,
        "inflation": 0.16,
        "monetary_policy": 0.18,
        "real_rate": 0.12,
        "current_account": 0.16,
        "fiscal": 0.07,
        "labor": 0.07,
    }
    stale = {
        name for name, row in metrics.items()
        if isinstance(row, Mapping) and row.get("freshness_status") == "stale"
    }
    confidences = {
        name: (row.get("confidence", 0.0) if isinstance(row, Mapping) else 0.0)
        for name, row in metrics.items()
    }
    # Les noms des métriques brutes diffèrent parfois des noms de sous-score.
    component_confidence = {
        "growth": confidences.get("growth", 0.0),
        "inflation": min(confidences.get("inflation", 0.0), confidences.get("policy_rate", 1.0)),
        "monetary_policy": min(confidences.get("policy_rate", 0.0), finite_or_none(neutral_rate.get("confidence")) or 0.0),
        "real_rate": min(confidences.get("policy_rate", 0.0), confidences.get("inflation", 0.0)),
        "current_account": confidences.get("current_account_pct_gdp", 0.0),
        "fiscal": confidences.get("fiscal_balance_pct_gdp", 0.0),
        "labor": confidences.get("unemployment_change_pp", 0.0),
    }
    stale_subscores = set()
    if "growth" in stale or "growth_momentum" in stale:
        stale_subscores.add("growth")
    if "inflation" in stale or "policy_rate" in stale:
        stale_subscores.update({"inflation", "real_rate"})
    if "policy_rate" in stale:
        stale_subscores.add("monetary_policy")
    if "current_account_pct_gdp" in stale:
        stale_subscores.add("current_account")
    if "fiscal_balance_pct_gdp" in stale:
        stale_subscores.add("fiscal")
    if "unemployment_change_pp" in stale:
        stale_subscores.add("labor")
    composite = bounded_weighted_composite(
        subscores,
        configured_weights,
        confidences=component_confidence,
        stale=stale_subscores,
    )
    return {
        "component": "AG5_MACRO",
        "schema_version": "AG5_MACRO_V2",
        "entity_type": "currency",
        "entity_id": str(entity_id).upper(),
        "macro_score": composite["score"],
        "subscores": subscores,
        "coverage_ratio": composite["coverage_ratio"],
        "confidence": composite["confidence"],
        "missing_inputs": composite["missing_components"],
        "stale_inputs": composite["stale_components"],
        "weights": composite["normalized_weights"],
        "contributions": composite["contributions"],
        "neutral_rate_method": dict(neutral_rate),
        "real_rate_pct": real_rate_value,
        "method_version": METHOD_VERSIONS["ag5"],
    }


def compute_fx_valuation(
    currency: str,
    *,
    nominal_carry_pct: Any,
    real_carry_pct: Any,
    spot_reference: Any,
    ppp_fair_value: Any = None,
    reer_gap_pct: Any = None,
    terms_of_trade_score: Any = None,
    input_confidence: Optional[Mapping[str, Any]] = None,
    stale_inputs: Optional[set[str]] = None,
) -> dict[str, Any]:
    """Valorisation relative FX ; PPP n'existe que si spot et juste valeur existent."""

    nominal = finite_or_none(nominal_carry_pct)
    real = finite_or_none(real_carry_pct)
    spot = finite_or_none(spot_reference)
    ppp = finite_or_none(ppp_fair_value)
    reer = finite_or_none(reer_gap_pct)
    terms = finite_or_none(terms_of_trade_score)
    carry_score = None if nominal is None else clamp(math.tanh(nominal / 4.0))
    real_carry_score = None if real is None else clamp(math.tanh(real / 4.0))
    ppp_gap = None
    if spot is not None and spot > 0 and ppp is not None and ppp > 0:
        ppp_gap = clamp((ppp - spot) / spot)
    reer_score = None if reer is None else clamp(reer / 25.0)
    terms_score = None if terms is None else clamp(terms)
    components = {
        "carry": carry_score,
        "real_carry": real_carry_score,
        "ppp": ppp_gap,
        "reer": reer_score,
        "terms_of_trade": terms_score,
    }
    weights = {"carry": 0.15, "real_carry": 0.15, "ppp": 0.30, "reer": 0.30, "terms_of_trade": 0.10}
    composite = bounded_weighted_composite(
        components,
        weights,
        confidences=input_confidence or {},
        stale=stale_inputs or set(),
    )
    proxy_inputs = []
    missing_inputs = list(composite["missing_components"])
    return {
        "component": "AG6_FX_VALUATION",
        "schema_version": "AG6_FX_VALUATION_V2",
        "currency": str(currency).upper(),
        "carry_score": carry_score,
        "real_carry_score": real_carry_score,
        "ppp_gap": ppp_gap,
        "reer_gap": reer,
        "terms_of_trade_score": terms_score,
        "valuation_score": composite["score"],
        "spot_reference": spot,
        "ppp_fair_value": ppp,
        "missing_inputs": missing_inputs,
        "stale_inputs": composite["stale_components"],
        "proxy_inputs": proxy_inputs,
        "coverage_ratio": composite["coverage_ratio"],
        "confidence": composite["confidence"],
        "weights": composite["normalized_weights"],
        "contributions": composite["contributions"],
        "method_version": METHOD_VERSIONS["ag6"],
    }


def cot_positioning_score(z_score: Any) -> Optional[float]:
    z = finite_or_none(z_score)
    return None if z is None else clamp(-z / 2.0)


def synthetic_usd_positioning(
    rows_by_currency: Mapping[str, Mapping[str, Any]],
    weights: Mapping[str, float],
) -> Optional[dict[str, Any]]:
    weighted = 0.0
    used = 0.0
    contributors = []
    for currency, configured_weight in weights.items():
        row = rows_by_currency.get(currency) or {}
        z = finite_or_none(row.get("z_score", row.get("net_z_score")))
        if z is None:
            continue
        weight = max(0.0, float(configured_weight))
        weighted += z * weight
        used += weight
        contributors.append(currency)
    if used <= 0:
        return None
    usd_z = -(weighted / used)
    return {
        "entity_id": "USD",
        "z_score": round(usd_z, 6),
        "positioning_score": cot_positioning_score(usd_z),
        "is_proxy": True,
        "source": "CFTC_SYNTHETIC_USD_BASKET",
        "contributors": contributors,
        "weights": {k: float(v) / used for k, v in weights.items() if k in contributors},
        "confidence": min(0.60, used),
    }


def rates_regime(
    *,
    yield_2y: Any,
    yield_10y: Any,
    slope_change: Any,
    policy_rate: Any = None,
    neutral_rate: Any = None,
    yield_2y_change: Any = None,
    yield_10y_change: Any = None,
) -> dict[str, Any]:
    y2 = finite_or_none(yield_2y)
    y10 = finite_or_none(yield_10y)
    change = finite_or_none(slope_change)
    slope = None if y2 is None or y10 is None else y10 - y2
    y2_change = finite_or_none(yield_2y_change)
    y10_change = finite_or_none(yield_10y_change)
    if slope is None:
        curve = "unknown"
    elif change is not None and change >= 0.10:
        if y2_change is None or y10_change is None:
            curve = "unknown"
        else:
            curve = "bull_steepening" if (y2_change + y10_change) / 2.0 < 0 else "bear_steepening"
    elif change is not None and change <= -0.10:
        curve = "flattening"
    elif slope < 0:
        curve = "inverted"
    else:
        curve = "normal"

    policy = finite_or_none(policy_rate)
    neutral = finite_or_none(neutral_rate)
    if policy is None or neutral is None:
        policy_regime = "unknown"
    elif policy - neutral > 1.0:
        policy_regime = "restrictive"
    elif policy - neutral > 0:
        policy_regime = "tightening"
    elif policy - neutral < -1.0:
        policy_regime = "accommodative"
    else:
        policy_regime = "easing"

    duration_pressure = None
    if y10 is not None and neutral is not None:
        duration_pressure = clamp(math.tanh((y10 - neutral) / 3.0))
    return {
        "policy_regime": policy_regime,
        "curve_regime": curve,
        "yield_2y": y2,
        "yield_10y": y10,
        "slope_10y2y": slope,
        "slope_change": change,
        "yield_2y_change": y2_change,
        "yield_10y_change": y10_change,
        "duration_pressure": duration_pressure,
        "method_version": METHOD_VERSIONS["ag8"],
    }


# Compatibilité de lecture pour les anciens imports. Ces wrappers ne doivent
# plus être utilisés comme source de vérité par les workflows AG5–AG8.
def score_gdp_growth(growth_qoq: Any, momentum: Any) -> Optional[float]:
    return score_growth(growth_qoq, momentum)


def score_inflation(cpi_yoy: Any, policy_rate: Any) -> Optional[float]:
    return score_inflation_response(cpi_yoy, policy_rate)


def score_current_account(balance_pct_gdp: Any) -> Optional[float]:
    return score_current_account_pct_gdp(balance_pct_gdp)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
