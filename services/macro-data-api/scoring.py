"""
Calcul des scores des 3 piliers pour le framework Global Macro.

Pilier 1 – Macro/Flows : growth, inflation, CB policy, current account
Pilier 2 – Valorisation : carry (rate differential), PPP deviation
Pilier 3 – Positionnement : COT z-score (inversé → hated = bullish)
"""

import logging
import math
import json
from datetime import date
from typing import Optional

logger = logging.getLogger("scoring")

# Cible CB pour l'inflation (2%)
CB_INFLATION_TARGET = 2.0

# Taux "neutre" estimé pour la politique monétaire (approx NAIRU-based)
NEUTRAL_POLICY_RATE = {
    "USD": 2.5,
    "EUR": 2.0,
    "JPY": 0.0,
    "GBP": 2.5,
    "CHF": 1.0,
    "CAD": 2.5,
    "AUD": 3.0,
    "NZD": 3.0,
    "MXN": 6.0,
    "SEK": 2.0,
    "NOK": 2.5,
    "KRW": 2.5,
}

CORE_G8 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
EXTENDED_SCORING_CURRENCIES = ["MXN", "SEK", "NOK"]
MACRO_ONLY_CURRENCIES = ["KRW"]
SCORING_CURRENCIES = CORE_G8 + EXTENDED_SCORING_CURRENCIES

# Backward-compatible alias used by existing tests and comments.
G10 = CORE_G8

CONFIDENCE_RANK = {"missing": 0, "low": 1, "medium": 2, "high": 3}
RANK_CONFIDENCE = {v: k for k, v in CONFIDENCE_RANK.items()}

USD_SYNTHETIC_COT_WEIGHTS = {
    "EUR": 0.30,
    "JPY": 0.18,
    "GBP": 0.12,
    "CAD": 0.10,
    "AUD": 0.08,
    "CHF": 0.08,
    "NZD": 0.04,
    "MXN": 0.10,
}

LOW_CONFIDENCE_POSITIONING_SOURCES = {"RATE_CARRY_PROXY"}


def clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _has_number(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _confidence_floor(*values: str) -> str:
    ranks = [CONFIDENCE_RANK.get(str(v or "missing").lower(), 0) for v in values]
    return RANK_CONFIDENCE.get(min(ranks) if ranks else 0, "missing")


def _is_complete_enough(confidence: str) -> bool:
    return CONFIDENCE_RANK.get(str(confidence or "missing").lower(), 0) >= CONFIDENCE_RANK["medium"]


def _positioning_score_from_z(z_score: float) -> float:
    # Contrarian convention: hated/short positioning is bullish, crowded long is bearish.
    return clamp(-float(z_score) / 2.0)


def _enrich_positioning_with_proxies(
    cot_by_ccy: dict[str, dict],
    carry_scores: dict[str, float],
    policy_rates_by_ccy: dict[str, float],
) -> dict[str, dict]:
    """
    Fill useful-but-labelled positioning gaps.

    1. USD has no direct COT currency contract. It is inferred as the inverse
       of a liquid COT basket.
    2. SEK/NOK can keep the cube usable through a low-confidence carry proxy
       until a real option-RR/CME-OI feed is wired.
    """
    enriched = dict(cot_by_ccy)

    if "USD" not in enriched:
        weighted_z = 0.0
        used_weight = 0.0
        contributors = []
        for currency, weight in USD_SYNTHETIC_COT_WEIGHTS.items():
            row = enriched.get(currency)
            z = row.get("net_z_score") if row else None
            if _has_number(z):
                weighted_z += float(z) * weight
                used_weight += weight
                contributors.append(currency)
        if used_weight > 0:
            usd_z = -(weighted_z / used_weight)
            enriched["USD"] = {
                "currency": "USD",
                "net_z_score": round(usd_z, 3),
                "positioning_score": round(_positioning_score_from_z(usd_z), 3),
                "crowded_flag": abs(usd_z) >= 1.5,
                "crowded_direction": "long" if usd_z >= 1.5 else "short" if usd_z <= -1.5 else "neutral",
                "source": "CFTC_COT_SYNTHETIC_USD_BASKET",
                "confidence": "medium",
                "proxy_contributors": contributors,
            }

    for currency in ("SEK", "NOK"):
        if currency in enriched:
            continue
        carry = carry_scores.get(currency)
        policy = policy_rates_by_ccy.get(currency)
        if _has_number(carry) and _has_number(policy):
            # High positive carry tends to attract crowded longs; this is not a
            # true positioning feed, so keep it low-confidence and size-aware.
            positioning_score = clamp(-float(carry) * 0.50)
            z_score = -2.0 * positioning_score
            enriched[currency] = {
                "currency": currency,
                "net_z_score": round(z_score, 3),
                "positioning_score": round(positioning_score, 3),
                "crowded_flag": abs(z_score) >= 1.5,
                "crowded_direction": "long" if z_score >= 1.5 else "short" if z_score <= -1.5 else "neutral",
                "source": "RATE_CARRY_PROXY",
                "confidence": "low",
            }

    return enriched


def score_gdp_growth(growth_qoq: Optional[float], momentum: Optional[float]) -> float:
    """
    Score de croissance PIB.
    QoQ > 3% → très bon, > 1.5% → bon, 0-1.5% → neutre, < 0 → mauvais.
    Momentum = variation de la croissance (accélération/décélération).
    """
    if growth_qoq is None:
        return 0.0
    base_score = 0.0
    if growth_qoq > 3.0:
        base_score = 1.0
    elif growth_qoq > 1.5:
        base_score = 0.5
    elif growth_qoq > 0:
        base_score = 0.1
    elif growth_qoq > -1.0:
        base_score = -0.3
    else:
        base_score = -0.8
    # Bonus/malus momentum
    mom_bonus = clamp((momentum or 0.0) * 0.15, -0.3, 0.3)
    return clamp(base_score + mom_bonus)


def score_inflation(cpi_yoy: Optional[float], policy_rate: Optional[float]) -> float:
    """
    Score inflation vis-à-vis de la cible CB (2%).
    L'inflation contrôlée (proche cible) = hawkish → positif pour la devise.
    Inflation hors de contrôle (>> 2%) = risque = négatif.
    Déflation = négatif.
    """
    if cpi_yoy is None:
        return 0.0
    deviation = cpi_yoy - CB_INFLATION_TARGET
    # CB hawkish efficacement : inflation proche de 2% ET taux élevé
    if abs(deviation) < 0.5:
        return 0.3  # maîtrisée, neutre-positif
    if deviation > 4.0:
        return -0.8  # hyperinflation, très négatif
    if deviation > 2.0:
        return -0.4  # inflation élevée
    if deviation > 0.5:
        return -0.1  # légèrement au-dessus cible
    if deviation < -1.5:
        return -0.6  # déflation (like Japon)
    return -0.2  # légèrement en dessous cible


def score_cb_policy(policy_rate: Optional[float], currency: str, cpi_yoy: Optional[float]) -> float:
    """
    Score de politique monétaire.
    Taux directeur au-dessus du neutre = hawkish = attractif pour les capitaux.
    Taux proche de zéro ou négatif = dovish = répulsif.
    Différentiel vs. taux neutre estimé.
    """
    if policy_rate is None:
        return 0.0
    neutral = NEUTRAL_POLICY_RATE.get(currency, 2.0)
    diff = policy_rate - neutral
    # Score basé sur l'écart au taux neutre
    if diff > 2.0:
        return 0.8  # très au-dessus du neutre → très attractif
    if diff > 1.0:
        return 0.5
    if diff > 0.0:
        return 0.2
    if diff > -1.0:
        return -0.2
    if diff > -2.0:
        return -0.6
    return -1.0  # taux très bas ou négatifs


def score_current_account(balance_bn_usd: Optional[float]) -> float:
    """
    Score de la balance du compte courant.
    Excédent → flux entrant de capitaux → bullish devise.
    Déficit → flux sortant → bearish.
    Note : score plus dur pour les très gros déficits (>50 Mds USD/T).
    """
    if balance_bn_usd is None:
        return 0.0
    if balance_bn_usd > 50:
        return 1.0   # gros excédent (Japon, Allemagne)
    if balance_bn_usd > 20:
        return 0.6
    if balance_bn_usd > 0:
        return 0.2
    if balance_bn_usd > -30:
        return -0.3
    if balance_bn_usd > -100:
        return -0.6
    return -1.0  # énorme déficit (US, UK)


def compute_macro_score(indicators: list[dict], policy_rates: list[dict]) -> dict[str, dict]:
    """
    Score Pilier 1 (Macro/Flows) par devise.
    Pondérations : growth 0.30, CB policy 0.30, current_account 0.25, inflation 0.15
    """
    # Indexer les indicateurs par devise + type
    ind_by_ccy: dict[str, dict[str, float]] = {}
    for row in indicators:
        ccy = row.get("currency", "")
        ind = row.get("indicator", "")
        val = row.get("value")
        if ccy and ind and val is not None:
            if ccy not in ind_by_ccy:
                ind_by_ccy[ccy] = {}
            ind_by_ccy[ccy][ind] = float(val)

    # Taux directeurs
    rates_by_ccy = {r["currency"]: r.get("rate_pct") for r in policy_rates}

    result = {}
    for ccy in SCORING_CURRENCIES:
        inds = ind_by_ccy.get(ccy, {})
        policy = rates_by_ccy.get(ccy)

        gdp_qoq = inds.get("gdp_growth_qoq")
        gdp_mom = inds.get("gdp_momentum", 0.0)
        cpi = inds.get("cpi_yoy")
        ca = inds.get("current_account_bn_usd")

        s_growth = score_gdp_growth(gdp_qoq, gdp_mom)
        s_inflation = score_inflation(cpi, policy)
        s_policy = score_cb_policy(policy, ccy, cpi)
        s_ca = score_current_account(ca)

        macro_score = clamp(
            s_growth * 0.30 + s_policy * 0.30 + s_ca * 0.25 + s_inflation * 0.15
        )

        result[ccy] = {
            "macro_growth_score": round(s_growth, 3),
            "macro_inflation_score": round(s_inflation, 3),
            "macro_policy_score": round(s_policy, 3),
            "macro_ca_score": round(s_ca, 3),
            "macro_score": round(macro_score, 3),
        }
    return result


def compute_carry_score(policy_rates: list[dict]) -> dict[str, float]:
    """
    Score carry par devise = taux directeur normalisé vs. moyenne G10.
    Devise avec taux élevé = positif (attire les capitaux carry trade).
    """
    all_rates = {
        r["currency"]: r.get("rate_pct", 0.0) or 0.0
        for r in policy_rates
        if r.get("currency") in SCORING_CURRENCIES
    }
    baseline_rates = {ccy: all_rates[ccy] for ccy in CORE_G8 if ccy in all_rates}
    if not baseline_rates:
        return {}
    avg = sum(baseline_rates.values()) / len(baseline_rates)
    std = max(math.sqrt(sum((v - avg) ** 2 for v in baseline_rates.values()) / len(baseline_rates)), 0.5)
    return {ccy: clamp((rate - avg) / std / 2) for ccy, rate in all_rates.items()}


def compute_ppp_deviation(cpi_data: list[dict]) -> dict[str, float]:
    """
    Approximation PPP simplifiée : écart d'inflation cumulé vs. USD sur 5 ans.
    Si l'inflation d'un pays est systématiquement plus haute que celle des US,
    sa devise devrait se déprécier (PPP → surévaluée si elle ne l'a pas fait).
    Retourne l'écart de surévaluation relatif : positif = sous-évalué, négatif = surévalué.
    """
    cpi_by_ccy: dict[str, list] = {}
    for row in cpi_data:
        ccy = row.get("currency", "")
        if ccy in SCORING_CURRENCIES and row.get("value") is not None:
            if ccy not in cpi_by_ccy:
                cpi_by_ccy[ccy] = []
            cpi_by_ccy[ccy].append(float(row["value"]))
    if "USD" not in cpi_by_ccy:
        return {}
    usd_avg = sum(cpi_by_ccy["USD"]) / len(cpi_by_ccy["USD"]) if cpi_by_ccy["USD"] else 0.0
    result = {}
    for ccy, vals in cpi_by_ccy.items():
        if ccy == "USD":
            continue
        if not vals:
            continue
        avg = sum(vals) / len(vals)
        # Si inflation plus haute que USD → devise devrait se déprécier → si ça n'a pas eu lieu elle est surévaluée
        # ppp_deviation positif = sous-évalué (inflation plus basse que USD → devrait s'apprécier)
        ppp_dev = clamp((usd_avg - avg) / max(usd_avg, 0.01))
        result[ccy] = round(ppp_dev, 3)
    return result


def compute_valuation_scores(policy_rates: list[dict], cpi_history: list[dict]) -> dict[str, dict]:
    """
    Score Pilier 2 (Valorisation) par devise.
    Pondérations : carry 0.50, PPP 0.30, placeholder REER 0.20 (carry par défaut)
    """
    carry = compute_carry_score(policy_rates)
    ppp = compute_ppp_deviation(cpi_history)

    result = {}
    for ccy in SCORING_CURRENCIES:
        c = carry.get(ccy, 0.0)
        p = ppp.get(ccy, 0.0)
        # Sans REER, on double la pondération du carry
        valuation = clamp(c * 0.60 + p * 0.40)
        result[ccy] = {
            "carry_score": round(c, 3),
            "ppp_deviation": round(p, 3),
            "valuation_score": round(valuation, 3),
        }
    return result


def compute_input_confidence(
    currency: str,
    indicators_by_ccy: dict[str, dict[str, float]],
    policy_rates_by_ccy: dict[str, float],
    carry_scores: dict[str, float],
    ppp_scores: dict[str, float],
    cot_by_ccy: dict[str, dict],
    yield_by_ccy: dict[str, dict],
) -> dict:
    """Return completeness metadata without changing the legacy score formulas."""
    inds = indicators_by_ccy.get(currency, {})
    has_policy = _has_number(policy_rates_by_ccy.get(currency))
    has_cpi = _has_number(inds.get("cpi_yoy"))
    has_gdp = _has_number(inds.get("gdp_growth_qoq"))
    has_ca = _has_number(inds.get("current_account_bn_usd"))
    has_unemployment = _has_number(inds.get("unemployment_pct"))

    macro_inputs = [has_policy, has_cpi, has_gdp]
    if has_ca or has_unemployment:
        macro_conf = "high" if all(macro_inputs) else "medium"
    elif all(macro_inputs):
        macro_conf = "medium"
    else:
        macro_conf = "low" if any(macro_inputs) else "missing"

    has_carry = _has_number(carry_scores.get(currency))
    has_ppp = currency == "USD" or _has_number(ppp_scores.get(currency))
    if has_carry and has_ppp:
        valuation_conf = "high"
    elif has_carry:
        valuation_conf = "medium"
    else:
        valuation_conf = "missing"

    cot = cot_by_ccy.get(currency, {})
    positioning_source = str(cot.get("source") or "").upper()
    if cot:
        positioning_conf = str(cot.get("confidence") or "high").lower()
    else:
        positioning_conf = "missing"

    curve = yield_by_ccy.get(currency, {})
    has_y2 = _has_number(curve.get("yield_2y_pct") or curve.get("yield_2y"))
    has_y10 = _has_number(curve.get("yield_10y_pct") or curve.get("yield_10y"))
    curve_source = str(curve.get("source") or "").lower()
    if has_y2 and has_y10:
        rates_conf = "medium" if "proxy" in curve_source or "manual" in curve_source else "high"
    else:
        rates_conf = "missing"

    floor = _confidence_floor(macro_conf, valuation_conf, positioning_conf, rates_conf)
    missing = []
    if not _is_complete_enough(macro_conf):
        missing.append("macro")
    if not _is_complete_enough(valuation_conf):
        missing.append("valuation")
    if not _is_complete_enough(positioning_conf):
        missing.append("positioning")
    if not _is_complete_enough(rates_conf):
        missing.append("yield_curve")

    proxy_usable = (
        floor == "low"
        and positioning_conf == "low"
        and positioning_source in LOW_CONFIDENCE_POSITIONING_SOURCES
        and _is_complete_enough(macro_conf)
        and _is_complete_enough(valuation_conf)
        and _is_complete_enough(rates_conf)
    )
    if proxy_usable:
        missing = [m for m in missing if m != "positioning"]
        missing.append("positioning_low_confidence")

    is_complete = _is_complete_enough(floor)

    return {
        "macro_confidence": macro_conf,
        "valuation_confidence": valuation_conf,
        "positioning_confidence": positioning_conf,
        "rates_confidence": rates_conf,
        "confidence_floor": floor,
        "data_completeness": "complete" if is_complete else "proxy_complete" if proxy_usable else "data_incomplete",
        "score_status": "scored" if is_complete else "scored_proxy" if proxy_usable else "data_incomplete",
        "missing_inputs": missing,
    }


def compute_all_pillar_scores(db) -> list[dict]:
    """
    Calcule les scores des 3 piliers pour toutes les devises G10.
    Retourne la liste des scores prêts à être insérés dans DuckDB.
    """
    today = date.today().isoformat()

    # Données depuis DB
    indicators = db.get_indicators()
    policy_rates = db.get_latest_policy_rates()
    cot_latest = db.get_latest_cot()
    cpi_history = db.get_indicators(indicator="cpi_yoy")
    yield_curves = db.get_latest_yield_curve()

    ind_by_ccy: dict[str, dict[str, float]] = {}
    for row in indicators:
        ccy = row.get("currency", "")
        ind = row.get("indicator", "")
        val = row.get("value")
        if ccy and ind and val is not None:
            ind_by_ccy.setdefault(ccy, {})[ind] = float(val)
    policy_by_ccy = {r["currency"]: r.get("rate_pct") for r in policy_rates}

    # Calcul des 3 piliers
    macro_scores = compute_macro_score(indicators, policy_rates)
    valuation_scores = compute_valuation_scores(policy_rates, cpi_history)
    carry_scores = compute_carry_score(policy_rates)
    ppp_scores = compute_ppp_deviation(cpi_history)

    # COT positioning scores
    cot_by_ccy = {r["currency"]: r for r in cot_latest}
    yield_by_ccy = {r["currency"]: r for r in yield_curves}
    cot_by_ccy = _enrich_positioning_with_proxies(cot_by_ccy, carry_scores, policy_by_ccy)

    results = []
    for ccy in SCORING_CURRENCIES:
        m = macro_scores.get(ccy, {})
        v = valuation_scores.get(ccy, {})
        c = cot_by_ccy.get(ccy, {})

        macro_s = m.get("macro_score", 0.0)
        valuation_s = v.get("valuation_score", 0.0)
        positioning_s = c.get("positioning_score", 0.0)
        cot_z = c.get("net_z_score", 0.0)
        crowded = c.get("crowded_flag", False)
        completeness = compute_input_confidence(
            ccy,
            ind_by_ccy,
            policy_by_ccy,
            carry_scores,
            ppp_scores,
            cot_by_ccy,
            yield_by_ccy,
        )
        can_score = completeness["score_status"] in ("scored", "scored_proxy") or ccy in CORE_G8

        # Composite (pondération égale des 3 piliers)
        composite = clamp((macro_s + valuation_s + positioning_s) / 3.0)

        # Alignement : les 3 piliers doivent pointer dans la même direction
        # avec un seuil minimum de 0.20 pour éviter le bruit
        THRESHOLD = 0.20
        all_aligned = can_score and (
            abs(macro_s) >= THRESHOLD and
            abs(valuation_s) >= THRESHOLD and
            abs(positioning_s) >= THRESHOLD and
            (macro_s > 0) == (valuation_s > 0) == (positioning_s > 0)
        )

        results.append({
            "as_of": today,
            "currency": ccy,
            **m,
            **v,
            "cot_z_score": round(cot_z, 3) if cot_z else None,
            "positioning_score": round(positioning_s, 3),
            "crowded_flag": crowded,
            "composite_score": round(composite, 3) if can_score else None,
            "all_pillars_aligned": all_aligned,
            "data_completeness": completeness["data_completeness"],
            "score_status": "scored_legacy" if completeness["score_status"] == "data_incomplete" and ccy in CORE_G8 else completeness["score_status"],
            "confidence_floor": completeness["confidence_floor"],
            "macro_confidence": completeness["macro_confidence"],
            "valuation_confidence": completeness["valuation_confidence"],
            "positioning_confidence": completeness["positioning_confidence"],
            "rates_confidence": completeness["rates_confidence"],
            "missing_inputs": json.dumps(completeness["missing_inputs"]),
        })

    return results
