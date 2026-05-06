import json
import math
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
currencies = ctx.get("currencies") or ["USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD"]
macro_news = ctx.get("macro_news") or {}
macro_regime = macro_news.get("macro_regime") or {}
pair_focus = macro_news.get("pair_focus") or {}
observations = ctx.get("macro_observations") or []
macro_data_degraded = bool(ctx.get("macro_data_degraded"))

WEIGHTS = {
    "monetary_score": 0.30,
    "real_yield_score": 0.20,
    "inflation_score": 0.10,
    "growth_score": 0.15,
    "labor_score": 0.05,
    "external_balance_score": 0.10,
    "risk_regime_score": 0.10,
}
FACTOR_TO_COMPONENT = {
    "policy_rate": "monetary_score",
    "central_bank_bias": "monetary_score",
    "real_yield": "real_yield_score",
    "inflation": "inflation_score",
    "growth": "growth_score",
    "labor": "labor_score",
    "external_balance": "external_balance_score",
}
SAFE_HAVENS = {"USD", "JPY", "CHF"}
CYCLICALS = {"AUD", "NZD", "CAD"}


def clamp(v, lo=-1.0, hi=1.0):
    try:
        n = float(v)
        if not math.isfinite(n):
            return 0.0
        return max(lo, min(hi, n))
    except Exception:
        return 0.0


def to_float(v, default=0.0):
    try:
        n = float(v)
        return n if math.isfinite(n) else default
    except Exception:
        return default


def avg(values):
    clean = [to_float(x) for x in values if x is not None]
    return clamp(sum(clean) / len(clean)) if clean else 0.0


def parse_bias_value(value):
    if isinstance(value, (int, float)):
        return clamp(value)
    if isinstance(value, dict):
        for key in ("score", "bias_score", "value"):
            if key in value:
                return clamp(value.get(key))
        value = value.get("bias") or value.get("direction") or value.get("label") or ""
    text = str(value or "").lower()
    if any(x in text for x in ("strong_bull", "very bullish", "bullish")):
        return 0.45
    if any(x in text for x in ("strong_bear", "very bearish", "bearish")):
        return -0.45
    return 0.0


def macro_biases():
    raw = macro_regime.get("biases") or macro_regime.get("currency_biases") or {}
    out = {ccy: 0.0 for ccy in currencies}
    if isinstance(raw, dict):
        for ccy, value in raw.items():
            key = str(ccy or "").upper()
            if key in out:
                out[key] = parse_bias_value(value)
    return out


def pair_focus_votes():
    votes = {ccy: [] for ccy in currencies}
    for pair, focus in (pair_focus or {}).items():
        pair = str(pair or "").upper()
        if len(pair) != 6 or not isinstance(focus, dict):
            continue
        base, quote = pair[:3], pair[3:]
        confidence = max(0.1, min(1.0, to_float(focus.get("confidence"), 0.35)))
        score = to_float(focus.get("bias_news_score"), 0.0)
        label = str(focus.get("bias_news") or focus.get("bias") or "").upper()
        if not score:
            if label in {"BUY_BASE", "BULLISH", "LONG_BASE"}:
                score = 0.35
            elif label in {"SELL_BASE", "BEARISH", "SHORT_BASE"}:
                score = -0.35
        score = clamp(score) * confidence
        if base in votes:
            votes[base].append(score)
        if quote in votes:
            votes[quote].append(-score)
    return {ccy: avg(vals) for ccy, vals in votes.items()}


def regime_score(ccy):
    label = str(macro_regime.get("market_regime") or macro_regime.get("regime") or "").lower()
    if "risk-off" in label or "risk_off" in label or "stress" in label:
        if ccy in SAFE_HAVENS:
            return 0.35
        if ccy in CYCLICALS:
            return -0.30
        return -0.08
    if "risk-on" in label or "risk_on" in label:
        if ccy in SAFE_HAVENS:
            return -0.25
        if ccy in CYCLICALS:
            return 0.30
        return 0.08
    return 0.0


grouped = {ccy: {} for ccy in currencies}
missing = {ccy: set(FACTOR_TO_COMPONENT.keys()) for ccy in currencies}
stale = {ccy: set() for ccy in currencies}

for obs in observations:
    ccy = str(obs.get("currency") or "").upper()
    factor = str(obs.get("factor") or "")
    component = FACTOR_TO_COMPONENT.get(factor)
    if ccy not in grouped or not component:
        continue
    status = str(obs.get("data_status") or "").upper()
    if status == "OK":
        grouped[ccy].setdefault(component, []).append(obs.get("normalized_score"))
        missing[ccy].discard(factor)
    elif status in {"STALE", "ERROR"}:
        stale[ccy].add(factor)

regime_bias = macro_biases()
news_votes = pair_focus_votes()
scores = []

for ccy in currencies:
    components = {
        "monetary_score": avg(grouped[ccy].get("monetary_score", [])),
        "real_yield_score": avg(grouped[ccy].get("real_yield_score", [])),
        "inflation_score": avg(grouped[ccy].get("inflation_score", [])),
        "growth_score": avg(grouped[ccy].get("growth_score", [])),
        "labor_score": avg(grouped[ccy].get("labor_score", [])),
        "external_balance_score": avg(grouped[ccy].get("external_balance_score", [])),
        "risk_regime_score": clamp(0.55 * regime_score(ccy) + 0.25 * regime_bias.get(ccy, 0.0) + 0.20 * news_votes.get(ccy, 0.0)),
    }
    news_bias_score = clamp(0.60 * news_votes.get(ccy, 0.0) + 0.40 * regime_bias.get(ccy, 0.0))
    composite = clamp(sum(components[k] * w for k, w in WEIGHTS.items()))
    ok_components = sum(1 for k in components if abs(components[k]) > 0.00001 and k != "risk_regime_score")
    mapped_components = 6 - len(missing[ccy])
    data_quality = max(0.10, min(1.0, (mapped_components / 6.0) * 0.75 + 0.25 * (1.0 if macro_news else 0.0)))
    if macro_data_degraded:
        data_quality = min(data_quality, 0.55)
    confidence = max(0.20, min(0.90, 0.30 + 0.45 * data_quality + 0.15 * abs(composite) + 0.10 * min(1.0, ok_components / 6.0)))
    if macro_data_degraded:
        confidence = min(confidence, 0.45)
    rationale_bits = []
    if components["risk_regime_score"] > 0.10:
        rationale_bits.append(f"{ccy} supported by current risk/news regime")
    elif components["risk_regime_score"] < -0.10:
        rationale_bits.append(f"{ccy} pressured by current risk/news regime")
    if abs(news_bias_score) > 0.10:
        rationale_bits.append(f"{ccy} news proxy {'bullish' if news_bias_score > 0 else 'bearish'}")
    if not rationale_bits:
        rationale_bits.append(f"{ccy} fundamental inputs mostly neutral or unavailable")

    scores.append({
        "score_id": f"{run_id}_{ccy}",
        "run_id": run_id,
        "currency": ccy,
        "as_of": as_of,
        **components,
        "news_bias_score": news_bias_score,
        "composite_score": composite,
        "confidence": confidence,
        "data_quality_score": data_quality,
        "missing_factors": json.dumps(sorted(missing[ccy])),
        "stale_factors": json.dumps(sorted(stale[ccy])),
        "rationale": "; ".join(rationale_bits),
    })

return [{"json": {**ctx, "currency_scores": scores, "currencies_scored": len(scores)}}]
