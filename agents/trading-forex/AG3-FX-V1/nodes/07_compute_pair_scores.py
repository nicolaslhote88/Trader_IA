import json
import math
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
universe = ctx.get("universe_fx") or []
technical = ctx.get("technical_signals") or []
macro_news = ctx.get("macro_news") or {}
pair_focus = macro_news.get("pair_focus") or {}
macro_regime = macro_news.get("macro_regime") or {}
macro_data_degraded = bool(ctx.get("macro_data_degraded"))
currency_scores = {str(x.get("currency") or "").upper(): x for x in (ctx.get("currency_scores") or [])}
tech_by_pair = {str(x.get("pair") or "").upper(): x for x in technical}


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


def direct_pair_news_score(pair, label, raw_score):
    pair = str(pair or "").upper()
    base, quote = pair[:3], pair[3:]
    text = str(label or "").strip().lower()
    magnitude = abs(to_float(raw_score, 0.0))
    if magnitude <= 0:
        magnitude = 0.35
    magnitude = min(1.0, magnitude)

    if text in {"buy_base", "long_base", "bullish", "buy", "long"}:
        return magnitude
    if text in {"sell_base", "short_base", "bearish", "sell", "short"}:
        return -magnitude

    if base.lower() in text:
        if "bullish" in text:
            return magnitude
        if "bearish" in text:
            return -magnitude
    if quote.lower() in text:
        if "bullish" in text:
            return -magnitude
        if "bearish" in text:
            return magnitude
    return 0.0


def pair_news(pair):
    f = pair_focus.get(pair) or {}
    label = str(f.get("bias_news") or f.get("bias") or "unknown")
    conf = to_float(f.get("confidence"), 0.0)
    urgent = bool(f.get("urgent_event_within_4h"))
    score = direct_pair_news_score(pair, label, f.get("bias_news_score"))
    drivers = f.get("top_drivers") or []
    if not isinstance(drivers, list):
        drivers = [str(drivers)]
    return label, conf, urgent, score, drivers


def directional_bias(pressure):
    if pressure >= 0.25:
        return "BUY_BASE"
    if pressure <= -0.25:
        return "SELL_BASE"
    return "NEUTRAL"


def invalidators(pair, base, quote, bias):
    if bias == "BUY_BASE":
        return [
            f"{base} dovish repricing or weak growth surprise",
            f"{quote} hawkish repricing or positive inflation surprise",
            f"Fresh AG4-FX urgent event reversing {pair} macro direction",
        ]
    if bias == "SELL_BASE":
        return [
            f"{base} hawkish repricing or positive inflation surprise",
            f"{quote} dovish repricing or weak growth surprise",
            f"Fresh AG4-FX urgent event reversing {pair} macro direction",
        ]
    return [
        "Clear central-bank repricing on either currency",
        "Urgent macro/news event creating directional asymmetry",
        "Large risk-regime shift not reflected in current digest",
    ]


pairs = []
for row in universe:
    pair = str(row.get("pair") or "").upper()
    base = str(row.get("base_ccy") or pair[:3]).upper()
    quote = str(row.get("quote_ccy") or pair[3:]).upper()
    if not pair or len(pair) != 6:
        continue
    base_rec = currency_scores.get(base, {})
    quote_rec = currency_scores.get(quote, {})
    base_score = clamp(base_rec.get("composite_score"))
    quote_score = clamp(quote_rec.get("composite_score"))
    relative_pressure = clamp(base_score - quote_score)
    tech = tech_by_pair.get(pair) or {}
    news_label, news_conf, urgent, direct_news_score, news_drivers = pair_news(pair)
    direct_weight = 0.25 if macro_data_degraded else 0.15
    pair_pressure = clamp((1.0 - direct_weight) * relative_pressure + direct_weight * direct_news_score)
    pair_score = pair_pressure
    bias = directional_bias(pair_pressure)
    confidence = min(
        to_float(base_rec.get("confidence"), 0.25),
        to_float(quote_rec.get("confidence"), 0.25),
    )
    if news_conf:
        confidence = min(0.95, confidence * 0.75 + news_conf * 0.25)
    if urgent:
        confidence = max(confidence, min(0.80, news_conf + 0.10))
    if macro_data_degraded:
        confidence = min(confidence, 0.45)
    data_quality = min(
        to_float(base_rec.get("data_quality_score"), 0.1),
        to_float(quote_rec.get("data_quality_score"), 0.1),
    )
    drivers = []
    for rec in (base_rec, quote_rec):
        text = rec.get("rationale")
        if text:
            drivers.extend([x.strip() for x in str(text).split(";") if x.strip()])
    drivers.extend([str(x) for x in news_drivers if str(x).strip()])
    if not drivers:
        drivers = [f"{base}/{quote} relative fundamental pressure is neutral"]
    pairs.append({
        "score_id": f"{run_id}_{pair}",
        "run_id": run_id,
        "pair": pair,
        "symbol_yf": row.get("symbol_yf") or f"{pair}=X",
        "base_ccy": base,
        "quote_ccy": quote,
        "as_of": as_of,
        "spot": to_float(tech.get("last_close"), None),
        "base_score": base_score,
        "quote_score": quote_score,
        "pair_pressure": pair_pressure,
        "pair_score": pair_score,
        "directional_bias": bias,
        "confidence": confidence,
        "data_quality_score": data_quality,
        "macro_regime": str(macro_regime.get("market_regime") or macro_regime.get("regime") or "Unknown"),
        "news_bias": news_label,
        "news_confidence": news_conf,
        "rationale": json.dumps(drivers[:6], ensure_ascii=False),
        "invalidators": json.dumps(invalidators(pair, base, quote, bias), ensure_ascii=False),
    })

return [{"json": {**ctx, "pair_fundamental_scores": pairs, "pairs_scored": len(pairs)}}]
