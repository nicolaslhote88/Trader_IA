import json
import math
from collections import defaultdict
from datetime import datetime, timezone

CURRENCIES = ("USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD")


def split_csv(v):
    return [x.strip().upper() for x in str(v or "").replace(";", ",").split(",") if x.strip()]


def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def parse_dt(value):
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def age_hours(row, as_of_dt):
    published = parse_dt(row.get("published_at"))
    if not published or not as_of_dt:
        return None
    return (as_of_dt - published).total_seconds() / 3600.0


def event_weight(row):
    mag = str(row.get("impact_magnitude") or "").strip().lower()
    mag_weight = {"high": 3.0, "medium": 2.0, "low": 1.0}.get(mag, 1.0)
    score = safe_float(row.get("impact_score"), 0.0)
    score_weight = 1.0 + min(max(score, 0.0), 10.0) / 20.0
    confidence = safe_float(row.get("confidence"), 0.5)
    confidence_weight = 0.75 + min(max(confidence, 0.0), 1.0) * 0.5
    urgency = safe_float(row.get("urgency"), None)
    if urgency is None:
        urgency_label = str(row.get("urgency") or "").strip().lower()
        urgency = {"immediate": 1.0, "today": 0.75, "this_week": 0.45, "low": 0.2}.get(urgency_label, 0.2)
    urgency_weight = 0.85 + min(max(urgency, 0.0), 1.0) * 0.3
    return mag_weight * score_weight * confidence_weight * urgency_weight


def recency_weight(row, as_of_dt):
    age = age_hours(row, as_of_dt)
    if age is None:
        return 0.70
    if age < 0:
        return 0.50
    if age <= 6:
        return 1.0
    if age <= 24:
        return 0.80
    if age <= 48:
        return 0.55
    if age <= 96:
        return 0.35
    return 0.15


def bias_for_pair(pair, bull, bear, hint=""):
    base, quote = pair[:3], pair[3:]
    if base in bull or quote in bear:
        return f"bullish_{base.lower()}"
    if base in bear or quote in bull:
        return f"bearish_{base.lower()}"

    normalized_hint = str(hint or "").upper().replace("/", "")
    if pair in normalized_hint:
        long_terms = ("LONG", "BUY", "BULL", "UP", "HAUSSE")
        short_terms = ("SHORT", "SELL", "BEAR", "DOWN", "BAISSE")
        if any(term in normalized_hint for term in long_terms):
            return f"bullish_{base.lower()}"
        if any(term in normalized_hint for term in short_terms):
            return f"bearish_{base.lower()}"
    return "mixed"


def add_currency_votes(score_map, bull, bear, weight):
    for cur in bull:
        if cur in CURRENCIES:
            score_map[cur] += weight
    for cur in bear:
        if cur in CURRENCIES:
            score_map[cur] -= weight


def normalize_currency_biases(score_map):
    max_abs = max([abs(score_map[c]) for c in CURRENCIES] + [1.0])
    return {cur: round(score_map[cur] / max_abs, 3) for cur in CURRENCIES}


def parse_bias_json(raw):
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def macro_from_news(news, as_of):
    regime_scores = defaultdict(float)
    currency_scores = defaultdict(float)
    drivers = []
    total_weight = 0.0

    for row in news:
        weight = event_weight(row)
        total_weight += weight
        regime = str(row.get("regime") or row.get("market_regime") or "").strip()
        if regime:
            regime_scores[regime] += weight
        add_currency_votes(currency_scores, split_csv(row.get("currencies_bullish")), split_csv(row.get("currencies_bearish")), weight)
        if len(drivers) < 5 and row.get("title"):
            drivers.append(str(row.get("title")))

    if regime_scores:
        market_regime, regime_weight = max(regime_scores.items(), key=lambda kv: kv[1])
        regime_share = regime_weight / max(sum(regime_scores.values()), 1.0)
    else:
        risk_on = currency_scores["AUD"] + currency_scores["NZD"] + currency_scores["CAD"]
        risk_off = currency_scores["JPY"] + currency_scores["CHF"] + currency_scores["USD"]
        if risk_on - risk_off > 1.5:
            market_regime, regime_share = "Risk-On", 0.55
        elif risk_off - risk_on > 1.5:
            market_regime, regime_share = "Risk-Off", 0.55
        else:
            market_regime, regime_share = "Neutral", 0.45

    confidence = 0.0 if not news else min(0.9, 0.35 + min(len(news), 30) * 0.01 + regime_share * 0.35)
    return {
        "market_regime": market_regime or "Neutral",
        "drivers": " | ".join(drivers),
        "confidence": round(confidence, 3),
        "biases": normalize_currency_biases(currency_scores),
        "as_of": as_of,
        "source": "computed_from_news",
        "total_weight": round(total_weight, 3),
    }


ctx = (_items or [{"json": {}}])[0].get("json", {})
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
as_of_dt = parse_dt(as_of) or datetime.now(timezone.utc)
news = ctx.get("deduped_news") or []

top_items = []
currency_scores = defaultdict(float)
pair_focus = defaultdict(
    lambda: {
        "news_count_24h": 0,
        "bias_scores": defaultdict(float),
        "top_drivers": [],
        "urgent_event_within_4h": False,
        "evidence_weight": 0.0,
    }
)

for n in news:
    pairs = split_csv(n.get("impact_fx_pairs"))
    bull = split_csv(n.get("currencies_bullish"))
    bear = split_csv(n.get("currencies_bearish"))
    age = age_hours(n, as_of_dt)
    recent_24h = age is None or (0 <= age <= 24)
    urgent_4h = age is not None and 0 <= age <= 4
    weight = event_weight(n) * recency_weight(n, as_of_dt)
    pair_weight = weight / max(math.sqrt(max(len(pairs), 1)), 1.0)
    add_currency_votes(currency_scores, bull, bear, weight)
    top_items.append(
        {
            "dedupe_key": n.get("dedupe_key"),
            "published_at": n.get("published_at"),
            "title": n.get("title"),
            "source": n.get("source"),
            "snippet": n.get("snippet"),
            "impact_magnitude": n.get("impact_magnitude"),
            "impact_fx_pairs": pairs,
            "currencies_bullish": bull,
            "currencies_bearish": bear,
            "fx_directional_hint": n.get("fx_directional_hint"),
            "origin": n.get("origin"),
        }
    )
    for pair in pairs:
        if len(pair) != 6:
            continue
        pf = pair_focus[pair]
        if recent_24h:
            pf["news_count_24h"] += 1
        pf["evidence_weight"] += pair_weight
        bias = bias_for_pair(pair, bull, bear, n.get("fx_directional_hint"))
        if bias != "mixed":
            pf["bias_scores"][bias] += pair_weight
        if len(pf["top_drivers"]) < 5 and n.get("title"):
            pf["top_drivers"].append(n.get("title"))
        if str(n.get("impact_magnitude") or "").lower() == "high" and urgent_4h:
            pf["urgent_event_within_4h"] = True

for pr in ctx.get("fx_pairs") or []:
    pair = str(pr.get("pair") or "").upper()
    if len(pair) != 6:
        continue
    pf = pair_focus[pair]
    macro_bias = str(pr.get("directional_bias") or pr.get("bias") or "").strip()
    if macro_bias:
        pf["bias_macro"] = macro_bias
    conf = pr.get("confidence")
    if conf is not None:
        try:
            pf["confidence"] = float(conf)
        except Exception:
            pass

pair_payload = {"pairs": {}}
for pair, pf in sorted(pair_focus.items()):
    scores = dict(pf.pop("bias_scores"))
    if scores:
        ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        top_bias, top_score = ordered[0]
        runner_up = ordered[1][1] if len(ordered) > 1 else 0.0
        bias_news = top_bias if top_score >= 1.25 and top_score >= runner_up * 1.25 else "mixed"
    else:
        top_score = 0.0
        bias_news = "mixed"

    evidence = pf["evidence_weight"]
    pair_payload["pairs"][pair] = {
        "news_count_24h": pf["news_count_24h"],
        "bias_news": bias_news,
        "bias_news_score": round(top_score, 3),
        "bias_macro": pf.get("bias_macro", "unknown"),
        "confidence": pf.get("confidence", round(min(0.85, 0.3 + min(evidence, 6.0) * 0.08), 3)),
        "top_drivers": pf["top_drivers"],
        "urgent_event_within_4h": pf["urgent_event_within_4h"],
    }

macro = ctx.get("fx_macro") or {}
table_biases = parse_bias_json(macro.get("bias_json") or macro.get("biases"))
if not table_biases:
    table_biases = {
        cur: safe_float(macro.get(f"{cur.lower()}_bias"), None)
        for cur in CURRENCIES
        if macro.get(f"{cur.lower()}_bias") is not None
    }

computed_macro = macro_from_news(news, as_of)
macro_payload = {
    "market_regime": macro.get("market_regime") or macro.get("regime") or computed_macro["market_regime"],
    "drivers": macro.get("drivers") or macro.get("narrative") or macro.get("notes") or computed_macro["drivers"],
    "confidence": macro.get("confidence") if macro.get("confidence") is not None else computed_macro["confidence"],
    "biases": table_biases or computed_macro["biases"],
    "as_of": str(macro.get("as_of") or as_of),
    "source": "fx_macro_table" if macro else computed_macro["source"],
}

sections = {
    "top_news": {"items": top_items, "as_of": as_of, "lookback_hours": int(ctx.get("lookback_hours") or 96)},
    "pair_focus": pair_payload,
    "macro_regime": macro_payload,
}

return [{"json": {**ctx, "digest_sections": sections}}]
