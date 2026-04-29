from collections import defaultdict
from datetime import datetime, timezone


def split_csv(v):
    return [x.strip().upper() for x in str(v or "").replace(";", ",").split(",") if x.strip()]


def bias_for_pair(pair, bull, bear):
    base, quote = pair[:3], pair[3:]
    if base in bull or quote in bear:
        return f"bullish_{base.lower()}"
    if base in bear or quote in bull:
        return f"bearish_{base.lower()}"
    return "mixed"


ctx = (_items or [{"json": {}}])[0].get("json", {})
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
news = ctx.get("deduped_news") or []

top_items = []
pair_focus = defaultdict(lambda: {"news_count_24h": 0, "bias_votes": defaultdict(int), "top_drivers": [], "urgent_event_within_4h": False})
for n in news:
    pairs = split_csv(n.get("impact_fx_pairs"))
    bull = split_csv(n.get("currencies_bullish"))
    bear = split_csv(n.get("currencies_bearish"))
    top_items.append({
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
    })
    for pair in pairs:
        pf = pair_focus[pair]
        pf["news_count_24h"] += 1
        pf["bias_votes"][bias_for_pair(pair, bull, bear)] += 1
        if len(pf["top_drivers"]) < 5 and n.get("title"):
            pf["top_drivers"].append(n.get("title"))
        if str(n.get("impact_magnitude") or "").lower() == "high":
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
    votes = dict(pf.pop("bias_votes"))
    bias_news = max(votes.items(), key=lambda kv: kv[1])[0] if votes else "mixed"
    pair_payload["pairs"][pair] = {
        "news_count_24h": pf["news_count_24h"],
        "bias_news": bias_news,
        "bias_macro": pf.get("bias_macro", "unknown"),
        "confidence": pf.get("confidence", min(0.9, 0.35 + 0.08 * pf["news_count_24h"])),
        "top_drivers": pf["top_drivers"],
        "urgent_event_within_4h": pf["urgent_event_within_4h"],
    }

macro = ctx.get("fx_macro") or {}
macro_payload = {
    "market_regime": macro.get("market_regime") or macro.get("regime") or "unknown",
    "drivers": macro.get("drivers") or macro.get("narrative") or macro.get("notes") or "",
    "confidence": macro.get("confidence"),
    "biases": macro.get("biases") or {},
    "as_of": str(macro.get("as_of") or as_of),
}

sections = {
    "top_news": {"items": top_items, "as_of": as_of, "lookback_hours": int(ctx.get("lookback_hours") or 24)},
    "pair_focus": pair_payload,
    "macro_regime": macro_payload,
}

return [{"json": {**ctx, "digest_sections": sections}}]
