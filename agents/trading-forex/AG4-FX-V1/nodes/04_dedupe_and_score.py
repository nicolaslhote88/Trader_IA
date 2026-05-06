import hashlib


def key_for(row):
    raw = str(row.get("dedupe_key") or "").strip()
    if raw:
        return raw
    title = " ".join(str(row.get("title") or "").lower().split())
    day = str(row.get("published_at") or "")[:10]
    return hashlib.sha1(f"{title}|{day}".encode("utf-8")).hexdigest()


def impact_weight(row):
    mag = str(row.get("impact_magnitude") or "").lower()
    base = {"high": 3, "medium": 2, "low": 1}.get(mag, 1)
    pairs = [p for p in str(row.get("impact_fx_pairs") or "").replace(";", ",").split(",") if p.strip()]
    bull = [x for x in str(row.get("currencies_bullish") or "").replace(";", ",").split(",") if x.strip()]
    bear = [x for x in str(row.get("currencies_bearish") or "").replace(";", ",").split(",") if x.strip()]
    hint = str(row.get("fx_directional_hint") or "").strip()
    signal_bonus = 8 if (bull or bear or hint) else 0
    neutral_penalty = -8 if str(row.get("origin") or "") == "official_source" and not signal_bonus else 0
    return base * 10 + min(len(pairs), 8) + signal_bonus + neutral_penalty


ctx = (_items or [{"json": {}}])[0].get("json", {})
seen = {}
for row in (ctx.get("global_news") or []) + (ctx.get("fx_channel_news") or []) + (ctx.get("official_news") or []):
    k = key_for(row)
    row = dict(row)
    row["dedupe_key"] = k
    old = seen.get(k)
    if old is None or impact_weight(row) > impact_weight(old):
        seen[k] = row

items = sorted(seen.values(), key=lambda r: (impact_weight(r), str(r.get("published_at") or "")), reverse=True)[:30]
return [{"json": {**ctx, "deduped_news": items, "news_after_dedupe": len(items)}}]
