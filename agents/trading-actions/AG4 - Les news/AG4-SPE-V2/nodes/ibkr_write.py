import duckdb
import time
import json
from datetime import datetime, timezone

def connect_w(path, tries=6, delay=0.4):
    last = None
    for i in range(tries):
        try:
            return duckdb.connect(path)
        except Exception as e:
            last = e
            if "lock" in str(e).lower() and i < tries - 1:
                time.sleep(delay * (2 ** i)); continue
            raise
    raise last

def tb(v, d=False):
    if isinstance(v, bool): return v
    if v is None: return d
    return str(v).strip().lower() in ("1", "true", "yes", "y")

items = _items or []
if not items:
    return []
DB = str((items[0].get("json", {}) or {}).get("db_path") or "/files/duckdb/ag4_spe_v2.duckdb")
cols = ["news_id","run_id","symbol","company_name","source","url","canonical_url","title",
        "published_at","published_at_raw","snippet","text","summary","category","impact_score",
        "sentiment","confidence_score","horizon","urgency","suggested_signal","key_drivers",
        "needs_follow_up","is_relevant","relevance_reason","action","reason","status",
        "first_seen_at","last_seen_at","analyzed_at","fetched_at","provider","news_article_id",
        "ibkr_sentiment"]
ph = ",".join(["?"] * len(cols))
sql = "INSERT OR REPLACE INTO news_history (" + ",".join(cols) + ",updated_at) VALUES (" + ph + ",CURRENT_TIMESTAMP)"
con = connect_w(DB)
n = 0
for it in items:
    j = it.get("json", {}) or {}
    observed = j.get("firstSeenAt") or j.get("analyzedAt") or datetime.now(timezone.utc).isoformat()
    pub = j.get("publishedAt")
    try:
        if datetime.fromisoformat(str(pub).replace("Z", "+00:00")) > datetime.fromisoformat(str(observed).replace("Z", "+00:00")):
            pub = observed
    except (TypeError, ValueError):
        pub = observed
    con.execute(sql, [
        j.get("newsId"), j.get("run_id"), j.get("symbol"), j.get("companyName") or j.get("symbol"),
        j.get("source") or "ibkr", j.get("url"), j.get("canonicalUrl") or j.get("url"), j.get("title"),
        pub, json.dumps(j.get("providerTimeContract") or {"provider_published_at": j.get("publishedAt"), "timestamp_quality": "LEGACY_UNVERIFIED"}), j.get("snippet"), j.get("text"),
        j.get("summary"), j.get("category"), j.get("impactScore"), j.get("sentiment"),
        j.get("confidence"), j.get("horizon"), j.get("urgency"), j.get("suggestedSignal"),
        j.get("keyDrivers"), tb(j.get("needsFollowUp")), tb(j.get("isRelevant"), True),
        j.get("relevanceReason"), j.get("action") or "analyze", j.get("reason") or "ibkr_portfolio",
        j.get("status") or "ANALYZED", j.get("firstSeenAt"), j.get("firstSeenAt"),
        j.get("analyzedAt"), j.get("fetchedAt"), j.get("provider"), j.get("newsArticleId"),
        j.get("ibkrSentiment"),
    ])
    n += 1
con.close()
return [{"json": {"written": n, "db_path": DB}}]
