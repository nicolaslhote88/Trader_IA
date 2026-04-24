import duckdb
from datetime import datetime, timezone

DB_PATH = "/files/duckdb/ag4_forex_v1.duckdb"


def urgency_score(v):
    s = str(v or "").strip().lower()
    return {"immediate": 1.0, "today": 0.75, "this_week": 0.45, "low": 0.2}.get(s, 0.2)


def init_schema(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.fx_news_history (
            dedupe_key VARCHAR PRIMARY KEY,
            event_key VARCHAR,
            run_id VARCHAR,
            origin VARCHAR,
            canonical_url VARCHAR,
            published_at TIMESTAMP,
            title VARCHAR,
            source VARCHAR,
            source_tier VARCHAR,
            snippet VARCHAR,
            impact_region VARCHAR,
            impact_magnitude VARCHAR,
            impact_fx_pairs VARCHAR,
            currencies_bullish VARCHAR,
            currencies_bearish VARCHAR,
            regime VARCHAR,
            theme VARCHAR,
            urgency DOUBLE,
            confidence DOUBLE,
            impact_score INTEGER,
            fx_narrative VARCHAR,
            fx_directional_hint VARCHAR,
            tagger_version VARCHAR,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            analyzed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_fxnh_published ON main.fx_news_history(published_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fxnh_magnitude ON main.fx_news_history(impact_magnitude)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fxnh_pairs ON main.fx_news_history(impact_fx_pairs)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fxnh_origin ON main.fx_news_history(origin)")


def ts(v):
    if not v:
        return None
    s = str(v)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


items = _items or []
if not items:
    return []

db_path = next((str((it.get("json") or {}).get("db_path")) for it in items if (it.get("json") or {}).get("db_path")), DB_PATH)

with duckdb.connect(db_path) as con:
    init_schema(con)
    for it in items:
        j = it.get("json", {}) or {}
        classes = {p.strip() for p in str(j.get("impact_asset_class") or "").split(",") if p.strip()}
        if "FX" not in classes and "Mixed" not in classes:
            continue
        now = datetime.now(timezone.utc)
        con.execute(
            """
            INSERT OR REPLACE INTO main.fx_news_history (
                dedupe_key, event_key, run_id, origin, canonical_url, published_at,
                title, source, source_tier, snippet,
                impact_region, impact_magnitude, impact_fx_pairs,
                currencies_bullish, currencies_bearish, regime, theme,
                urgency, confidence, impact_score,
                fx_narrative, fx_directional_hint, tagger_version,
                first_seen_at, last_seen_at, analyzed_at, updated_at
            ) VALUES (?, ?, ?, 'fx_channel', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                j.get("dedupeKey"), j.get("eventKey") or "", j.get("run_id") or "",
                j.get("canonicalUrl") or "", ts(j.get("publishedAt")),
                j.get("title") or "", j.get("source") or "", str(j.get("sourceTier") or ""),
                j.get("Snippet") or "", j.get("impact_region") or "", j.get("impact_magnitude") or "",
                j.get("impact_fx_pairs") or "", j.get("currencies_bullish") or "",
                j.get("currencies_bearish") or "", j.get("Regime") or "", j.get("Theme") or "",
                urgency_score(j.get("urgency")), float(j.get("confidence") or 0),
                int(j.get("ImpactScore") or 0), j.get("Strategy") or "",
                j.get("fx_directional_hint") or "", j.get("tagger_version") or "geo_v1",
                ts(j.get("firstSeenAt")) or now, ts(j.get("seenNowAt")) or now, ts(j.get("analyzedAt")) or now,
            ],
        )

return items

