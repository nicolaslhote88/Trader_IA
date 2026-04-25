import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone, date

FX_DB_PATH = "/files/duckdb/ag4_forex_v1.duckdb"


@contextmanager
def db_con(path=FX_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        if isinstance(v, date) and not isinstance(v, datetime):
            return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
        return v
    s = str(v).strip()
    if not s or s.lower() == "unknown":
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def safe_int(v, d=0):
    try:
        if v is None or v == "":
            return d
        return int(float(v))
    except Exception:
        return d


def safe_float(v, d=0.0):
    try:
        if v is None or v == "":
            return d
        return float(v)
    except Exception:
        return d


def urgency_score(v):
    s = str(v or "").strip().lower()
    if s == "immediate":
        return 1.0
    if s == "today":
        return 0.75
    if s == "this_week":
        return 0.45
    try:
        return max(0.0, min(1.0, float(s)))
    except Exception:
        return 0.2


def contains_fx_or_mixed(value):
    parts = [p.strip() for p in str(value or "").split(",") if p.strip()]
    return "FX" in parts or "Mixed" in parts


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


items = _items or []
if not items:
    return []

db_path = FX_DB_PATH
for it in items:
    j = it.get("json", {}) or {}
    if j.get("fx_db_path"):
        db_path = str(j.get("fx_db_path"))

with db_con(db_path) as con:
    init_schema(con)
    for it in items:
        j = dict(it.get("json", {}) or {})
        if not contains_fx_or_mixed(j.get("impact_asset_class")):
            continue

        now = datetime.now(timezone.utc)
        dedupe_key = str(j.get("dedupeKey", "") or "").strip()
        if not dedupe_key:
            continue

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
            ) VALUES (?, ?, ?, 'global_base', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                dedupe_key,
                str(j.get("eventKey", "") or ""),
                str(j.get("run_id", "") or ""),
                str(j.get("canonicalUrl", "") or ""),
                parse_ts(j.get("publishedAt")),
                str(j.get("title", "") or ""),
                str(j.get("source", "") or ""),
                str(j.get("sourceTier", "") or ""),
                str(j.get("Snippet", "") or j.get("snippet", "") or ""),
                str(j.get("impact_region", "") or ""),
                str(j.get("impact_magnitude", "") or ""),
                str(j.get("impact_fx_pairs", "") or ""),
                str(j.get("currencies_bullish", "") or ""),
                str(j.get("currencies_bearish", "") or ""),
                str(j.get("Regime", "") or ""),
                str(j.get("Theme", "") or ""),
                urgency_score(j.get("urgency")),
                safe_float(j.get("confidence"), 0.0),
                safe_int(j.get("ImpactScore"), 0),
                str(j.get("Strategy", "") or j.get("notes", "") or ""),
                "",
                str(j.get("tagger_version", "") or "geo_v1"),
                parse_ts(j.get("firstSeenAt")) or now,
                parse_ts(j.get("seenNowAt")) or now,
                parse_ts(j.get("analyzedAt")) or now,
            ],
        )

return items
