import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone, date

DB_PATH = "/files/duckdb/ag4_v2.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
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
                con.close()
            except Exception:
                pass
        gc.collect()

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

def to_text(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return ", ".join([str(x) for x in v if x is not None and str(x).strip()])
    return str(v)

def ensure_utc_aware(dt):
    if dt is None:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        if isinstance(v, date) and not isinstance(v, datetime):
            return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
        return ensure_utc_aware(v)
    s = str(v).strip()
    if not s or s.lower() == "unknown":
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return ensure_utc_aware(datetime.fromisoformat(s))
    except Exception:
        return None

items = _items or []
if not items:
    return []

db_path = DB_PATH
running_run_id = ""
for it in items:
    j = it.get("json", {}) or {}
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    rr = con.execute("SELECT run_id FROM run_log WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
    running_run_id = str(rr[0]) if rr and rr[0] else ""

    for it in items:
        j = dict(it.get("json", {}) or {})

        dedupe_key = str(j.get("dedupeKey", "") or "").strip()
        if not dedupe_key:
            continue

        run_id = str(j.get("run_id", "") or running_run_id)
        event_key = str(j.get("eventKey", "") or "")
        canonical_url = str(j.get("canonicalUrl", "") or "")
        published_at = parse_ts(j.get("publishedAt"))
        title = str(j.get("title", "") or "")
        source = str(j.get("source", "") or "")
        feed_url = str(j.get("feedUrl", "") or "")
        symbols = to_text(j.get("symbols", ""))
        typ = str(j.get("type", "") or ("symbol" if symbols else "macro"))
        notes = str(j.get("notes", "") or "")
        impact_score = safe_int(j.get("ImpactScore"), 0)
        confidence = safe_float(j.get("confidence"), 0.0)
        urgency = str(j.get("urgency", "") or "low")
        snippet = str(j.get("Snippet", "") or j.get("snippet", "") or "")
        now_utc = datetime.now(timezone.utc)
        new_first_seen = parse_ts(j.get("firstSeenAt")) or published_at or now_utc
        strategy = str(j.get("Strategy", "") or "")
        losers = str(j.get("Losers", "") or "")
        winners = str(j.get("Winners", "") or "")
        theme = str(j.get("Theme", "") or "Resultats/Micro")
        regime = str(j.get("Regime", "") or "Neutral")
        analyzed_at = parse_ts(j.get("analyzedAt")) or now_utc
        last_seen_at = parse_ts(j.get("seenNowAt")) or now_utc
        source_tier = safe_int(j.get("sourceTier"), 2)
        action = str(j.get("_action", "") or "skip")
        reason = str(j.get("_reason", "") or "")

        existing = con.execute(
            "SELECT first_seen_at FROM news_history WHERE dedupe_key = ?",
            [dedupe_key],
        ).fetchone()
        existing_first_seen = parse_ts(existing[0]) if existing and existing[0] is not None else None
        if existing_first_seen is not None and existing_first_seen < new_first_seen:
            first_seen_at = existing_first_seen
        else:
            first_seen_at = new_first_seen

        con.execute(
            """
            INSERT OR REPLACE INTO news_history (
              dedupe_key, event_key, run_id, canonical_url, published_at, title, source, feed_url,
              symbols, type, notes, impact_score, confidence, urgency, snippet, first_seen_at,
              strategy, losers, winners, theme, regime, analyzed_at, last_seen_at,
              source_tier, action, reason, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                dedupe_key, event_key, run_id, canonical_url, published_at, title, source, feed_url,
                symbols, typ, notes, impact_score, confidence, urgency, snippet, first_seen_at,
                strategy, losers, winners, theme, regime, analyzed_at, last_seen_at,
                source_tier, action, reason,
            ],
        )

return items
