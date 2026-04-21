import duckdb
import gc
import hashlib
import time
from contextlib import contextmanager
from datetime import date, datetime

DEFAULT_DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
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


def to_iso(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


def bool_or_default(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y"):
        return True
    if s in ("0", "false", "no", "n"):
        return False
    return default


def build_news_id(symbol, canonical_url):
    seed = f"{str(symbol or '').upper()}|{str(canonical_url or '')}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


items = _items or []
if len(items) == 0:
    return []

db_path = str((items[0].get("json", {}) or {}).get("db_path") or DEFAULT_DB_PATH)

query = """
SELECT
  news_id,
  title,
  published_at,
  snippet,
  text,
  summary,
  category,
  impact_score,
  sentiment,
  confidence_score,
  horizon,
  urgency,
  suggested_signal,
  key_drivers,
  needs_follow_up,
  is_relevant,
  relevance_reason,
  first_seen_at,
  analyzed_at,
  reason,
  status
FROM news_history
WHERE news_id = ?
LIMIT 1
"""

out = []
with db_con(db_path) as con:
    for it in items:
        j = dict(it.get("json", {}) or {})

        symbol = str(j.get("symbol", "") or "").upper()
        canonical_url = str(j.get("articleCanonicalUrl", "") or j.get("canonicalUrl", "") or "")
        news_id = str(j.get("newsId", "") or "").strip()
        if not news_id and canonical_url:
            news_id = build_news_id(symbol, canonical_url)
            j["newsId"] = news_id

        existing = None
        if news_id:
            existing = con.execute(query, [news_id]).fetchone()

        if existing:
            j["_action"] = "skip"
            j["_reason"] = "duplicate_known"
            j["_articlesLoopReset"] = False
            j["title"] = j.get("articleTitleGuess") or existing[1] or j.get("title") or ""
            j["publishedAt"] = j.get("publishedAtGuess") or to_iso(existing[2]) or j.get("publishedAt") or None
            j["snippet"] = existing[3] or j.get("snippetGuess") or ""
            j["text"] = existing[4] or j.get("text") or ""
            j["summary"] = existing[5] or ""
            j["category"] = existing[6] or "Noise"
            j["impactScore"] = int(existing[7]) if existing[7] is not None else 0
            j["sentiment"] = existing[8] or "Neutral"
            j["confidence"] = int(existing[9]) if existing[9] is not None else 0
            j["horizon"] = existing[10] or "Days"
            j["urgency"] = existing[11] or "Low"
            j["suggestedSignal"] = existing[12] or "WATCH"
            j["keyDrivers"] = existing[13] or ""
            j["needsFollowUp"] = bool_or_default(existing[14], False)
            j["isRelevant"] = bool_or_default(existing[15], True)
            j["relevanceReason"] = existing[16] or "Duplicate entry already analyzed"
            j["firstSeenAt"] = to_iso(existing[17]) or j.get("firstSeenAt")
            j["analyzedAt"] = to_iso(existing[18]) or j.get("analyzedAt")
            j["status"] = existing[20] or "SKIPPED_DUPLICATE"
        else:
            j["_action"] = "analyze"
            j["_reason"] = "new_item"
            j["_articlesLoopReset"] = False
            if not j.get("publishedAt"):
                j["publishedAt"] = j.get("publishedAtGuess") or None
            if not j.get("snippet"):
                j["snippet"] = j.get("snippetGuess") or ""

        out.append({"json": j, "pairedItem": it.get("pairedItem")})

return out
