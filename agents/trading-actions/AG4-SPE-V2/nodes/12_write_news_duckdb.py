import duckdb
import gc
import hashlib
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone

DEFAULT_DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
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
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def to_int(v, d=0):
    try:
        if v is None or v == "":
            return d
        return int(round(float(v)))
    except Exception:
        return d


def to_text(v):
    if v is None:
        return ""
    return str(v)


def to_bool(v, d=True):
    if isinstance(v, bool):
        return v
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y"):
        return True
    if s in ("0", "false", "no", "n"):
        return False
    return d


def ensure_utc(dt):
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
        return ensure_utc(v)

    s = str(v).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        return ensure_utc(datetime.fromisoformat(s))
    except Exception:
        pass

    try:
        d = datetime.strptime(s, "%d.%m.%Y")
        return ensure_utc(d)
    except Exception:
        return None


def build_news_id(symbol, canonical_url):
    seed = f"{str(symbol or '').upper()}|{str(canonical_url or '')}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


items = _items or []
if len(items) == 0:
    return []

first_json = dict(items[0].get("json", {}) or {})
flush_rows = first_json.get("_flushRows")
rows = []
if isinstance(flush_rows, list):
    for row in flush_rows:
        if isinstance(row, dict):
            rows.append(dict(row))
else:
    for it in items:
        rows.append(dict(it.get("json", {}) or {}))

if len(rows) == 0:
    return items

db_path = str(first_json.get("db_path") or rows[0].get("db_path") or DEFAULT_DB_PATH)

with db_con(db_path) as con:
    for j in rows:

        symbol = str(j.get("symbol", "") or "").upper()
        canonical_url = str(j.get("canonicalUrl", "") or j.get("url", "") or "")
        news_id = str(j.get("newsId", "") or "").strip()
        if not news_id:
            news_id = build_news_id(symbol, canonical_url)

        run_id = str(j.get("run_id", "") or "")
        company_name = to_text(j.get("companyName", ""))
        source = to_text(j.get("source", "boursorama"))
        boursorama_ref = to_text(j.get("boursoramaRef", ""))
        listing_url = to_text(j.get("listingUrl", ""))
        url = to_text(j.get("url", canonical_url))
        title = to_text(j.get("title", ""))
        published_at_raw = to_text(j.get("publishedAt", ""))
        published_at = parse_ts(j.get("publishedAt"))
        snippet = to_text(j.get("snippet", ""))
        text = to_text(j.get("text", ""))
        summary = to_text(j.get("summary", ""))
        category = to_text(j.get("category", "Noise"))
        impact_score = to_int(j.get("impactScore"), 0)
        sentiment = to_text(j.get("sentiment", "Neutral"))
        confidence_score = to_int(j.get("confidence"), 0)
        horizon = to_text(j.get("horizon", "Days"))
        urgency = to_text(j.get("urgency", "Low"))
        suggested_signal = to_text(j.get("suggestedSignal", "WATCH"))
        key_drivers = to_text(j.get("keyDrivers", ""))
        needs_follow_up = to_bool(j.get("needsFollowUp"), False)
        is_relevant = to_bool(j.get("isRelevant"), True)
        relevance_reason = to_text(j.get("relevanceReason", ""))
        action = to_text(j.get("action", "skip"))
        reason = to_text(j.get("reason", ""))
        status = to_text(j.get("status", ""))
        now_utc = datetime.now(timezone.utc)
        incoming_first_seen = parse_ts(j.get("firstSeenAt")) or parse_ts(j.get("publishedAt")) or now_utc
        incoming_last_seen = parse_ts(j.get("lastSeenAt")) or now_utc
        analyzed_at = parse_ts(j.get("analyzedAt")) or now_utc
        fetched_at = parse_ts(j.get("fetchedAt")) or now_utc

        existing = con.execute(
            "SELECT first_seen_at, vector_status, vector_id, vectorized_at, chunk_total FROM news_history WHERE news_id = ?",
            [news_id],
        ).fetchone()
        existing_first_seen = parse_ts(existing[0]) if existing and existing[0] is not None else None
        if existing_first_seen is not None and existing_first_seen < incoming_first_seen:
            first_seen_at = existing_first_seen
        else:
            first_seen_at = incoming_first_seen

        existing_vector_status = to_text(existing[1]) if existing and existing[1] is not None else ""
        existing_vector_id = to_text(existing[2]) if existing and existing[2] is not None else ""
        existing_vectorized_at = parse_ts(existing[3]) if existing and existing[3] is not None else None
        existing_chunk_total = to_int(existing[4], None) if existing and existing[4] is not None else None

        should_vectorize = bool((text or "").strip() or (summary or "").strip() or (snippet or "").strip())
        vector_status = "PENDING" if should_vectorize else "SKIPPED"
        if existing_vector_status in ("DONE", "PENDING"):
            vector_status = existing_vector_status
        vector_id = existing_vector_id if existing_vector_status == "DONE" else ""
        vectorized_at = existing_vectorized_at if existing_vector_status == "DONE" else None
        chunk_total = existing_chunk_total if existing_chunk_total is not None else None

        con.execute(
            """
            INSERT OR REPLACE INTO news_history (
              news_id, run_id, symbol, company_name, source, boursorama_ref, listing_url,
              url, canonical_url, title, published_at, published_at_raw, snippet, text,
              summary, category, impact_score, sentiment, confidence_score, horizon,
              urgency, suggested_signal, key_drivers, needs_follow_up, is_relevant, relevance_reason,
              action, reason, status, vector_status, vector_id, vectorized_at, chunk_total,
              first_seen_at, last_seen_at, analyzed_at, fetched_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                news_id,
                run_id,
                symbol,
                company_name,
                source,
                boursorama_ref,
                listing_url,
                url,
                canonical_url,
                title,
                published_at,
                published_at_raw,
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
                action,
                reason,
                status,
                vector_status,
                vector_id,
                vectorized_at,
                chunk_total,
                first_seen_at,
                incoming_last_seen,
                analyzed_at,
                fetched_at,
            ],
        )

return items
