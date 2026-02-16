import json
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

def json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)

items = _items or []
if not items:
    return []

db_path = DB_PATH
for it in items:
    j = it.get("json", {}) or {}
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    rr = con.execute("SELECT run_id FROM run_log WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
    running_run_id = str(rr[0]) if rr and rr[0] else ""

    for it in items:
        j = it.get("json", {}) or {}
        dedupe_key = str(j.get("dedupeKey", "") or "").strip()
        if not dedupe_key:
            continue

        run_id = str(j.get("run_id", "") or running_run_id)
        feed_url = str(j.get("feedUrl", "") or j.get("canonicalUrl", "") or "")
        http_code = safe_int(j.get("httpCode"), None)
        error_message = str(j.get("notes", "") or "unknown")
        occurred_at = parse_ts(j.get("occurredAt")) or parse_ts(j.get("publishedAt")) or datetime.now(timezone.utc)
        raw_error = j.get("rawError", {})

        con.execute(
            """
            INSERT OR REPLACE INTO news_errors (
              dedupe_key, run_id, feed_url, http_code, error_message, raw_error, occurred_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                dedupe_key,
                run_id,
                feed_url,
                http_code,
                error_message,
                json.dumps(raw_error, ensure_ascii=False, default=json_default),
                occurred_at,
            ],
        )

return items
