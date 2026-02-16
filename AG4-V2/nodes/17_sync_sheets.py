import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, date

DB_PATH = "/files/duckdb/ag4_v2.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
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

def fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    s = str(v)
    if s.lower() in ("nan", "nat", "none"):
        return ""
    return s

items = _items or []
run_id = ""
db_path = DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id", "") or "")
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

out = []
with db_con(db_path) as con:
    if not run_id:
        rr = con.execute("SELECT run_id FROM run_log ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(rr[0]) if rr and rr[0] else ""

    if not run_id:
        return []

    news_rows = con.execute(
        """
        SELECT
          dedupe_key, event_key, run_id, canonical_url, published_at, title, source, feed_url,
          symbols, type, notes, impact_score, confidence, urgency, snippet, first_seen_at,
          strategy, losers, winners, theme, regime, analyzed_at
        FROM news_history
        WHERE run_id = ?
        ORDER BY COALESCE(published_at, analyzed_at) DESC, updated_at DESC
        """,
        [run_id],
    ).fetchall()

    for r in news_rows:
        out.append({
            "json": {
                "dedupeKey": fmt(r[0]),
                "eventKey": fmt(r[1]),
                "runId": fmt(r[2]),
                "canonicalUrl": fmt(r[3]),
                "publishedAt": fmt(r[4]),
                "title": fmt(r[5]),
                "source": fmt(r[6]),
                "feedUrl": fmt(r[7]),
                "symbols": fmt(r[8]),
                "type": fmt(r[9]),
                "notes": fmt(r[10]),
                "ImpactScore": fmt(r[11]),
                "confidence": fmt(r[12]),
                "urgency": fmt(r[13]),
                "Snippet": fmt(r[14]),
                "firstSeenAt": fmt(r[15]),
                "Strategy": fmt(r[16]),
                "Losers": fmt(r[17]),
                "Winners": fmt(r[18]),
                "Theme": fmt(r[19]),
                "Regime": fmt(r[20]),
                "analyzedAt": fmt(r[21]),
            }
        })

    err_rows = con.execute(
        """
        SELECT dedupe_key, run_id, feed_url, http_code, error_message, occurred_at
        FROM news_errors
        WHERE run_id = ?
        ORDER BY occurred_at DESC
        """,
        [run_id],
    ).fetchall()

    for r in err_rows:
        http_txt = f" {int(r[3])}" if r[3] is not None else ""
        occurred = fmt(r[5])
        feed_url = fmt(r[2])
        out.append({
            "json": {
                "dedupeKey": fmt(r[0]),
                "eventKey": "",
                "runId": fmt(r[1]),
                "canonicalUrl": feed_url,
                "publishedAt": occurred,
                "title": f"RSS_ERROR{http_txt}".strip(),
                "source": "rss_feed",
                "feedUrl": feed_url,
                "symbols": "",
                "type": "rss_error",
                "notes": fmt(r[4]),
                "ImpactScore": "0",
                "confidence": "1",
                "urgency": "immediate",
                "Snippet": f"RSS fetch error on {feed_url}",
                "firstSeenAt": occurred,
                "Strategy": "",
                "Losers": "",
                "Winners": "",
                "Theme": "Pipeline/Error",
                "Regime": "Neutral",
                "analyzedAt": occurred,
            }
        })

return out
