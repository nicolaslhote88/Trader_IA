import duckdb, time, gc
from contextlib import contextmanager

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

items = _items or []
run_id = ""
db_path = DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id", "") or "")
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    if not run_id:
        rr = con.execute("SELECT run_id FROM run_log WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(rr[0]) if rr and rr[0] else ""

    if not run_id:
        return [{"json": {"status": "NO_RUN", "run_id": "", "db_path": db_path}}]

    r = con.execute("SELECT COALESCE(sources_total, 0) FROM run_log WHERE run_id = ?", [run_id]).fetchone()
    sources_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(DISTINCT COALESCE(feed_url, dedupe_key)) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    feeds_error = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    errors_logged = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ?", [run_id]).fetchone()
    items_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'analyze'", [run_id]).fetchone()
    items_analyzed = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'skip'", [run_id]).fetchone()
    items_skipped = int(r[0]) if r else 0

    feeds_ok = max(sources_total - feeds_error, 0)

    if items_total == 0 and sources_total == 0:
        status = "NO_DATA"
    elif feeds_error == 0:
        status = "SUCCESS"
    elif items_total > 0:
        status = "PARTIAL"
    else:
        status = "FAILED"

    con.execute(
        """
        UPDATE run_log
        SET finished_at = CURRENT_TIMESTAMP,
            status = ?,
            feeds_ok = ?,
            feeds_error = ?,
            items_total = ?,
            items_analyzed = ?,
            items_skipped = ?,
            errors_logged = ?,
            error_detail = ?
        WHERE run_id = ?
        """,
        [status, feeds_ok, feeds_error, items_total, items_analyzed, items_skipped, errors_logged, None, run_id],
    )

return [{
    "json": {
        "run_id": run_id,
        "db_path": db_path,
        "status": status,
        "sources_total": sources_total,
        "feeds_ok": feeds_ok,
        "feeds_error": feeds_error,
        "items_total": items_total,
        "items_analyzed": items_analyzed,
        "items_skipped": items_skipped,
        "errors_logged": errors_logged,
    }
}]
