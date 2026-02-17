import duckdb
import gc
import time
from contextlib import contextmanager

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
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


items = _items or []
run_id = ""
db_path = DEFAULT_DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id", "") or "")
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    if not run_id:
        rr = con.execute(
            "SELECT run_id FROM run_log WHERE status = 'RUNNING' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        run_id = str(rr[0]) if rr and rr[0] else ""

    if not run_id:
        return [{"json": {"status": "NO_RUN", "run_id": "", "db_path": db_path}}]

    r = con.execute("SELECT COALESCE(symbols_total, 0) FROM run_log WHERE run_id = ?", [run_id]).fetchone()
    symbols_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(DISTINCT symbol) FROM news_history WHERE run_id = ?", [run_id]).fetchone()
    symbols_ok = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(DISTINCT symbol) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    symbols_error = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ?", [run_id]).fetchone()
    articles_total = int(r[0]) if r else 0

    r = con.execute(
        "SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'analyze'",
        [run_id],
    ).fetchone()
    items_analyzed = int(r[0]) if r else 0

    r = con.execute(
        "SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'skip'",
        [run_id],
    ).fetchone()
    items_skipped = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    errors_logged = int(r[0]) if r else 0

    r = con.execute("SELECT COALESCE(vector_docs_written, 0) FROM run_log WHERE run_id = ?", [run_id]).fetchone()
    vector_docs_written = int(r[0]) if r else 0

    if symbols_total == 0:
        status = "NO_DATA"
    elif articles_total > 0 and errors_logged == 0:
        status = "SUCCESS"
    elif articles_total > 0 and errors_logged > 0:
        status = "PARTIAL"
    elif articles_total == 0 and errors_logged > 0:
        status = "FAILED"
    else:
        status = "NO_OUTPUT"

    con.execute(
        """
        UPDATE run_log
        SET finished_at = CURRENT_TIMESTAMP,
            status = ?,
            symbols_ok = ?,
            symbols_error = ?,
            articles_total = ?,
            items_analyzed = ?,
            items_skipped = ?,
            errors_logged = ?,
            error_detail = ?
        WHERE run_id = ?
        """,
        [status, symbols_ok, symbols_error, articles_total, items_analyzed, items_skipped, errors_logged, None, run_id],
    )

return [
    {
        "json": {
            "run_id": run_id,
            "db_path": db_path,
            "status": status,
            "symbols_total": symbols_total,
            "symbols_ok": symbols_ok,
            "symbols_error": symbols_error,
            "articles_total": articles_total,
            "items_analyzed": items_analyzed,
            "items_skipped": items_skipped,
            "errors_logged": errors_logged,
            "vector_docs_written": vector_docs_written,
        }
    }
]
