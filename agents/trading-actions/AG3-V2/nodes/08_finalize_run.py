import duckdb, time, gc
from contextlib import contextmanager

DEFAULT_DB_PATH = "/files/duckdb/ag3_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
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


items = _items or []
run_id = ""
db_path = DEFAULT_DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id") or "").strip()
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    if not run_id:
        rr = con.execute("SELECT run_id FROM run_log WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(rr[0]) if rr and rr[0] else ""

    if not run_id:
        return [{"json": {"status": "NO_RUN", "run_id": "", "db_path": db_path}}]

    r = con.execute("SELECT COALESCE(symbols_total, 0) FROM run_log WHERE run_id = ?", [run_id]).fetchone()
    symbols_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM fundamentals_snapshot WHERE run_id = ?", [run_id]).fetchone()
    snapshot_rows = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM fundamentals_snapshot WHERE run_id = ? AND status = 'OK'", [run_id]).fetchone()
    symbols_ok = int(r[0]) if r else 0
    symbols_error = max(snapshot_rows - symbols_ok, 0)

    r = con.execute("SELECT COUNT(*) FROM fundamentals_triage_history WHERE run_id = ?", [run_id]).fetchone()
    triage_rows = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM analyst_consensus_history WHERE run_id = ?", [run_id]).fetchone()
    consensus_rows = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM fundamental_metrics_history WHERE run_id = ?", [run_id]).fetchone()
    metric_rows = int(r[0]) if r else 0

    if symbols_total == 0:
        status = "NO_DATA"
    elif symbols_ok == symbols_total and symbols_total > 0:
        status = "SUCCESS"
    elif symbols_ok > 0:
        status = "PARTIAL"
    else:
        status = "FAILED"

    con.execute(
        """
        UPDATE run_log
        SET finished_at = CURRENT_TIMESTAMP,
            status = ?,
            symbols_ok = ?,
            symbols_error = ?,
            triage_rows = ?,
            consensus_rows = ?,
            metric_rows = ?,
            snapshot_rows = ?,
            error_detail = ?
        WHERE run_id = ?
        """,
        [status, symbols_ok, symbols_error, triage_rows, consensus_rows, metric_rows, snapshot_rows, None, run_id],
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
            "triage_rows": triage_rows,
            "consensus_rows": consensus_rows,
            "metric_rows": metric_rows,
            "snapshot_rows": snapshot_rows,
        }
    }
]
