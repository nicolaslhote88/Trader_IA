import duckdb, time, gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"

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

items = _items

run_id = ""
symbols_ok = 0
symbols_error = 0
ai_calls = 0
vectors_written = 0
errors = []

for it in items:
    d = it.get("json", {}) or {}

    if not run_id:
        run_id = str(d.get("run_id", "") or "")

    if d.get("_status") == "error":
        symbols_error += 1
        sym = str(d.get("symbol", "?") or "?")
        err = str(d.get("error", "?") or "?")
        errors.append(f"{sym}: {err}")
    else:
        symbols_ok += 1

    if d.get("call_ai") is True:
        ai_calls += 1

    if str(d.get("vector_status", "") or "").upper() == "DONE":
        vectors_written += 1

status = "NO_RUN"
if run_id:
    status = "SUCCESS" if symbols_error == 0 else ("PARTIAL" if symbols_ok > 0 else "FAILED")

    with db_con() as con:
        con.execute(
            "UPDATE run_log SET finished_at = CURRENT_TIMESTAMP, status = ?, symbols_ok = ?, symbols_error = ?, ai_calls = ?, vectors_written = ?, error_detail = ? WHERE run_id = ?",
            [
                status,
                symbols_ok,
                symbols_error,
                ai_calls,
                vectors_written,
                ("; ".join(errors)[:500] if errors else None),
                run_id,
            ],
        )

return [{"json": {
    "run_id": run_id,
    "status": status,
    "symbols_ok": symbols_ok,
    "symbols_error": symbols_error,
    "ai_calls": ai_calls,
    "vectors_written": vectors_written,
}}]
