import duckdb
import time
import gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=10, delay=0.2):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(min(1.5, delay * (2 ** attempt)))
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

items = _items

run_id = ""
symbols_ok = 0
symbols_error = 0
ai_calls = 0
errors = []
batch_info = {}

for it in items:
    d = it.get("json", {}) or {}

    if not run_id:
        run_id = str(d.get("run_id", "") or "")
        batch_info = d.get("batch_info", {}) or {}

    if d.get("_status") == "error":
        symbols_error += 1
        sym = str(d.get("symbol", "?") or "?")
        err = str(d.get("error", "?") or "?")
        errors.append(f"{sym}: {err}")
    else:
        symbols_ok += 1

    if d.get("call_ai") is True:
        ai_calls += 1

status = "NO_RUN"
if run_id:
    status = "SUCCESS" if symbols_error == 0 else ("PARTIAL" if symbols_ok > 0 else "FAILED")

    with db_con() as con:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            "UPDATE run_log SET finished_at = CURRENT_TIMESTAMP, status = ?, symbols_ok = ?, symbols_error = ?, ai_calls = ?, error_detail = ? WHERE run_id = ?",
            [
                status,
                symbols_ok,
                symbols_error,
                ai_calls,
                ("; ".join(errors)[:500] if errors else None),
                run_id,
            ],
        )
        expected = int(batch_info.get("size") or 0)
        processed = symbols_ok + symbols_error
        if status == "SUCCESS" and expected > 0 and processed == expected:
            state_key = str(batch_info.get("state_key") or "")
            next_index = int(batch_info.get("next_index") or 0)
            if state_key:
                con.execute(
                    "INSERT OR REPLACE INTO batch_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [state_key, next_index],
                )
        con.execute("COMMIT")

return [{"json": {
    "run_id": run_id,
    "status": status,
    "symbols_ok": symbols_ok,
    "symbols_error": symbols_error,
    "ai_calls": ai_calls,
}}]
