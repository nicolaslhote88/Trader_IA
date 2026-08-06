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
cursor_advanced = False
cursor_error = ""
state_key = ""
next_index = None
expected = 0
processed = 0

for it in items:
    d = it.get("json", {}) or {}

    if not run_id:
        run_id = str(d.get("run_id", "") or "")
    if not batch_info and isinstance(d.get("batch_info"), dict):
        batch_info = dict(d.get("batch_info") or {})

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
    expected = int(batch_info.get("size") or 0)
    processed = symbols_ok + symbols_error
    state_key = str(batch_info.get("state_key") or "")
    if "next_index" in batch_info:
        next_index = int(batch_info.get("next_index") or 0)

    can_advance = status in ("SUCCESS", "PARTIAL")
    if can_advance:
        if not batch_info:
            cursor_error = "BATCH_INFO_MISSING"
        elif expected <= 0:
            cursor_error = f"BATCH_SIZE_INVALID:{expected}"
        elif processed != expected:
            cursor_error = f"BATCH_PROCESSED_MISMATCH:{processed}!={expected}"
        elif not state_key:
            cursor_error = "BATCH_STATE_KEY_MISSING"
        elif next_index is None or next_index < 0:
            cursor_error = f"BATCH_NEXT_INDEX_INVALID:{next_index}"

    with db_con() as con:
        con.execute("BEGIN TRANSACTION")
        if can_advance and not cursor_error:
            con.execute(
                "INSERT OR REPLACE INTO batch_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                [state_key, next_index],
            )
            persisted = con.execute(
                "SELECT value FROM batch_state WHERE key = ?",
                [state_key],
            ).fetchone()
            persisted_value = int(persisted[0]) if persisted else None
            if persisted_value != next_index:
                cursor_error = f"BATCH_CURSOR_VERIFY_FAILED:{persisted_value}!={next_index}"
            else:
                cursor_advanced = True

        if cursor_error:
            status = "FAILED"
            errors.append(cursor_error)

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
        con.execute("COMMIT")

if not run_id:
    raise RuntimeError("AG2_RUN_CONTEXT_MISSING: Finalize Run received no run_id")

if cursor_error:
    raise RuntimeError(f"AG2_CURSOR_GUARD_FAILED: {cursor_error}")

if status == "FAILED":
    sample = "; ".join(errors[:3])
    raise RuntimeError(
        f"AG2_RUN_FAILED: all {symbols_error} symbols failed"
        + (f" ({sample})" if sample else "")
    )

return [{"json": {
    "run_id": run_id,
    "status": status,
    "symbols_ok": symbols_ok,
    "symbols_error": symbols_error,
    "ai_calls": ai_calls,
    "batch_start": batch_info.get("start"),
    "batch_size": expected,
    "batch_state_key": state_key,
    "batch_next_index": next_index,
    "cursor_advanced": cursor_advanced,
}}]
