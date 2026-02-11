import duckdb

DB_PATH = "/files/duckdb/ag2_v2.duckdb"
items = _input.all()

run_id = ""
symbols_ok = 0
symbols_error = 0
ai_calls = 0
vectors_written = 0
errors = []

for item in items:
    d = item.json
    if not run_id: run_id = d.get("run_id", "")
    if d.get("_status") == "error":
        symbols_error += 1
        errors.append(d.get("symbol","?") + ": " + d.get("error","?"))
    else:
        symbols_ok += 1
    if d.get("call_ai"): ai_calls += 1
    if d.get("vector_status") == "DONE": vectors_written += 1

if run_id:
    status = "SUCCESS" if symbols_error == 0 else ("PARTIAL" if symbols_ok > 0 else "FAILED")
    con = duckdb.connect(DB_PATH)
    con.execute(
        "UPDATE run_log SET finished_at = CURRENT_TIMESTAMP, status = ?, symbols_ok = ?, symbols_error = ?, ai_calls = ?, vectors_written = ?, error_detail = ? WHERE run_id = ?",
        [status, symbols_ok, symbols_error, ai_calls, vectors_written, "; ".join(errors)[:500] if errors else None, run_id]
    )
    con.close()

return [{"json": {"run_id": run_id, "status": status if run_id else "NO_RUN", "symbols_ok": symbols_ok, "symbols_error": symbols_error, "ai_calls": ai_calls, "vectors_written": vectors_written}}]
