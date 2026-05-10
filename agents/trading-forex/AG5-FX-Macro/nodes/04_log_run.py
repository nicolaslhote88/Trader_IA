"""
AG5-FX-Macro — Log du run dans macro_data.duckdb.
"""
import os
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))

run_id = ctx.get("run_id", "unknown")
scores = ctx.get("pillar_scores", [])
error = ctx.get("scores_error") or ctx.get("refresh_error")
status = "error" if error else "ok"

try:
    if os.path.exists(db_path):
        with duckdb.connect(db_path) as con:
            con.execute(
                """INSERT OR REPLACE INTO pillars.run_log
                   (run_id, finished_at, status, error_msg, records_written)
                   VALUES (?, ?, ?, ?, ?)""",
                [run_id, datetime.now(timezone.utc).isoformat(), status, error, len(scores)],
            )
except Exception as exc:
    pass  # Ne pas crasher sur le log

return [{"json": {
    "run_id": run_id,
    "status": status,
    "currencies_scored": len(scores),
    "error": error,
}}]
