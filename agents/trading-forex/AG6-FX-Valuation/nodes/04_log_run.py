"""AG6-FX-Valuation — Log du run."""
import os
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
run_id = ctx.get("run_id", "unknown")
scores = ctx.get("valuation_scores", [])
error = ctx.get("fetch_error") or ctx.get("write_error")
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
except Exception:
    pass

return [{"json": {"run_id": run_id, "status": status, "currencies_scored": len(scores), "error": error}}]
