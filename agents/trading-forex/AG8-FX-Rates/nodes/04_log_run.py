"""AG8-FX-Rates — Log du run."""
import os
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
run_id = ctx.get("run_id", "unknown")
signals = ctx.get("rates_signals", [])
error = ctx.get("rates_error") or ctx.get("db_error")
status = "error" if error else "ok"

steepener_count = sum(1 for s in signals if s.get("rates_signal") == "steepener")
us_slope = ctx.get("us_slope")

try:
    if os.path.exists(db_path):
        with duckdb.connect(db_path) as con:
            con.execute(
                """INSERT OR REPLACE INTO pillars.run_log
                   (run_id, finished_at, status, error_msg, records_written)
                   VALUES (?, ?, ?, ?, ?)""",
                [run_id, datetime.now(timezone.utc).isoformat(), status, error, len(signals)],
            )
except Exception:
    pass

return [{"json": {
    "run_id": run_id,
    "status": status,
    "currencies_analyzed": len(signals),
    "steepener_signals": steepener_count,
    "us_slope_10y2y": us_slope,
    "error": error,
}}]
