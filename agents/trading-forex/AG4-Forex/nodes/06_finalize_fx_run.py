import duckdb
from datetime import datetime, timezone, timedelta

DB_PATH = "/files/duckdb/ag4_forex_v1.duckdb"


items = _items or []
run_id = ""
db_path = DB_PATH
for it in items:
    j = it.get("json", {}) or {}
    run_id = run_id or str(j.get("run_id") or "")
    db_path = str(j.get("db_path") or db_path)

with duckdb.connect(db_path) as con:
    if not run_id:
        row = con.execute("SELECT run_id FROM main.run_log ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(row[0]) if row else ""
    if not run_id:
        return [{"json": {"status": "NO_RUN"}}]

    count = con.execute(
        "SELECT COUNT(*) FROM main.fx_news_history WHERE run_id = ? AND origin = 'fx_channel'",
        [run_id],
    ).fetchone()[0]
    errors = con.execute(
        "SELECT COUNT(*) FROM main.news_errors WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]

    con.execute(
        """
        UPDATE main.run_log
        SET finished_at = CURRENT_TIMESTAMP,
            news_ingested = ?,
            news_from_fx_channels = ?,
            errors = ?,
            notes = ?
        WHERE run_id = ?
        """,
        [count, count, errors, "AG4_Forex rss ingestion", run_id],
    )

return [{"json": {"run_id": run_id, "news_from_fx_channels": count, "errors": errors}}]
