import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"
rows = []
latest_ag2_run_id = ""

if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            row = con.execute(
                """
                SELECT run_id
                FROM main.run_log
                ORDER BY finished_at DESC NULLS LAST, started_at DESC NULLS LAST
                LIMIT 1
                """
            ).fetchone()
            latest_ag2_run_id = row[0] if row else ""
            if latest_ag2_run_id:
                rows = con.execute(
                    """
                    SELECT *
                    FROM main.technical_signals_fx
                    WHERE run_id = ?
                    ORDER BY pair
                    """,
                    [latest_ag2_run_id],
                ).fetchdf().to_dict("records")
    except Exception as exc:
        ctx["technical_error"] = str(exc)

return [{"json": {**ctx, "technical_signals": rows, "latest_ag2_run_id": latest_ag2_run_id}}]
