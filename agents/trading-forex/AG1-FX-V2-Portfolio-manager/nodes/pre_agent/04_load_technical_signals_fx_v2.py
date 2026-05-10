import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"
rows = []
if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            rows = con.execute(
                """
                SELECT *
                FROM main.technical_signals_fx
                WHERE run_id = (SELECT run_id FROM main.run_log ORDER BY finished_at DESC NULLS LAST, started_at DESC LIMIT 1)
                ORDER BY pair
                """
            ).fetchdf().to_dict("records")
    except Exception as exc:
        ctx["technical_error"] = str(exc)

return [{"json": {**ctx, "technical_signals": rows}}]
