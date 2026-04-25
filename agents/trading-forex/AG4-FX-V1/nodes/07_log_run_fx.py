import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag4_fx_v1.duckdb"

with duckdb.connect(db_path) as con:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.run_log (
          run_id VARCHAR PRIMARY KEY,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          news_global_pulled INTEGER,
          news_fx_channel_pulled INTEGER,
          news_after_dedupe INTEGER,
          sections_written INTEGER,
          errors INTEGER,
          notes VARCHAR
        )
        """
    )
    errors = int(bool(ctx.get("global_error"))) + int(bool(ctx.get("fx_channel_error")))
    con.execute(
        """
        INSERT OR REPLACE INTO main.run_log VALUES (
          ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            ctx.get("run_id"),
            int(ctx.get("news_global_pulled") or 0),
            int(ctx.get("news_fx_channel_pulled") or 0),
            int(ctx.get("news_after_dedupe") or 0),
            int(ctx.get("sections_written") or 0),
            errors,
            "AG4-FX-V1 digest completed",
        ],
    )

return [{"json": {"run_id": ctx.get("run_id"), "sections_written": ctx.get("sections_written"), "errors": errors}}]
