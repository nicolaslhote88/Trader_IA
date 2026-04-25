import json
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag4_fx_v1.duckdb"
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of")
sections = ctx.get("digest_sections") or {}

with duckdb.connect(db_path) as con:
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.fx_digest (
          run_id VARCHAR NOT NULL,
          as_of TIMESTAMP NOT NULL,
          section VARCHAR NOT NULL,
          payload VARCHAR NOT NULL,
          items_count INTEGER,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (run_id, section)
        )
        """
    )
    for name, payload in sections.items():
        if name == "top_news":
            cnt = len(payload.get("items") or [])
        elif name == "pair_focus":
            cnt = len((payload.get("pairs") or {}).keys())
        else:
            cnt = 1 if payload else 0
        con.execute(
            "INSERT OR REPLACE INTO main.fx_digest VALUES (?, CAST(? AS TIMESTAMP), ?, ?, ?, CURRENT_TIMESTAMP)",
            [run_id, as_of, name, json.dumps(payload, ensure_ascii=False), cnt],
        )

return [{"json": {**ctx, "sections_written": len(sections)}}]
