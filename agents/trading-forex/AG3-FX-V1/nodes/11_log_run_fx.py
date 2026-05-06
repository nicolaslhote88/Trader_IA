import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag3_fx_v1.duckdb"

errors = 0
for key in ("universe_error", "technical_error", "macro_news_error"):
    errors += int(bool(ctx.get(key)))
errors += len(ctx.get("macro_fetch_errors") or [])

notes = "AG3-FX-V1 completed"
if ctx.get("macro_data_degraded"):
    notes += " with DEGRADED_MACRO_DATA"

with duckdb.connect(db_path) as con:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.run_log (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            pairs_total INTEGER,
            pairs_scored INTEGER,
            currencies_scored INTEGER,
            macro_observations INTEGER,
            target_rows INTEGER,
            errors INTEGER,
            notes VARCHAR,
            workflow_version VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT OR REPLACE INTO main.run_log VALUES (
            ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
        )
        """,
        [
            ctx.get("run_id"),
            "DEGRADED" if ctx.get("macro_data_degraded") else "OK",
            int(ctx.get("pairs_total") or len(ctx.get("universe_fx") or [])),
            int(ctx.get("pairs_scored") or len(ctx.get("pair_fundamental_scores") or [])),
            int(ctx.get("currencies_scored") or len(ctx.get("currency_scores") or [])),
            int(len(ctx.get("macro_observations") or [])),
            int(ctx.get("target_rows") or len(ctx.get("equilibrium_targets") or [])),
            int(errors),
            notes,
            ctx.get("workflow_version") or "AG3-FX-V1",
        ],
    )

return [{
    "json": {
        "run_id": ctx.get("run_id"),
        "status": "DEGRADED" if ctx.get("macro_data_degraded") else "OK",
        "pairs_scored": int(ctx.get("pairs_scored") or 0),
        "target_rows": int(ctx.get("target_rows") or 0),
        "errors": int(errors),
    }
}]
