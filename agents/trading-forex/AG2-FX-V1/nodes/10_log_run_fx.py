import duckdb

items = _items or []
if not items:
    return [{"json": {"status": "NO_ITEMS"}}]

first = items[0].get("json", {}) or {}
db_path = first.get("db_path") or "/files/duckdb/ag2_fx_v1.duckdb"
run_id = first.get("run_id") or ""
pairs_fetched = sum(1 for it in items if not (it.get("json", {}) or {}).get("fetch_error"))
pairs_with_signal = sum(1 for it in items if (it.get("json", {}) or {}).get("signal_label"))
errors = sum(1 for it in items if (it.get("json", {}) or {}).get("fetch_error"))

with duckdb.connect(db_path) as con:
    con.execute(
        """
        INSERT OR REPLACE INTO main.run_log
        VALUES (?, COALESCE((SELECT started_at FROM main.run_log WHERE run_id = ?), CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP, ?, ?, ?, ?)
        """,
        [run_id, run_id, pairs_fetched, pairs_with_signal, errors, "AG2-FX-V1 completed"],
    )

return [{"json": {"run_id": run_id, "pairs_fetched": pairs_fetched, "pairs_with_signal": pairs_with_signal, "errors": errors}}]
