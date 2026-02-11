import duckdb

DB_PATH = "/files/duckdb/ag2_v2.duckdb"
items = _input.all()

for item in items:
    d = item.json
    signal_id = d.get("metadata", {}).get("signal_id") or d.get("signal_id", "")
    vector_id = d.get("id", "")
    if signal_id:
        con = duckdb.connect(DB_PATH)
        con.execute("UPDATE technical_signals SET vector_status = 'DONE', vector_id = ?, vectorized_at = CURRENT_TIMESTAMP WHERE id = ?", [vector_id, signal_id])
        con.close()

return items
