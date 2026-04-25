import duckdb, time, gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()

items = _items

with db_con() as con:
    for it in items:
        d = it.get("json", {}) or {}
        meta = d.get("metadata") or {}
        signal_id = meta.get("signal_id") or d.get("signal_id") or ""
        vector_id = meta.get("doc_id") or meta.get("id") or signal_id or ""

        if signal_id and vector_id:
            con.execute(
                "UPDATE technical_signals SET vector_status = 'DONE', vector_id = ?, vectorized_at = CURRENT_TIMESTAMP WHERE id = ?",
                [vector_id, signal_id],
            )

return items
