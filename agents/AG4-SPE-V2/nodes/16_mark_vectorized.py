import duckdb
import gc
import time
from contextlib import contextmanager

DEFAULT_DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
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


def to_text(v):
    if v is None:
        return ""
    return str(v).strip()


items = _items or []
if len(items) == 0:
    return []

db_path = DEFAULT_DB_PATH
agg = {}
run_ids = set()

for it in items:
    d = it.get("json", {}) or {}
    meta = d.get("metadata") or {}

    news_id = to_text(meta.get("news_id") or d.get("news_id"))
    run_id = to_text(meta.get("run_id") or d.get("run_id"))
    vector_id = to_text(meta.get("doc_id") or meta.get("id") or news_id)
    local_db_path = to_text(meta.get("db_path") or d.get("db_path"))

    if local_db_path:
        db_path = local_db_path
    if not news_id:
        continue

    row = agg.get(news_id) or {"run_id": run_id, "vector_id": vector_id, "chunk_total": 0}
    row["chunk_total"] += 1
    if vector_id and not row.get("vector_id"):
        row["vector_id"] = vector_id
    if run_id:
        row["run_id"] = run_id
    agg[news_id] = row

    if run_id:
        run_ids.add(run_id)

if len(agg) == 0:
    return items

with db_con(db_path) as con:
    for news_id, data in agg.items():
        con.execute(
            """
            UPDATE news_history
            SET vector_status = 'DONE',
                vector_id = COALESCE(NULLIF(?, ''), vector_id),
                chunk_total = ?,
                vectorized_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE news_id = ?
            """,
            [to_text(data.get("vector_id")), int(data.get("chunk_total") or 0), news_id],
        )

    for run_id in run_ids:
        rec = con.execute(
            """
            SELECT COUNT(*)
            FROM news_history
            WHERE run_id = ? AND vector_status = 'DONE'
            """,
            [run_id],
        ).fetchone()
        done_count = int(rec[0]) if rec else 0
        con.execute(
            "UPDATE run_log SET vector_docs_written = ? WHERE run_id = ?",
            [done_count, run_id],
        )

return items
