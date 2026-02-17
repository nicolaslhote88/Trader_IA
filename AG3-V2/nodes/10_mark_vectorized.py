import duckdb, time, gc
from contextlib import contextmanager

DEFAULT_DB_PATH = "/files/duckdb/ag3_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
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
if not items:
    return []

updates = []
run_ids = set()
db_path = DEFAULT_DB_PATH

for it in items:
    d = it.get("json", {}) or {}
    meta = d.get("metadata") or {}

    run_id = to_text(meta.get("run_id") or d.get("run_id"))
    symbol = to_text(meta.get("symbol") or d.get("symbol")).upper()
    record_id = to_text(meta.get("record_id") or d.get("record_id"))
    vector_id = to_text(d.get("id") or meta.get("id"))

    p = to_text(meta.get("db_path") or d.get("db_path"))
    if p:
        db_path = p

    if run_id and symbol:
        updates.append(
            {
                "run_id": run_id,
                "symbol": symbol,
                "record_id": record_id,
                "vector_id": vector_id,
            }
        )
        run_ids.add(run_id)

if not updates:
    return items

with db_con(db_path) as con:
    for row in updates:
        rid = row["record_id"]
        if rid:
            con.execute(
                """
                UPDATE fundamentals_triage_history
                SET vector_status = 'DONE',
                    vector_id = ?,
                    vectorized_at = CURRENT_TIMESTAMP,
                    updated_row_at = CURRENT_TIMESTAMP
                WHERE record_id = ?
                """,
                [row["vector_id"] or rid, rid],
            )
        else:
            con.execute(
                """
                UPDATE fundamentals_triage_history
                SET vector_status = 'DONE',
                    vector_id = ?,
                    vectorized_at = CURRENT_TIMESTAMP,
                    updated_row_at = CURRENT_TIMESTAMP
                WHERE run_id = ? AND symbol = ?
                """,
                [row["vector_id"] or f"{row['run_id']}|{row['symbol']}", row["run_id"], row["symbol"]],
            )

        con.execute(
            """
            UPDATE fundamentals_snapshot
            SET vector_status = 'DONE',
                vector_id = ?,
                vectorized_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = ? AND symbol = ?
            """,
            [row["vector_id"] or f"{row['run_id']}|{row['symbol']}", row["run_id"], row["symbol"]],
        )

    for run_id in run_ids:
        rec = con.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals_triage_history
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
