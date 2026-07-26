import duckdb
import gc
import time
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"
WORKFLOW_VERSION = "2.0.0"


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


items = _items or []
db_path = DEFAULT_DB_PATH
if len(items) > 0:
    db_path = str((items[0].get("json", {}) or {}).get("db_path") or DEFAULT_DB_PATH)

run_id = f"AG4SPEV2_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

with db_con(db_path) as con:
    if len(items) == 0:
        con.execute(
            """
            INSERT OR REPLACE INTO run_log (
              run_id, started_at, finished_at, status, symbols_total, symbols_ok, symbols_error,
              articles_total, items_analyzed, items_skipped, errors_logged, error_detail, version
            )
            VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'NO_DATA', 0, 0, 0, 0, 0, 0, 0, NULL, ?)
            """,
            [run_id, WORKFLOW_VERSION],
        )
        return []

    con.execute(
        """
        INSERT OR REPLACE INTO run_log (
          run_id, started_at, status, symbols_total, symbols_ok, symbols_error,
          articles_total, items_analyzed, items_skipped, errors_logged, error_detail, version
        )
        VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?, 0, 0, 0, 0, 0, 0, NULL, ?)
        """,
        [run_id, len(items), WORKFLOW_VERSION],
    )

out = []
for it in items:
    j = dict(it.get("json", {}) or {})
    j["run_id"] = run_id
    j["db_path"] = db_path
    out.append({"json": j, "pairedItem": it.get("pairedItem")})

return out

