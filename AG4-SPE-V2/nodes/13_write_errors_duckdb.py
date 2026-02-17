import duckdb
import gc
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone

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
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def to_text(v):
    if v is None:
        return ""
    return str(v)


def to_int(v, d=None):
    try:
        if v is None or v == "":
            return d
        return int(float(v))
    except Exception:
        return d


def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        if isinstance(v, date) and not isinstance(v, datetime):
            return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
        return v
    s = str(v).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


items = _items or []
if len(items) == 0:
    return []

db_path = str((items[0].get("json", {}) or {}).get("db_path") or DEFAULT_DB_PATH)

with db_con(db_path) as con:
    for it in items:
        j = dict(it.get("json", {}) or {})
        error_id = to_text(j.get("errorId", "")).strip()
        if not error_id:
            continue

        run_id = to_text(j.get("run_id", ""))
        stage = to_text(j.get("stage", "unknown"))
        symbol = to_text(j.get("symbol", ""))
        company_name = to_text(j.get("companyName", ""))
        url = to_text(j.get("url", ""))
        http_code = to_int(j.get("httpCode"), None)
        message = to_text(j.get("message", "unknown_error"))
        raw_error = to_text(j.get("rawError", ""))
        occurred_at = parse_ts(j.get("occurredAt")) or datetime.now(timezone.utc)

        con.execute(
            """
            INSERT OR REPLACE INTO news_errors (
              error_id, run_id, stage, symbol, company_name, url,
              http_code, message, raw_error, occurred_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                error_id,
                run_id,
                stage,
                symbol,
                company_name,
                url,
                http_code,
                message,
                raw_error,
                occurred_at,
            ],
        )

return items

