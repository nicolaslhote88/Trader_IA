import json
import os
import math
import time
from contextlib import contextmanager

import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag4_fx_path") or "/files/duckdb/ag4_fx_v1.duckdb"
payload = {"top_news": [], "pair_focus": {}, "macro_regime": {}}


def is_retryable_duckdb_error(exc):
    msg = str(exc).lower()
    return any(token in msg for token in ("lock", "locked", "conflict", "busy", "timeout"))


@contextmanager
def duckdb_connect_retry(db_path, read_only=True, attempts=10, base_delay=0.35):
    con = None
    for attempt in range(attempts):
        try:
            con = duckdb.connect(db_path, read_only=read_only)
            break
        except Exception as exc:
            if is_retryable_duckdb_error(exc) and attempt < attempts - 1:
                time.sleep(base_delay * (1.7 ** attempt))
                continue
            raise
    try:
        yield con
    finally:
        if con is not None:
            con.close()


def json_safe(value):
    if value is None:
        return None
    try:
        if isinstance(value, float) and not math.isfinite(value):
            return None
    except Exception:
        pass
    try:
        isoformat = value.isoformat
    except Exception:
        return value
    try:
        return isoformat()
    except Exception:
        return str(value)


if os.path.exists(path):
    try:
        with duckdb_connect_retry(path, read_only=True) as con:
            cur = con.execute(
                """
                SELECT section, payload
                FROM main.fx_digest
                WHERE run_id = (SELECT run_id FROM main.run_log ORDER BY finished_at DESC NULLS LAST, started_at DESC LIMIT 1)
                """
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rec = {col: json_safe(value) for col, value in zip(cols, row)}
                section = rec.get("section")
                obj = json.loads(rec.get("payload") or "{}")
                if section == "top_news":
                    payload["top_news"] = obj.get("items") or []
                elif section == "pair_focus":
                    payload["pair_focus"] = obj.get("pairs") or {}
                elif section == "macro_regime":
                    payload["macro_regime"] = obj
    except Exception as exc:
        ctx["macro_news_error"] = str(exc)

return [{"json": {**ctx, "macro_news": payload}}]
