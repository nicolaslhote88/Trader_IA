import duckdb
from datetime import datetime, timezone

DB_PATH = "/files/duckdb/ag4_forex_v1.duckdb"


ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = str(ctx.get("db_path") or DB_PATH)
dedupe_key = str(ctx.get("dedupeKey") or "").strip()
now = datetime.now(timezone.utc)

if not dedupe_key:
    return [{"json": {**ctx, "_action": "analyze", "_reason": "missing_dedupe_key"}}]

try:
    with duckdb.connect(db_path) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        tables = {
            r[0].lower()
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if "fx_news_history" not in tables:
            return [{"json": {**ctx, "_action": "analyze", "_reason": "history_table_missing"}}]

        row = con.execute(
            """
            SELECT dedupe_key
            FROM main.fx_news_history
            WHERE dedupe_key = ?
            LIMIT 1
            """,
            [dedupe_key],
        ).fetchone()

        if not row:
            return [{"json": {**ctx, "_action": "analyze", "_reason": "new_dedupe_key"}}]

        con.execute(
            """
            UPDATE main.fx_news_history
            SET last_seen_at = ?, updated_at = CURRENT_TIMESTAMP
            WHERE dedupe_key = ?
            """,
            [now, dedupe_key],
        )
        return [{"json": {**ctx, "_action": "skip", "_reason": "dedupe_key_seen"}}]
except Exception as exc:
    return [{"json": {**ctx, "_action": "analyze", "_reason": "dedupe_check_error", "_dedupe_error": str(exc)}}]
