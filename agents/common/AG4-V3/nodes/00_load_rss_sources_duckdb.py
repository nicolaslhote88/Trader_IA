import duckdb
import time
from datetime import datetime

DB_PATH = "/files/duckdb/ag4_v3.duckdb"


def connect(path=DB_PATH, retries=36, delay=10):
    last = None
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as e:
            last = e
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise Exception(f"Unable to open AG4 config DuckDB: {last}")


con = connect()
try:
    exists = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'cfg'
          AND table_name = 'ag4_rss_sources'
        """
    ).fetchone()[0]
    if not exists:
        raise Exception("Missing cfg.ag4_rss_sources in ag4_v3.duckdb")

    rows = con.execute(
        """
        SELECT
          source_id,
          family,
          source,
          feed_name,
          url,
          interest,
          source_tier,
          enabled
        FROM cfg.ag4_rss_sources
        WHERE enabled = TRUE
        ORDER BY source_tier ASC, source_id ASC
        """
    ).fetchall()
finally:
    con.close()

if not rows:
    raise Exception("cfg.ag4_rss_sources contains no enabled RSS source")

fetched_at = datetime.utcnow().isoformat() + "Z"
out = []
for row in rows:
    out.append({
        "json": {
            "enabled": bool(row[7]),
            "family": row[1] or "unknown",
            "source": row[2] or "unknown",
            "feedName": row[3] or "unknown",
            "url": row[4] or "",
            "interest": int(row[5] or 0),
            "sourceTier": int(row[6] or 3),
            "sourceId": row[0] or "unknown",
            "fetchedAt": fetched_at,
        }
    })

return out
