import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, date

DB_PATH = "/files/duckdb/ag4_v2.duckdb"
LOOKBACK_DAYS = 20

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
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

def to_iso(v):
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)

def to_num(v):
    try:
        if v is None or v == "":
            return 0
        return float(v)
    except Exception:
        return 0

items = _items or []
db_path = DB_PATH
lookback_days = LOOKBACK_DAYS

for it in items:
    j = it.get("json", {}) or {}
    if j.get("db_path"):
        db_path = str(j.get("db_path"))
    if j.get("lookbackDays") is not None:
        try:
            lookback_days = max(1, int(float(j.get("lookbackDays"))))
        except Exception:
            pass

cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
out = []

with db_con(db_path) as con:
    rows = con.execute(
        """
        SELECT
          COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS published_at_eff,
          impact_score,
          winners,
          losers
        FROM news_history
        WHERE COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) >= ?
          AND (COALESCE(impact_score, 0) <> 0 OR COALESCE(winners, '') <> '' OR COALESCE(losers, '') <> '')
        ORDER BY published_at_eff DESC, updated_at DESC
        """,
        [cutoff],
    ).fetchall()

for row in rows:
    out.append({
        "json": {
            "publishedAt": to_iso(row[0]),
            "ImpactScore": to_num(row[1]),
            "Winners": row[2] or "",
            "Losers": row[3] or "",
        }
    })

if not out:
    return [{"json": {"publishedAt": "", "ImpactScore": 0, "Winners": "", "Losers": "", "_emptyNews": True}}]

return out
