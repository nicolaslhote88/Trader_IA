import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, date

DB_PATH = "/files/duckdb/ag4_v3.duckdb"
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
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def fetch_news_rows(con, cutoff):
    # Preferred path: use AG4-V3 normalized sector columns (aligned to AG4 universe sectors).
    sql_v2 = """
        SELECT
          COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS published_at_eff,
          impact_score,
          COALESCE(sectors_bullish, winners, '') AS sectors_bullish,
          COALESCE(sectors_bearish, losers, '') AS sectors_bearish,
          COALESCE(winners, '') AS winners,
          COALESCE(losers, '') AS losers
        FROM news_history
        WHERE COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) >= ?
          AND COALESCE(type, 'macro') = 'macro'
          AND (
            COALESCE(impact_score, 0) <> 0
            OR COALESCE(sectors_bullish, '') <> ''
            OR COALESCE(sectors_bearish, '') <> ''
            OR COALESCE(winners, '') <> ''
            OR COALESCE(losers, '') <> ''
          )
        ORDER BY published_at_eff DESC, updated_at DESC
    """

    sql_legacy = """
        SELECT
          COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS published_at_eff,
          impact_score,
          COALESCE(winners, '') AS winners,
          COALESCE(losers, '') AS losers
        FROM news_history
        WHERE COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) >= ?
          AND COALESCE(type, 'macro') = 'macro'
          AND (COALESCE(impact_score, 0) <> 0 OR COALESCE(winners, '') <> '' OR COALESCE(losers, '') <> '')
        ORDER BY published_at_eff DESC, updated_at DESC
    """

    try:
        rows = con.execute(sql_v2, [cutoff]).fetchall()
        return rows, "sectors_bullish_bearish"
    except Exception:
        rows = con.execute(sql_legacy, [cutoff]).fetchall()
        return rows, "winners_losers_legacy"


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
    rows, sector_field_source = fetch_news_rows(con, cutoff)

for row in rows:
    if sector_field_source == "sectors_bullish_bearish":
        sectors_bullish = row[2] or ""
        sectors_bearish = row[3] or ""
        winners = row[4] or sectors_bullish
        losers = row[5] or sectors_bearish
    else:
        sectors_bullish = row[2] or ""
        sectors_bearish = row[3] or ""
        winners = row[2] or ""
        losers = row[3] or ""

    out.append(
        {
            "json": {
                "publishedAt": to_iso(row[0]),
                "ImpactScore": to_num(row[1]),
                "sectors_bullish": sectors_bullish,
                "sectors_bearish": sectors_bearish,
                "Winners": winners,
                "Losers": losers,
                "lookbackDays": lookback_days,
                "_sectorFieldSource": sector_field_source,
            }
        }
    )

if not out:
    return [
        {
            "json": {
                "publishedAt": "",
                "ImpactScore": 0,
                "sectors_bullish": "",
                "sectors_bearish": "",
                "Winners": "",
                "Losers": "",
                "lookbackDays": lookback_days,
                "_sectorFieldSource": "none",
                "_emptyNews": True,
            }
        }
    ]

return out
