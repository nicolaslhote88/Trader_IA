import duckdb, time, gc, signal
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, date

DB_PATH = "/files/duckdb/ag4_v3.duckdb"
LOOKBACK_DAYS = 10
QUERY_BUDGET_SECONDS = 28
RUN_ID_SCAN_LIMIT = 6
ROW_LIMIT = 500
CANDIDATE_LIMIT = 2000


class NewsFetchTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise NewsFetchTimeout(f"news fetch budget exceeded after {QUERY_BUDGET_SECONDS}s")


@contextmanager
def db_con(path=DB_PATH, retries=2, delay=0.25):
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
    latest_run_ids = []
    try:
        latest_run_ids = [
            str(r[0])
            for r in con.execute(
                """
                SELECT run_id
                FROM run_log
                WHERE run_id IS NOT NULL
                  AND status NOT IN ('RUNNING', 'ERROR', 'FAILED')
                ORDER BY COALESCE(finished_at, started_at) DESC
                LIMIT ?
                """,
                [RUN_ID_SCAN_LIMIT],
            ).fetchall()
            if r and r[0]
        ]
    except Exception:
        latest_run_ids = []

    if latest_run_ids:
        placeholders = ",".join(["?"] * len(latest_run_ids))
        sql_recent_runs = f"""
            SELECT
              COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS published_at_eff,
              impact_score,
              COALESCE(sectors_bullish, winners, '') AS sectors_bullish,
              COALESCE(sectors_bearish, losers, '') AS sectors_bearish,
              COALESCE(winners, '') AS winners,
              COALESCE(losers, '') AS losers
            FROM news_history
            WHERE run_id IN ({placeholders})
              AND COALESCE(type, 'macro') = 'macro'
              AND (
                COALESCE(impact_score, 0) <> 0
                OR COALESCE(sectors_bullish, '') <> ''
                OR COALESCE(sectors_bearish, '') <> ''
                OR COALESCE(winners, '') <> ''
                OR COALESCE(losers, '') <> ''
              )
            ORDER BY published_at_eff DESC, updated_at DESC
            LIMIT ?
        """
        rows = con.execute(sql_recent_runs, [*latest_run_ids, ROW_LIMIT]).fetchall()
        if rows:
            return rows, "sectors_bullish_bearish_recent_runs"

    # Preferred path: use AG4-V3 normalized sector columns (aligned to AG4 universe sectors).
    sql_v2 = """
        WITH recent AS (
          SELECT *
          FROM news_history
          WHERE COALESCE(type, 'macro') = 'macro'
            AND (
              (updated_at IS NOT NULL AND updated_at >= ?)
              OR (last_seen_at IS NOT NULL AND last_seen_at >= ?)
              OR (published_at IS NOT NULL AND published_at >= ?)
              OR (first_seen_at IS NOT NULL AND first_seen_at >= ?)
              OR (analyzed_at IS NOT NULL AND analyzed_at >= ?)
              OR (created_at IS NOT NULL AND created_at >= ?)
            )
          ORDER BY updated_at DESC NULLS LAST
          LIMIT ?
        )
        SELECT
          COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS published_at_eff,
          impact_score,
          COALESCE(sectors_bullish, winners, '') AS sectors_bullish,
          COALESCE(sectors_bearish, losers, '') AS sectors_bearish,
          COALESCE(winners, '') AS winners,
          COALESCE(losers, '') AS losers
        FROM recent
        WHERE COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) >= ?
          AND (
            COALESCE(impact_score, 0) <> 0
            OR COALESCE(sectors_bullish, '') <> ''
            OR COALESCE(sectors_bearish, '') <> ''
            OR COALESCE(winners, '') <> ''
            OR COALESCE(losers, '') <> ''
          )
        ORDER BY published_at_eff DESC, updated_at DESC
        LIMIT ?
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
        LIMIT ?
    """

    try:
        rows = con.execute(sql_v2, [cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, CANDIDATE_LIMIT, cutoff, ROW_LIMIT]).fetchall()
        return rows, "sectors_bullish_bearish"
    except Exception:
        rows = con.execute(sql_legacy, [cutoff, ROW_LIMIT]).fetchall()
        return rows, "winners_losers_legacy"


def degraded_output(reason, detail, db_path, lookback_days):
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
                "_sectorFieldSource": "degraded",
                "_emptyNews": True,
                "_newsFetchDegraded": True,
                "_newsFetchReason": str(reason or ""),
                "_newsFetchDetail": str(detail or "")[:500],
                "_newsFetchDbPath": db_path,
            }
        }
    ]


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

alarm_enabled = hasattr(signal, "SIGALRM") and hasattr(signal, "setitimer")
try:
    if alarm_enabled:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, QUERY_BUDGET_SECONDS)

    with db_con(db_path) as con:
        rows, sector_field_source = fetch_news_rows(con, cutoff)
except Exception as e:
    return degraded_output(type(e).__name__, e, db_path, lookback_days)
finally:
    if alarm_enabled:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        except Exception:
            pass

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
