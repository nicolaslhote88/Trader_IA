import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = "/files/duckdb/ag4_v3.duckdb"
WORKFLOW_VERSION = "3.0.0"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
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

items = _items or []

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS news_history (
      dedupe_key VARCHAR PRIMARY KEY,
      event_key VARCHAR,
      run_id VARCHAR NOT NULL,
      canonical_url VARCHAR,
      published_at TIMESTAMP,
      title VARCHAR,
      source VARCHAR,
      feed_url VARCHAR,
      symbols VARCHAR,
      type VARCHAR,
      notes VARCHAR,
      impact_score INTEGER,
      confidence DOUBLE,
      urgency VARCHAR,
      snippet VARCHAR,
      first_seen_at TIMESTAMP,
      strategy VARCHAR,
      losers VARCHAR,
      winners VARCHAR,
      sectors_bullish VARCHAR,
      sectors_bearish VARCHAR,
      theme VARCHAR,
      regime VARCHAR,
      analyzed_at TIMESTAMP,
      last_seen_at TIMESTAMP,
      source_tier INTEGER,
      action VARCHAR,
      reason VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS news_errors (
      dedupe_key VARCHAR PRIMARY KEY,
      run_id VARCHAR,
      feed_url VARCHAR,
      http_code INTEGER,
      error_message VARCHAR,
      raw_error VARCHAR,
      occurred_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_log (
      run_id VARCHAR PRIMARY KEY,
      started_at TIMESTAMP NOT NULL,
      finished_at TIMESTAMP,
      status VARCHAR DEFAULT 'RUNNING',
      sources_total INTEGER DEFAULT 0,
      feeds_ok INTEGER DEFAULT 0,
      feeds_error INTEGER DEFAULT 0,
      items_total INTEGER DEFAULT 0,
      items_analyzed INTEGER DEFAULT 0,
      items_skipped INTEGER DEFAULT 0,
      errors_logged INTEGER DEFAULT 0,
      error_detail VARCHAR,
      version VARCHAR DEFAULT '3.0.0'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ag4_fx_macro (
      run_id VARCHAR PRIMARY KEY,
      as_of TIMESTAMP NOT NULL,
      market_regime VARCHAR,
      drivers VARCHAR,
      confidence DOUBLE,
      usd_bias DOUBLE,
      eur_bias DOUBLE,
      jpy_bias DOUBLE,
      gbp_bias DOUBLE,
      chf_bias DOUBLE,
      aud_bias DOUBLE,
      cad_bias DOUBLE,
      nzd_bias DOUBLE,
      bias_json VARCHAR,
      source_window_days INTEGER DEFAULT 7,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ag4_fx_pairs (
      id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      pair VARCHAR NOT NULL,
      symbol_internal VARCHAR,
      directional_bias VARCHAR,
      rationale VARCHAR,
      confidence DOUBLE,
      urgent_event_window BOOLEAN DEFAULT FALSE,
      as_of TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]

run_id = f"AG4V2_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

with db_con() as con:
    for stmt in SCHEMA:
        con.execute(stmt)

    # Lightweight schema migration for existing databases (backward compatible).
    cols = {str(r[1]).lower() for r in con.execute("PRAGMA table_info('news_history')").fetchall()}
    if "sectors_bullish" not in cols:
        con.execute("ALTER TABLE news_history ADD COLUMN sectors_bullish VARCHAR")
    if "sectors_bearish" not in cols:
        con.execute("ALTER TABLE news_history ADD COLUMN sectors_bearish VARCHAR")

    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_news_history_run ON news_history(run_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_news_history_type ON news_history(type)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_fx_pairs_run ON ag4_fx_pairs(run_id)")
    except Exception:
        pass

    con.execute(
        """
        INSERT OR REPLACE INTO run_log (run_id, started_at, status, sources_total, version)
        VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?, ?)
        """,
        [run_id, len(items), WORKFLOW_VERSION],
    )

    if len(items) == 0:
        con.execute(
            """
            UPDATE run_log
            SET finished_at = CURRENT_TIMESTAMP,
                status = 'NO_DATA',
                feeds_ok = 0,
                feeds_error = 0,
                items_total = 0,
                items_analyzed = 0,
                items_skipped = 0,
                errors_logged = 0
            WHERE run_id = ?
            """,
            [run_id],
        )
        return []

out = []
for it in items:
    j = dict(it.get("json", {}) or {})
    j["run_id"] = run_id
    j["db_path"] = DB_PATH
    out.append({"json": j, "pairedItem": it.get("pairedItem")})

return out
