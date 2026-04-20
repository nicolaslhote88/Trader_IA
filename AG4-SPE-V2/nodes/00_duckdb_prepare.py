import duckdb
import gc
import threading
import time
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


def _duckdb_connect_timeout(path, read_only=False, timeout=30):
    """Wrap duckdb.connect() with a timeout to avoid indefinite blocking on file locks."""
    result = [None]
    exc = [None]

    def _connect():
        try:
            result[0] = duckdb.connect(path, read_only=read_only)
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_connect, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise Exception(f"duckdb lock timeout: {path} verrouille depuis >{timeout}s")
    if exc[0] is not None:
        raise exc[0]
    return result[0]


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = _duckdb_connect_timeout(path, timeout=30)
            break
        except Exception as exc:
            if ("lock" in str(exc).lower() or "timeout" in str(exc).lower()) and attempt < retries - 1:
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


SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS universe_symbols (
      symbol VARCHAR PRIMARY KEY,
      name VARCHAR,
      isin VARCHAR,
      asset_class VARCHAR,
      exchange VARCHAR,
      currency VARCHAR,
      country VARCHAR,
      enabled BOOLEAN DEFAULT TRUE,
      boursorama_ref VARCHAR,
      notes_json VARCHAR,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS news_history (
      news_id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      symbol VARCHAR NOT NULL,
      company_name VARCHAR,
      source VARCHAR,
      boursorama_ref VARCHAR,
      listing_url VARCHAR,
      url VARCHAR,
      canonical_url VARCHAR,
      title VARCHAR,
      published_at TIMESTAMP,
      published_at_raw VARCHAR,
      snippet VARCHAR,
      text VARCHAR,
      summary VARCHAR,
      category VARCHAR,
      impact_score INTEGER,
      sentiment VARCHAR,
      confidence_score INTEGER,
      horizon VARCHAR,
      urgency VARCHAR,
      suggested_signal VARCHAR,
      key_drivers VARCHAR,
      needs_follow_up BOOLEAN,
      is_relevant BOOLEAN,
      relevance_reason VARCHAR,
      action VARCHAR,
      reason VARCHAR,
      status VARCHAR,
      vector_status VARCHAR DEFAULT 'PENDING',
      vector_id VARCHAR,
      vectorized_at TIMESTAMP,
      chunk_total INTEGER,
      first_seen_at TIMESTAMP,
      last_seen_at TIMESTAMP,
      analyzed_at TIMESTAMP,
      fetched_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS news_errors (
      error_id VARCHAR PRIMARY KEY,
      run_id VARCHAR,
      stage VARCHAR,
      symbol VARCHAR,
      company_name VARCHAR,
      url VARCHAR,
      http_code INTEGER,
      message VARCHAR,
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
      symbols_total INTEGER DEFAULT 0,
      symbols_ok INTEGER DEFAULT 0,
      symbols_error INTEGER DEFAULT 0,
      articles_total INTEGER DEFAULT 0,
      items_analyzed INTEGER DEFAULT 0,
      items_skipped INTEGER DEFAULT 0,
      errors_logged INTEGER DEFAULT 0,
      vector_docs_written INTEGER DEFAULT 0,
      error_detail VARCHAR,
      version VARCHAR DEFAULT '2.0.0'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_state (
      state_key VARCHAR PRIMARY KEY,
      state_value VARCHAR,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]


with db_con() as con:
    for stmt in SCHEMA:
        con.execute(stmt)

    # Lightweight migrations for existing DB files.
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS confidence_score INTEGER")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS horizon VARCHAR")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS urgency VARCHAR")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS suggested_signal VARCHAR")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS key_drivers VARCHAR")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS needs_follow_up BOOLEAN")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS vector_status VARCHAR DEFAULT 'PENDING'")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS vector_id VARCHAR")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS vectorized_at TIMESTAMP")
    con.execute("ALTER TABLE news_history ADD COLUMN IF NOT EXISTS chunk_total INTEGER")
    con.execute("ALTER TABLE run_log ADD COLUMN IF NOT EXISTS vector_docs_written INTEGER DEFAULT 0")

out = []
for it in (_items or []):
    j = dict(it.get("json", {}) or {})
    j["db_path"] = DB_PATH
    out.append({"json": j, "pairedItem": it.get("pairedItem")})

return out
