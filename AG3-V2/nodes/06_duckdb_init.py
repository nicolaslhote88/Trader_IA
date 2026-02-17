import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_DB_PATH = "/files/duckdb/ag3_v2.duckdb"
WORKFLOW_VERSION = "2.1.0"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
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
first = items[0].get("json", {}) if items else {}
run_id = str(first.get("run_id") or f"AG3V2_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}")
db_path = str(first.get("db_path") or DEFAULT_DB_PATH)

schema_stmts = [
    """
    CREATE TABLE IF NOT EXISTS run_log (
      run_id VARCHAR PRIMARY KEY,
      started_at TIMESTAMP NOT NULL,
      finished_at TIMESTAMP,
      status VARCHAR DEFAULT 'RUNNING',
      symbols_total INTEGER DEFAULT 0,
      symbols_ok INTEGER DEFAULT 0,
      symbols_error INTEGER DEFAULT 0,
      triage_rows INTEGER DEFAULT 0,
      consensus_rows INTEGER DEFAULT 0,
      metric_rows INTEGER DEFAULT 0,
      snapshot_rows INTEGER DEFAULT 0,
      vector_docs_written INTEGER DEFAULT 0,
      error_detail VARCHAR,
      version VARCHAR DEFAULT '2.1.0'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
      snapshot_id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      symbol VARCHAR NOT NULL,
      name VARCHAR,
      sector VARCHAR,
      industry VARCHAR,
      country VARCHAR,
      boursorama_ref VARCHAR,
      as_of_date DATE,
      fetched_at TIMESTAMP,
      status VARCHAR,
      error VARCHAR,
      source VARCHAR,
      source_url VARCHAR,
      data_coverage_pct DOUBLE,
      profile_json VARCHAR,
      price_json VARCHAR,
      valuation_json VARCHAR,
      profitability_json VARCHAR,
      growth_json VARCHAR,
      financial_health_json VARCHAR,
      consensus_json VARCHAR,
      dividends_json VARCHAR,
      vector_status VARCHAR DEFAULT 'PENDING',
      vector_id VARCHAR,
      vectorized_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamentals_triage_history (
      record_id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      updated_at TIMESTAMP,
      fetched_at TIMESTAMP,
      as_of_date DATE,
      status VARCHAR,
      error VARCHAR,
      symbol VARCHAR NOT NULL,
      name VARCHAR,
      sector VARCHAR,
      industry VARCHAR,
      country VARCHAR,
      boursorama_ref VARCHAR,
      source VARCHAR,
      source_url VARCHAR,
      score INTEGER,
      funda_conf INTEGER,
      risk_score INTEGER,
      quality_score INTEGER,
      growth_score INTEGER,
      valuation_score INTEGER,
      health_score INTEGER,
      consensus_score INTEGER,
      horizon VARCHAR,
      current_price DOUBLE,
      target_price DOUBLE,
      upside_pct DOUBLE,
      recommendation VARCHAR,
      analyst_count INTEGER,
      valuation VARCHAR,
      why VARCHAR,
      risks VARCHAR,
      next_steps VARCHAR,
      data_coverage_pct DOUBLE,
      strategy_version VARCHAR,
      config_version VARCHAR,
      vector_status VARCHAR DEFAULT 'PENDING',
      vector_id VARCHAR,
      vectorized_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_row_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS analyst_consensus_history (
      record_id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      updated_at TIMESTAMP,
      as_of_date DATE,
      symbol VARCHAR NOT NULL,
      name VARCHAR,
      sector VARCHAR,
      recommendation VARCHAR,
      recommendation_mean DOUBLE,
      analyst_count INTEGER,
      current_price DOUBLE,
      target_mean_price DOUBLE,
      target_high_price DOUBLE,
      target_low_price DOUBLE,
      upside_pct DOUBLE,
      dispersion_pct DOUBLE,
      confidence_proxy INTEGER,
      risk_proxy INTEGER,
      source VARCHAR,
      source_url VARCHAR,
      status VARCHAR,
      error VARCHAR,
      horizon VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_row_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamental_metrics_history (
      record_id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      extracted_at TIMESTAMP,
      as_of_date DATE,
      symbol VARCHAR NOT NULL,
      boursorama_ref VARCHAR,
      data_type VARCHAR,
      section VARCHAR,
      metric VARCHAR,
      period VARCHAR,
      value_num DOUBLE,
      value_text VARCHAR,
      unit VARCHAR,
      source_url VARCHAR,
      sig_hash VARCHAR,
      title VARCHAR,
      author VARCHAR,
      excerpt VARCHAR,
      raw_text VARCHAR,
      signal VARCHAR,
      score DOUBLE,
      currency VARCHAR,
      title_or_label VARCHAR,
      notes VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_row_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE VIEW IF NOT EXISTS v_latest_triage AS
    SELECT * EXCLUDE(rn)
    FROM (
      SELECT t.*,
             ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY t.updated_at DESC, t.created_at DESC) AS rn
      FROM fundamentals_triage_history t
    )
    WHERE rn = 1
    """,
    """
    CREATE VIEW IF NOT EXISTS v_latest_consensus AS
    SELECT * EXCLUDE(rn)
    FROM (
      SELECT c.*,
             ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY c.updated_at DESC, c.created_at DESC) AS rn
      FROM analyst_consensus_history c
    )
    WHERE rn = 1
    """,
    "ALTER TABLE run_log ADD COLUMN IF NOT EXISTS vector_docs_written INTEGER DEFAULT 0",
    "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS vector_status VARCHAR DEFAULT 'PENDING'",
    "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS vector_id VARCHAR",
    "ALTER TABLE fundamentals_snapshot ADD COLUMN IF NOT EXISTS vectorized_at TIMESTAMP",
    "ALTER TABLE fundamentals_triage_history ADD COLUMN IF NOT EXISTS vector_status VARCHAR DEFAULT 'PENDING'",
    "ALTER TABLE fundamentals_triage_history ADD COLUMN IF NOT EXISTS vector_id VARCHAR",
    "ALTER TABLE fundamentals_triage_history ADD COLUMN IF NOT EXISTS vectorized_at TIMESTAMP",
]

with db_con(db_path) as con:
    for stmt in schema_stmts:
        con.execute(stmt)

    total = len(items)
    if total == 0:
        con.execute(
            """
            INSERT OR REPLACE INTO run_log (
              run_id, started_at, finished_at, status, symbols_total, symbols_ok, symbols_error,
              triage_rows, consensus_rows, metric_rows, snapshot_rows, error_detail, version
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
          triage_rows, consensus_rows, metric_rows, snapshot_rows, error_detail, version
        )
        VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?, 0, 0, 0, 0, 0, 0, NULL, ?)
        """,
        [run_id, total, WORKFLOW_VERSION],
    )

out = []
for it in items:
    j = dict(it.get("json", {}) or {})
    j["run_id"] = run_id
    j["db_path"] = db_path
    out.append({"json": j, "pairedItem": it.get("pairedItem")})

return out
