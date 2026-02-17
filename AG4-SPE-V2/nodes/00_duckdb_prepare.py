import duckdb
import gc
import time
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
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


def table_exists(con, table_name):
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


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
      is_relevant BOOLEAN,
      relevance_reason VARCHAR,
      action VARCHAR,
      reason VARCHAR,
      status VARCHAR,
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
      error_detail VARCHAR,
      version VARCHAR DEFAULT '2.0.0'
    )
    """,
]


with db_con() as con:
    for stmt in SCHEMA:
        con.execute(stmt)

    rows = []

    if table_exists(con, "universe_symbols"):
        rows = con.execute(
            """
            SELECT
              symbol,
              COALESCE(name, symbol) AS name,
              isin,
              COALESCE(asset_class, 'Equity') AS asset_class,
              COALESCE(exchange, 'Euronext Paris') AS exchange,
              COALESCE(currency, 'EUR') AS currency,
              country,
              COALESCE(enabled, TRUE) AS enabled,
              boursorama_ref,
              notes_json
            FROM universe_symbols
            WHERE COALESCE(enabled, TRUE) = TRUE
            ORDER BY symbol
            """
        ).fetchall()

    # Fallback for AG2 schema if universe_symbols is still empty.
    if len(rows) == 0 and table_exists(con, "universe"):
        rows = con.execute(
            """
            SELECT
              symbol,
              COALESCE(name, symbol) AS name,
              isin,
              COALESCE(asset_class, 'Equity') AS asset_class,
              COALESCE(exchange, 'Euronext Paris') AS exchange,
              COALESCE(currency, 'EUR') AS currency,
              country,
              COALESCE(enabled, TRUE) AS enabled,
              boursorama_ref,
              NULL AS notes_json
            FROM universe
            WHERE COALESCE(enabled, TRUE) = TRUE
            ORDER BY symbol
            """
        ).fetchall()

out = []
for row in rows:
    out.append(
        {
            "json": {
                "symbol": str(row[0] or "").strip().upper(),
                "companyName": str(row[1] or "").strip(),
                "isin": str(row[2] or "").strip(),
                "assetClass": str(row[3] or "").strip(),
                "exchange": str(row[4] or "").strip(),
                "currency": str(row[5] or "").strip(),
                "country": str(row[6] or "").strip(),
                "enabled": bool(row[7]) if row[7] is not None else True,
                "boursoramaRef": str(row[8] or "").strip(),
                "notesJson": str(row[9] or "").strip(),
                "sourceUniverse": "duckdb",
                "db_path": DB_PATH,
            }
        }
    )

return out

