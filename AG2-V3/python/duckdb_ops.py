"""
AG2-V3 DuckDB Operations
=========================
All DuckDB read/write operations for the AG2 pipeline.
Used inline in n8n Python Code nodes.
Path: /files/duckdb/ag2_v3.duckdb (mounted in task-runners)
"""

import duckdb
import json
import threading
import time
from datetime import datetime, timezone

DB_PATH = "/files/duckdb/ag2_v3.duckdb"
_CONNECT_TIMEOUT = 30
_CONNECT_RETRIES = 5
_CONNECT_DELAY = 0.3


def _duckdb_connect_timeout(path, read_only=False, timeout=_CONNECT_TIMEOUT):
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


def _connect_with_retry(path=DB_PATH, read_only=False):
    """Connect to DuckDB with timeout + exponential backoff retry on lock."""
    last_exc = None
    for attempt in range(_CONNECT_RETRIES):
        try:
            return _duckdb_connect_timeout(path, read_only=read_only)
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if ("lock" in msg or "timeout" in msg) and attempt < _CONNECT_RETRIES - 1:
                time.sleep(_CONNECT_DELAY * (2 ** attempt))
            else:
                raise
    raise last_exc

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS universe (
    symbol VARCHAR PRIMARY KEY, name VARCHAR, asset_class VARCHAR DEFAULT 'Equity',
    exchange VARCHAR DEFAULT 'Euronext Paris', currency VARCHAR DEFAULT 'EUR',
    country VARCHAR, sector VARCHAR, industry VARCHAR, isin VARCHAR,
    enabled BOOLEAN DEFAULT TRUE, boursorama_ref VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS technical_signals (
    id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, symbol VARCHAR NOT NULL,
    workflow_date TIMESTAMP NOT NULL,
    h1_date TIMESTAMP, h1_source VARCHAR, h1_status VARCHAR, h1_warnings VARCHAR,
    h1_action VARCHAR, h1_score INTEGER, h1_confidence DOUBLE, h1_rationale VARCHAR,
    d1_date TIMESTAMP, d1_source VARCHAR, d1_status VARCHAR, d1_warnings VARCHAR,
    d1_action VARCHAR, d1_score INTEGER, d1_confidence DOUBLE, d1_rationale VARCHAR,
    last_close DOUBLE,
    h1_sma20 DOUBLE, h1_sma50 DOUBLE, h1_sma200 DOUBLE, h1_ema12 DOUBLE, h1_ema26 DOUBLE,
    h1_macd DOUBLE, h1_macd_signal DOUBLE, h1_macd_hist DOUBLE, h1_rsi14 DOUBLE,
    h1_volatility DOUBLE, h1_last_close DOUBLE, h1_atr DOUBLE, h1_atr_pct DOUBLE,
    h1_bb_upper DOUBLE, h1_bb_lower DOUBLE, h1_bb_width DOUBLE,
    h1_stoch_k DOUBLE, h1_stoch_d DOUBLE, h1_adx DOUBLE, h1_obv_slope DOUBLE,
    h1_resistance DOUBLE, h1_support DOUBLE, h1_dist_res_pct DOUBLE, h1_dist_sup_pct DOUBLE,
    d1_sma20 DOUBLE, d1_sma50 DOUBLE, d1_sma200 DOUBLE, d1_ema12 DOUBLE, d1_ema26 DOUBLE,
    d1_macd DOUBLE, d1_macd_signal DOUBLE, d1_macd_hist DOUBLE, d1_rsi14 DOUBLE,
    d1_volatility DOUBLE, d1_last_close DOUBLE, d1_atr DOUBLE, d1_atr_pct DOUBLE,
    d1_bb_upper DOUBLE, d1_bb_lower DOUBLE, d1_bb_width DOUBLE,
    d1_stoch_k DOUBLE, d1_stoch_d DOUBLE, d1_adx DOUBLE, d1_obv_slope DOUBLE,
    d1_resistance DOUBLE, d1_support DOUBLE, d1_dist_res_pct DOUBLE, d1_dist_sup_pct DOUBLE,
    filter_reason VARCHAR, pass_ai BOOLEAN DEFAULT FALSE, pass_pm BOOLEAN DEFAULT FALSE,
    sig_hash VARCHAR, call_ai BOOLEAN DEFAULT FALSE, dedup_reason VARCHAR,
    ai_decision VARCHAR, ai_validated BOOLEAN, ai_quality INTEGER, ai_reasoning VARCHAR,
    ai_chart_pattern VARCHAR, ai_stop_loss DOUBLE, ai_stop_basis VARCHAR,
    ai_bias_sma200 VARCHAR, ai_regime_d1 VARCHAR, ai_alignment VARCHAR,
    ai_bb_status VARCHAR, ai_rsi_status VARCHAR,
    ai_missing VARCHAR, ai_anomalies VARCHAR, ai_output_ref VARCHAR, ai_rr_theoretical DOUBLE,
    vector_status VARCHAR DEFAULT 'PENDING', vector_id VARCHAR,
    vectorized_at TIMESTAMP, row_hash VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ai_dedup_cache (
    symbol VARCHAR NOT NULL, interval_key VARCHAR NOT NULL,
    sig_hash VARCHAR NOT NULL, sig_json VARCHAR,
    last_ai_at TIMESTAMP, last_ai_run_id VARCHAR, last_ai_reason VARCHAR,
    last_ai_output_ref VARCHAR, ttl_minutes INTEGER DEFAULT 240,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, interval_key)
);
CREATE TABLE IF NOT EXISTS run_log (
    run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP, status VARCHAR DEFAULT 'RUNNING',
    batch_start INTEGER, batch_size INTEGER, total_pool INTEGER,
    symbols_ok INTEGER DEFAULT 0, symbols_error INTEGER DEFAULT 0,
    ai_calls INTEGER DEFAULT 0, vectors_written INTEGER DEFAULT 0,
    error_detail VARCHAR, version VARCHAR DEFAULT '2.0.0'
);
CREATE INDEX IF NOT EXISTS idx_ts_symbol ON technical_signals(symbol);
CREATE INDEX IF NOT EXISTS idx_ts_run ON technical_signals(run_id);
CREATE INDEX IF NOT EXISTS idx_ts_vector ON technical_signals(vector_status);
"""


def init_schema():
    """Create all tables if they don't exist."""
    con = _connect_with_retry(DB_PATH)
    for stmt in SCHEMA_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    con.close()


def create_run(run_id: str, batch_start: int, batch_size: int, total_pool: int):
    """Insert a new run log entry."""
    con = _connect_with_retry(DB_PATH)
    con.execute("""
        INSERT OR REPLACE INTO run_log (run_id, started_at, batch_start, batch_size, total_pool)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
    """, [run_id, batch_start, batch_size, total_pool])
    con.close()


def get_dedup_cache(symbol: str) -> dict:
    """Get cached dedup entry for a symbol."""
    con = _connect_with_retry(DB_PATH)
    result = con.execute(
        "SELECT * FROM ai_dedup_cache WHERE symbol = ? AND interval_key = 'combined'",
        [symbol]
    ).fetchone()
    con.close()
    if result is None:
        return {}
    cols = ["symbol", "interval_key", "sig_hash", "sig_json", "last_ai_at",
            "last_ai_run_id", "last_ai_reason", "last_ai_output_ref",
            "ttl_minutes", "updated_at"]
    return dict(zip(cols, result))


def write_signal(data: dict):
    """Upsert a technical signal row."""
    con = _connect_with_retry(DB_PATH)
    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO technical_signals ({col_names}) VALUES ({placeholders})"
    con.execute(sql, list(data.values()))
    con.close()


def write_dedup_cache(symbol: str, sig_hash: str, sig_json: str,
                      run_id: str, reason: str, output_ref: str, ttl: int):
    """Upsert dedup cache entry."""
    con = _connect_with_retry(DB_PATH)
    con.execute("""
        INSERT OR REPLACE INTO ai_dedup_cache
        (symbol, interval_key, sig_hash, sig_json, last_ai_at, last_ai_run_id,
         last_ai_reason, last_ai_output_ref, ttl_minutes, updated_at)
        VALUES (?, 'combined', ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [symbol, sig_hash, sig_json, run_id, reason, output_ref, ttl])
    con.close()


def update_ai_result(signal_id: str, ai_data: dict):
    """Update AI fields on an existing signal row."""
    con = _connect_with_retry(DB_PATH)
    sets = ", ".join(f"{k} = ?" for k in ai_data.keys())
    sql = f"UPDATE technical_signals SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
    vals = list(ai_data.values()) + [signal_id]
    con.execute(sql, vals)
    con.close()


def mark_vectorized(signal_id: str, vector_id: str):
    """Mark a signal as vectorized in DuckDB."""
    con = _connect_with_retry(DB_PATH)
    con.execute("""
        UPDATE technical_signals
        SET vector_status = 'DONE', vector_id = ?, vectorized_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, [vector_id, signal_id])
    con.close()


def finalize_run(run_id: str, symbols_ok: int, symbols_error: int,
                 ai_calls: int, vectors_written: int, error_detail: str = None):
    """Finalize run log entry."""
    status = "SUCCESS" if symbols_error == 0 else "PARTIAL"
    if symbols_ok == 0:
        status = "FAILED"
    con = _connect_with_retry(DB_PATH)
    con.execute("""
        UPDATE run_log
        SET finished_at = CURRENT_TIMESTAMP, status = ?,
            symbols_ok = ?, symbols_error = ?, ai_calls = ?,
            vectors_written = ?, error_detail = ?
        WHERE run_id = ?
    """, [status, symbols_ok, symbols_error, ai_calls, vectors_written,
          error_detail, run_id])
    con.close()


def sync_universe(rows: list):
    """Sync universe table from Google Sheets data."""
    con = _connect_with_retry(DB_PATH)
    for r in rows:
        con.execute("""
            INSERT OR REPLACE INTO universe (symbol, name, asset_class, exchange, currency,
                country, sector, industry, isin, enabled, boursorama_ref, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            r.get("Symbol", ""), r.get("Name", ""), r.get("AssetClass", "Equity"),
            r.get("Exchange", "Euronext Paris"), r.get("Currency", "EUR"),
            r.get("Country", ""), r.get("Sector", ""), r.get("Industry", ""),
            r.get("ISIN", ""), r.get("Enabled", "true").lower() == "true",
            r.get("BoursoramaRef", ""),
        ])
    con.close()
