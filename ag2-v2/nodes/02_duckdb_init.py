import duckdb, time, gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v2.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    """Open DuckDB with retry on lock, auto-close + gc on exit."""
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

SCHEMA_STMTS = [
    "CREATE TABLE IF NOT EXISTS universe (symbol VARCHAR PRIMARY KEY, name VARCHAR, asset_class VARCHAR DEFAULT 'Equity', exchange VARCHAR DEFAULT 'Euronext Paris', currency VARCHAR DEFAULT 'EUR', country VARCHAR, sector VARCHAR, industry VARCHAR, isin VARCHAR, enabled BOOLEAN DEFAULT TRUE, boursorama_ref VARCHAR, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS technical_signals (id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, symbol VARCHAR NOT NULL, workflow_date TIMESTAMP NOT NULL, h1_date TIMESTAMP, h1_source VARCHAR, h1_status VARCHAR, h1_warnings VARCHAR, h1_action VARCHAR, h1_score INTEGER, h1_confidence DOUBLE, h1_rationale VARCHAR, d1_date TIMESTAMP, d1_source VARCHAR, d1_status VARCHAR, d1_warnings VARCHAR, d1_action VARCHAR, d1_score INTEGER, d1_confidence DOUBLE, d1_rationale VARCHAR, last_close DOUBLE, h1_sma20 DOUBLE, h1_sma50 DOUBLE, h1_sma200 DOUBLE, h1_ema12 DOUBLE, h1_ema26 DOUBLE, h1_macd DOUBLE, h1_macd_signal DOUBLE, h1_macd_hist DOUBLE, h1_rsi14 DOUBLE, h1_volatility DOUBLE, h1_last_close DOUBLE, h1_atr DOUBLE, h1_atr_pct DOUBLE, h1_bb_upper DOUBLE, h1_bb_lower DOUBLE, h1_bb_width DOUBLE, h1_stoch_k DOUBLE, h1_stoch_d DOUBLE, h1_adx DOUBLE, h1_obv_slope DOUBLE, h1_resistance DOUBLE, h1_support DOUBLE, h1_dist_res_pct DOUBLE, h1_dist_sup_pct DOUBLE, d1_sma20 DOUBLE, d1_sma50 DOUBLE, d1_sma200 DOUBLE, d1_ema12 DOUBLE, d1_ema26 DOUBLE, d1_macd DOUBLE, d1_macd_signal DOUBLE, d1_macd_hist DOUBLE, d1_rsi14 DOUBLE, d1_volatility DOUBLE, d1_last_close DOUBLE, d1_atr DOUBLE, d1_atr_pct DOUBLE, d1_bb_upper DOUBLE, d1_bb_lower DOUBLE, d1_bb_width DOUBLE, d1_stoch_k DOUBLE, d1_stoch_d DOUBLE, d1_adx DOUBLE, d1_obv_slope DOUBLE, d1_resistance DOUBLE, d1_support DOUBLE, d1_dist_res_pct DOUBLE, d1_dist_sup_pct DOUBLE, filter_reason VARCHAR, pass_ai BOOLEAN DEFAULT FALSE, pass_pm BOOLEAN DEFAULT FALSE, sig_hash VARCHAR, call_ai BOOLEAN DEFAULT FALSE, dedup_reason VARCHAR, ai_decision VARCHAR, ai_validated BOOLEAN, ai_quality INTEGER, ai_reasoning VARCHAR, ai_chart_pattern VARCHAR, ai_stop_loss DOUBLE, ai_stop_basis VARCHAR, ai_bias_sma200 VARCHAR, ai_regime_d1 VARCHAR, ai_alignment VARCHAR, ai_missing VARCHAR, ai_anomalies VARCHAR, ai_output_ref VARCHAR, ai_rr_theoretical DOUBLE, vector_status VARCHAR DEFAULT 'PENDING', vector_id VARCHAR, vectorized_at TIMESTAMP, row_hash VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS ai_dedup_cache (symbol VARCHAR NOT NULL, interval_key VARCHAR NOT NULL, sig_hash VARCHAR NOT NULL, sig_json VARCHAR, last_ai_at TIMESTAMP, last_ai_run_id VARCHAR, last_ai_reason VARCHAR, last_ai_output_ref VARCHAR, ttl_minutes INTEGER DEFAULT 240, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (symbol, interval_key))",
    "CREATE TABLE IF NOT EXISTS run_log (run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, status VARCHAR DEFAULT 'RUNNING', batch_start INTEGER, batch_size INTEGER, total_pool INTEGER, symbols_ok INTEGER DEFAULT 0, symbols_error INTEGER DEFAULT 0, ai_calls INTEGER DEFAULT 0, vectors_written INTEGER DEFAULT 0, error_detail VARCHAR, version VARCHAR DEFAULT '2.0.0')",
]

items = _items
first_json = items[0].get("json", {}) if items else {}

with db_con() as con:
    for stmt in SCHEMA_STMTS:
        con.execute(stmt)

    universe = first_json.get("_universe", []) or []
    for r in universe:
        sym = str(r.get("Symbol", r.get("symbol", "")) or "").strip()
        if not sym:
            continue
        con.execute(
            "INSERT OR REPLACE INTO universe (symbol, name, asset_class, exchange, currency, country, sector, industry, isin, enabled, boursorama_ref, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            [sym, r.get("Name", ""), r.get("AssetClass", "Equity"), r.get("Exchange", "Euronext Paris"), r.get("Currency", "EUR"), r.get("Country", ""), r.get("Sector", ""), r.get("Industry", ""), r.get("ISIN", ""), str(r.get("Enabled", "true")).lower() == "true", r.get("BoursoramaRef", "")],
        )

    run_id = str(first_json.get("run_id", "UNKNOWN"))
    batch_info = first_json.get("batch_info", {}) or {}
    con.execute(
        "INSERT OR REPLACE INTO run_log (run_id, started_at, batch_start, batch_size, total_pool) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)",
        [run_id, int(batch_info.get("start", 0) or 0), int(batch_info.get("size", 0) or 0), int(batch_info.get("total", 0) or 0)],
    )

out = []
for it in items:
    d = dict(it.get("json", {}))
    d.pop("_universe", None)
    out.append({"json": d})

return out
