import gc
import json
import os
import duckdb
import time
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_DB_PATH = "/files/duckdb/ag2_v3.duckdb"
LEGACY_DB_PATH = "/files/duckdb/ag2_v2.duckdb"
LEGACY_SOURCE_PATH = str(os.getenv("AG2_LEGACY_DUCKDB_PATH", LEGACY_DB_PATH) or LEGACY_DB_PATH).strip() or LEGACY_DB_PATH
MIGRATION_KEY = "ag2_v2_bootstrap_v1"
DB_PATH = str(os.getenv("AG2_DUCKDB_PATH", DEFAULT_DB_PATH) or DEFAULT_DB_PATH).strip() or DEFAULT_DB_PATH
if DB_PATH == LEGACY_DB_PATH:
    # Guardrail: AG2-V3 should never write to the V2 database.
    DB_PATH = DEFAULT_DB_PATH
DEFAULT_BATCH_SIZE = 10
WORKFLOW_VERSION = "3.0.4"


def _to_bool(v, dflt=False):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v or "").strip().lower()
    if not s:
        return dflt
    if s in ("1", "true", "yes", "y", "on", "enabled"):
        return True
    if s in ("0", "false", "no", "n", "off", "disabled"):
        return False
    return dflt


MIGRATE_TECH_SIGNALS = _to_bool(os.getenv("AG2_MIGRATE_TECH_SIGNALS", "false"), False)
RUN_LEGACY_MIGRATION = _to_bool(os.getenv("AG2_RUN_LEGACY_MIGRATION", "false"), False)


def _is_wal_internal_error(exc):
    msg = str(exc or "").lower()
    return (
        "failure while replaying wal file" in msg
        or ("internal error" in msg and "wal file" in msg)
        or "getdefaultdatabase" in msg
    )


def _candidate_db_paths(path):
    p = str(path or "").strip() or DEFAULT_DB_PATH
    out = [p]
    if p == LEGACY_DB_PATH and DEFAULT_DB_PATH not in out:
        out.append(DEFAULT_DB_PATH)
    return out


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    last_exc = None
    selected = None
    for candidate in _candidate_db_paths(path):
        for attempt in range(retries):
            try:
                con = duckdb.connect(candidate)
                selected = candidate
                break
            except Exception as e:
                last_exc = e
                msg = str(e).lower()
                if "lock" in msg and attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
                    continue
                # If legacy DB/WAL is broken, transparently fallback to V3 DB.
                if candidate == LEGACY_DB_PATH and _is_wal_internal_error(e):
                    break
                raise
        if con is not None:
            break

    if con is None:
        raise last_exc or RuntimeError("DuckDB connection failed with unknown error.")

    try:
        if selected and selected != path:
            print(f"[AG2-V3] duckdb path fallback: '{path}' -> '{selected}'")
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Sans ça, ag2_v3.duckdb a
            # fragmenté jusqu'à 3.6 GB pour ~6 MB de données utiles.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


SCHEMA_STMTS = [
    """
    CREATE TABLE IF NOT EXISTS universe (
      symbol VARCHAR PRIMARY KEY,
      symbol_yahoo VARCHAR,
      name VARCHAR,
      asset_class VARCHAR DEFAULT 'EQUITY',
      exchange VARCHAR DEFAULT 'Euronext Paris',
      currency VARCHAR DEFAULT 'EUR',
      country VARCHAR,
      sector VARCHAR,
      industry VARCHAR,
      isin VARCHAR,
      enabled BOOLEAN DEFAULT TRUE,
      boursorama_ref VARCHAR,
      base_ccy VARCHAR,
      quote_ccy VARCHAR,
      pip_size DOUBLE,
      price_decimals INTEGER,
      trading_hours VARCHAR DEFAULT '24x5',
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS technical_signals (
      id VARCHAR PRIMARY KEY,
      run_id VARCHAR NOT NULL,
      symbol VARCHAR NOT NULL,
      symbol_internal VARCHAR,
      symbol_yahoo VARCHAR,
      asset_class VARCHAR DEFAULT 'EQUITY',
      workflow_date TIMESTAMP NOT NULL,
      base_ccy VARCHAR,
      quote_ccy VARCHAR,
      pip_size DOUBLE,
      price_decimals INTEGER,
      trading_hours VARCHAR,
      h1_date TIMESTAMP,
      h1_source VARCHAR,
      h1_status VARCHAR,
      h1_warnings VARCHAR,
      h1_action VARCHAR,
      h1_score INTEGER,
      h1_confidence DOUBLE,
      h1_rationale VARCHAR,
      d1_date TIMESTAMP,
      d1_source VARCHAR,
      d1_status VARCHAR,
      d1_warnings VARCHAR,
      d1_action VARCHAR,
      d1_score INTEGER,
      d1_confidence DOUBLE,
      d1_rationale VARCHAR,
      last_close DOUBLE,
      h1_sma20 DOUBLE,
      h1_sma50 DOUBLE,
      h1_sma200 DOUBLE,
      h1_ema12 DOUBLE,
      h1_ema26 DOUBLE,
      h1_macd DOUBLE,
      h1_macd_signal DOUBLE,
      h1_macd_hist DOUBLE,
      h1_rsi14 DOUBLE,
      h1_volatility DOUBLE,
      h1_last_close DOUBLE,
      h1_atr DOUBLE,
      h1_atr_pct DOUBLE,
      h1_bb_upper DOUBLE,
      h1_bb_lower DOUBLE,
      h1_bb_width DOUBLE,
      h1_stoch_k DOUBLE,
      h1_stoch_d DOUBLE,
      h1_adx DOUBLE,
      h1_obv_slope DOUBLE,
      h1_resistance DOUBLE,
      h1_support DOUBLE,
      h1_dist_res_pct DOUBLE,
      h1_dist_sup_pct DOUBLE,
      d1_sma20 DOUBLE,
      d1_sma50 DOUBLE,
      d1_sma200 DOUBLE,
      d1_ema12 DOUBLE,
      d1_ema26 DOUBLE,
      d1_macd DOUBLE,
      d1_macd_signal DOUBLE,
      d1_macd_hist DOUBLE,
      d1_rsi14 DOUBLE,
      d1_volatility DOUBLE,
      d1_last_close DOUBLE,
      d1_atr DOUBLE,
      d1_atr_pct DOUBLE,
      d1_bb_upper DOUBLE,
      d1_bb_lower DOUBLE,
      d1_bb_width DOUBLE,
      d1_stoch_k DOUBLE,
      d1_stoch_d DOUBLE,
      d1_adx DOUBLE,
      d1_obv_slope DOUBLE,
      d1_resistance DOUBLE,
      d1_support DOUBLE,
      d1_dist_res_pct DOUBLE,
      d1_dist_sup_pct DOUBLE,
      atr_pips_h1 DOUBLE,
      atr_pips_d1 DOUBLE,
      stop_pips_suggested DOUBLE,
      data_quality_flags VARCHAR,
      data_age_h1_hours DOUBLE,
      data_age_d1_hours DOUBLE,
      filter_reason VARCHAR,
      pass_ai BOOLEAN DEFAULT FALSE,
      pass_pm BOOLEAN DEFAULT FALSE,
      sig_hash VARCHAR,
      call_ai BOOLEAN DEFAULT FALSE,
      dedup_reason VARCHAR,
      ai_decision VARCHAR,
      ai_validated BOOLEAN,
      ai_quality INTEGER,
      ai_reasoning VARCHAR,
      ai_chart_pattern VARCHAR,
      ai_stop_loss DOUBLE,
      ai_stop_basis VARCHAR,
      ai_bias_sma200 VARCHAR,
      ai_regime_d1 VARCHAR,
      ai_alignment VARCHAR,
      ai_bb_status VARCHAR,
      ai_rsi_status VARCHAR,
      ai_missing VARCHAR,
      ai_anomalies VARCHAR,
      ai_output_ref VARCHAR,
      ai_rr_theoretical DOUBLE,
      should_vectorize BOOLEAN DEFAULT FALSE,
      vector_status VARCHAR DEFAULT 'PENDING',
      vector_id VARCHAR,
      vectorized_at TIMESTAMP,
      row_hash VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_dedup_cache (
      symbol VARCHAR NOT NULL,
      interval_key VARCHAR NOT NULL,
      sig_hash VARCHAR NOT NULL,
      sig_json VARCHAR,
      last_ai_at TIMESTAMP,
      last_ai_run_id VARCHAR,
      last_ai_reason VARCHAR,
      last_ai_output_ref VARCHAR,
      ttl_minutes INTEGER DEFAULT 240,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (symbol, interval_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_log (
      run_id VARCHAR PRIMARY KEY,
      started_at TIMESTAMP NOT NULL,
      finished_at TIMESTAMP,
      status VARCHAR DEFAULT 'RUNNING',
      batch_start INTEGER,
      batch_size INTEGER,
      total_pool INTEGER,
      symbols_ok INTEGER DEFAULT 0,
      symbols_error INTEGER DEFAULT 0,
      ai_calls INTEGER DEFAULT 0,
      vectors_written INTEGER DEFAULT 0,
      error_detail VARCHAR,
      version VARCHAR DEFAULT '3.0.0'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_state (
      key VARCHAR PRIMARY KEY,
      value INTEGER NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]

MIGRATE_STMTS = [
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS symbol_yahoo VARCHAR",
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS base_ccy VARCHAR",
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS quote_ccy VARCHAR",
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS pip_size DOUBLE",
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS price_decimals INTEGER",
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS trading_hours VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS symbol_internal VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS symbol_yahoo VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS asset_class VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS base_ccy VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS quote_ccy VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS pip_size DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS price_decimals INTEGER",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS trading_hours VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS atr_pips_h1 DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS atr_pips_d1 DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS stop_pips_suggested DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_quality_flags VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_age_h1_hours DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_age_d1_hours DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_bb_status VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_rsi_status VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS should_vectorize BOOLEAN DEFAULT FALSE",
]

VIEW_STMTS = [
    "CREATE INDEX IF NOT EXISTS idx_ts_symbol_internal ON technical_signals(symbol_internal)",
    "CREATE INDEX IF NOT EXISTS idx_ts_asset_class ON technical_signals(asset_class)",
    """
    CREATE OR REPLACE VIEW v_ag2_fx_output AS
    SELECT
      id,
      run_id,
      COALESCE(symbol_internal, symbol) AS symbol,
      symbol_yahoo,
      asset_class,
      base_ccy,
      quote_ccy,
      pip_size,
      price_decimals,
      trading_hours,
      workflow_date,
      h1_date,
      d1_date,
      h1_status,
      d1_status,
      h1_action,
      d1_action,
      h1_score,
      d1_score,
      h1_confidence,
      d1_confidence,
      h1_atr,
      d1_atr,
      atr_pips_h1,
      atr_pips_d1,
      stop_pips_suggested,
      data_quality_flags,
      data_age_h1_hours,
      data_age_d1_hours,
      ai_decision,
      ai_quality,
      ai_bb_status,
      ai_rsi_status,
      pass_pm,
      updated_at
    FROM technical_signals
    WHERE UPPER(COALESCE(asset_class, '')) = 'FX'
    """,
]


def _safe_sql_str(s):
    return str(s or "").replace("\\", "/").replace("'", "''")


def _relation_columns(con, relation):
    try:
        con.execute(f"SELECT * FROM {relation} LIMIT 0")
        return [str(d[0]) for d in (con.description or [])]
    except Exception:
        return []


def _table_exists(con, relation):
    try:
        con.execute(f"SELECT 1 FROM {relation} LIMIT 1")
        return True
    except Exception:
        return False


def _symbol_expr(cols, alias):
    candidates = []
    for c in ("symbol_internal", "symbol", "symbol_yahoo"):
        if c in cols:
            candidates.append(f"NULLIF(TRIM({alias}.{c}), '')")
    if not candidates:
        return "''"
    return "UPPER(TRIM(COALESCE(" + ", ".join(candidates) + ")))"


def _freshness_expr(cols, alias):
    for c in ("updated_at", "workflow_date", "created_at", "h1_date", "d1_date"):
        if c in cols:
            return f"{alias}.{c}"
    return "CURRENT_TIMESTAMP"


def _ensure_migration_log(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          key VARCHAR PRIMARY KEY,
          status VARCHAR,
          details VARCHAR,
          applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _get_migration_status(con, key):
    row = con.execute("SELECT status FROM schema_migrations WHERE key = ?", [key]).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def _set_migration_status(con, key, status, details_obj):
    details = json.dumps(details_obj or {}, ensure_ascii=False)
    con.execute(
        """
        INSERT OR REPLACE INTO schema_migrations (key, status, details, applied_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [key, status, details],
    )


def migrate_legacy_v2(con, legacy_path):
    report = {
        "key": MIGRATION_KEY,
        "status": "skipped",
        "legacy_path": legacy_path,
        "run_legacy_migration": RUN_LEGACY_MIGRATION,
        "migrate_tech_signals": MIGRATE_TECH_SIGNALS,
        "dedup_rows_copied": 0,
        "signals_rows_copied": 0,
        "error": "",
    }

    if not RUN_LEGACY_MIGRATION:
        report["status"] = "disabled"
        return report

    _ensure_migration_log(con)
    if _get_migration_status(con, MIGRATION_KEY) == "done":
        report["status"] = "already_done"
        return report

    lp = str(legacy_path or "").strip()
    if not lp:
        report["status"] = "no_legacy_path"
        return report
    if os.path.abspath(lp) == os.path.abspath(DB_PATH):
        report["status"] = "same_as_target"
        return report
    if not os.path.exists(lp):
        report["status"] = "legacy_missing"
        return report

    attached = False
    try:
        con.execute(f"ATTACH '{_safe_sql_str(lp)}' AS legacy")
        attached = True

        # ---- 1) AI dedup cache migration (high-value for avoiding unnecessary AI calls)
        if _table_exists(con, "legacy.ai_dedup_cache"):
            target_cols = _relation_columns(con, "ai_dedup_cache")
            source_cols = _relation_columns(con, "legacy.ai_dedup_cache")
            common = [c for c in target_cols if c in source_cols]
            if common:
                cols_sql = ", ".join(common)
                con.execute(
                    f"INSERT OR REPLACE INTO ai_dedup_cache ({cols_sql}) SELECT {cols_sql} FROM legacy.ai_dedup_cache"
                )
                report["dedup_rows_copied"] = -1

        # ---- 2) Optional technical signals bootstrap (can be heavy on large databases)
        if MIGRATE_TECH_SIGNALS and _table_exists(con, "legacy.technical_signals"):
            target_cols = _relation_columns(con, "technical_signals")
            source_cols = _relation_columns(con, "legacy.technical_signals")
            common = [c for c in target_cols if c in source_cols]
            required = {"id", "run_id", "symbol", "workflow_date"}
            if required.issubset(set(common)):
                cols_sql = ", ".join(common)
                symbol_expr = _symbol_expr(source_cols, "src")
                freshness_expr = _freshness_expr(source_cols, "src")
                con.execute(
                    f"""
                    WITH ranked AS (
                      SELECT
                        src.*,
                        ROW_NUMBER() OVER (
                          PARTITION BY {symbol_expr}
                          ORDER BY {freshness_expr} DESC NULLS LAST
                        ) AS __rn
                      FROM legacy.technical_signals src
                    )
                    INSERT OR REPLACE INTO technical_signals ({cols_sql})
                    SELECT {", ".join([f"ranked.{c}" for c in common])}
                    FROM ranked
                    WHERE ranked.__rn = 1
                      AND {_symbol_expr(source_cols, "ranked")} <> ''
                    """
                )
                report["signals_rows_copied"] = -1

        report["status"] = "done"
        _set_migration_status(con, MIGRATION_KEY, "done", report)
        return report

    except Exception as e:
        report["status"] = "failed"
        report["error"] = str(e)[:1200]
        _set_migration_status(con, MIGRATION_KEY, "failed", report)
        return report
    finally:
        if attached:
            try:
                con.execute("DETACH legacy")
            except Exception:
                pass

items = _items or []
first_json = items[0].get("json", {}) if items else {}

raw_queue = first_json.get("_process_queue")
if isinstance(raw_queue, list):
    process_queue = [x for x in raw_queue if isinstance(x, dict)]
else:
    process_queue = [{"symbol": str(s), "symbol_yahoo": str(s), "asset_class": "EQUITY", "enabled": True} for s in (first_json.get("_all_symbols") or [])]

batch_size = int(first_json.get("batch_size") or DEFAULT_BATCH_SIZE)
if batch_size <= 0:
    batch_size = DEFAULT_BATCH_SIZE

config = {
    "yfinance_api_base": first_json.get("yfinance_api_base", "http://yfinance-api:8080"),
    "intraday": first_json.get("intraday", {}),
    "daily": first_json.get("daily", {}),
    "strategy_version": str(first_json.get("strategy_version") or "strategy_v3"),
    "config_version": str(first_json.get("config_version") or "config_v3"),
    "prompt_version": str(first_json.get("prompt_version") or "prompt_v3"),
    "enable_fx": bool(first_json.get("enable_fx")),
    "universe_mode": str(first_json.get("universe_mode") or "ALL").upper(),
    "batch_state_key": str(first_json.get("batch_state_key") or "last_index"),
    "fx_universe_count": int(first_json.get("fx_universe_count") or 0),
    "non_fx_universe_count": int(first_json.get("non_fx_universe_count") or 0),
    "universe_scope": first_json.get("universe_scope") or ["EQUITY", "CRYPTO"],
}

with db_con() as con:
    for stmt in SCHEMA_STMTS:
        con.execute(stmt)
    for stmt in MIGRATE_STMTS:
        try:
            con.execute(stmt)
        except Exception:
            pass
    for stmt in VIEW_STMTS:
        try:
            con.execute(stmt)
        except Exception:
            pass

    legacy_migration_report = migrate_legacy_v2(con, LEGACY_SOURCE_PATH)

    # Universe sync
    universe = first_json.get("_universe", []) or []
    for r in universe:
        sym = str(r.get("symbol") or r.get("symbol_internal") or r.get("Symbol") or r.get("symbol_yahoo") or "").strip()
        if not sym:
            continue
        con.execute(
            """
            INSERT OR REPLACE INTO universe (
              symbol, symbol_yahoo, name, asset_class, exchange, currency, country, sector, industry,
              isin, enabled, boursorama_ref, base_ccy, quote_ccy, pip_size, price_decimals, trading_hours, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                sym,
                str(r.get("symbol_yahoo") or sym),
                r.get("name") or r.get("Name") or sym,
                str(r.get("asset_class") or r.get("AssetClass") or "EQUITY"),
                r.get("exchange") or r.get("Exchange") or "Euronext Paris",
                r.get("currency") or r.get("Currency") or "EUR",
                r.get("country") or r.get("Country") or "",
                r.get("sector") or r.get("Sector") or "",
                r.get("industry") or r.get("Industry") or "",
                r.get("isin") or r.get("ISIN") or "",
                str(r.get("enabled", True)).lower() == "true",
                r.get("boursorama_ref") or r.get("BoursoramaRef") or "",
                r.get("base_ccy"),
                r.get("quote_ccy"),
                r.get("pip_size"),
                r.get("price_decimals"),
                r.get("trading_hours") or ("24x5" if str(r.get("asset_class", "")).upper() == "FX" else ""),
            ],
        )

    # Batch rotation (persistent).
    row = con.execute("SELECT value FROM batch_state WHERE key = ?", [config["batch_state_key"]]).fetchone()
    idx = int(row[0]) if row else 0
    total = len(process_queue)
    if idx >= total:
        idx = 0

    batch = process_queue[idx : idx + batch_size]
    next_idx = 0 if (idx + batch_size >= total) else idx + batch_size

    con.execute(
        "INSERT OR REPLACE INTO batch_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
        [config["batch_state_key"], next_idx],
    )

    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d%H%M%S")
    run_id = f"AG2V3_{ts}_{idx}"

    con.execute(
        """
        INSERT OR REPLACE INTO run_log (run_id, started_at, batch_start, batch_size, total_pool, version)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
        """,
        [run_id, idx, len(batch), total, WORKFLOW_VERSION],
    )

out = []
for i, entry in enumerate(batch):
    symbol_internal = str(entry.get("symbol") or entry.get("symbol_internal") or "").strip()
    symbol_yahoo = str(entry.get("symbol_yahoo") or symbol_internal).strip()
    out.append(
        {
            "json": {
                "ok": True,
                "symbol": symbol_internal,
                "symbol_internal": symbol_internal,
                "symbol_yahoo": symbol_yahoo,
                "asset_class": str(entry.get("asset_class") or "EQUITY").upper(),
                "base_ccy": entry.get("base_ccy"),
                "quote_ccy": entry.get("quote_ccy"),
                "pip_size": entry.get("pip_size"),
                "price_decimals": entry.get("price_decimals"),
                "trading_hours": entry.get("trading_hours"),
                "run_id": run_id,
                "db_path": DB_PATH,
                "legacy_migration": legacy_migration_report,
                "yfinance_api_base": config["yfinance_api_base"],
                "intraday": config["intraday"],
                "daily": config["daily"],
                "strategy_version": config["strategy_version"],
                "config_version": config["config_version"],
                "prompt_version": config["prompt_version"],
                "enable_fx": config["enable_fx"],
                "universe_mode": config["universe_mode"],
                "batch_state_key": config["batch_state_key"],
                "fx_universe_count": config["fx_universe_count"],
                "non_fx_universe_count": config["non_fx_universe_count"],
                "universe_scope": config["universe_scope"],
                "batch_info": {"start": idx, "size": len(batch), "total": total},
                "_index": i,
            }
        }
    )

if not out:
    out = [{"json": {"ok": False, "error": "EMPTY_BATCH", "run_id": run_id}}]

return out
