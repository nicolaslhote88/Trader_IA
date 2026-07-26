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
def db_con(path=DB_PATH, retries=10, delay=0.2):
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
                    time.sleep(min(1.5, delay * (2 ** attempt)))
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
      exchange VARCHAR,
      currency VARCHAR,
      workflow_date TIMESTAMP NOT NULL,
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
      data_quality_flags VARCHAR,
      data_age_h1_hours DOUBLE,
      data_age_d1_hours DOUBLE,
      h1_closed_only BOOLEAN DEFAULT FALSE,
      d1_closed_only BOOLEAN DEFAULT FALSE,
      h1_dropped_open INTEGER DEFAULT 0,
      d1_dropped_open INTEGER DEFAULT 0,
      h1_dropped_invalid INTEGER DEFAULT 0,
      d1_dropped_invalid INTEGER DEFAULT 0,
      strategy_version VARCHAR,
      config_version VARCHAR,
      prompt_version VARCHAR,
      n8n_execution_id VARCHAR,
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
      ai_model VARCHAR,
      ai_rr_theoretical DOUBLE,
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
    """
    CREATE TABLE IF NOT EXISTS universe_quarantine (
      symbol VARCHAR PRIMARY KEY,
      symbol_yahoo VARCHAR,
      asset_class VARCHAR,
      reason VARCHAR,
      reason_detail VARCHAR,
      active BOOLEAN DEFAULT FALSE,
      first_quarantined_at TIMESTAMP,
      last_evaluated_at TIMESTAMP,
      last_released_at TIMESTAMP,
      rule_version VARCHAR,
      metrics_json VARCHAR,
      manual_override BOOLEAN DEFAULT FALSE,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS universe_segments (
      symbol VARCHAR NOT NULL,
      segment VARCHAR NOT NULL,
      active BOOLEAN DEFAULT TRUE,
      priority_score DOUBLE,
      source VARCHAR DEFAULT 'auto',
      reason VARCHAR,
      metrics_json VARCHAR,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (symbol, segment)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version VARCHAR PRIMARY KEY,
      applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      description VARCHAR
    )
    """,
]

MIGRATE_STMTS = [
    "ALTER TABLE universe ADD COLUMN IF NOT EXISTS symbol_yahoo VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS symbol_internal VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS symbol_yahoo VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS asset_class VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS exchange VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS currency VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS h1_closed_only BOOLEAN DEFAULT FALSE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS d1_closed_only BOOLEAN DEFAULT FALSE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS h1_dropped_open INTEGER DEFAULT 0",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS d1_dropped_open INTEGER DEFAULT 0",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS h1_dropped_invalid INTEGER DEFAULT 0",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS d1_dropped_invalid INTEGER DEFAULT 0",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS strategy_version VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS config_version VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS prompt_version VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS n8n_execution_id VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_model VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_quality_flags VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_age_h1_hours DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS data_age_d1_hours DOUBLE",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_bb_status VARCHAR",
    "ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_rsi_status VARCHAR",
    "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT FALSE",
    "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS manual_override BOOLEAN DEFAULT FALSE",
    "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS reason VARCHAR",
    "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS reason_detail VARCHAR",
    "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS metrics_json VARCHAR",
    "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE",
    "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS priority_score DOUBLE",
    "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'auto'",
    "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS reason VARCHAR",
    "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS metrics_json VARCHAR",
]

VIEW_STMTS = [
    "CREATE INDEX IF NOT EXISTS idx_ts_symbol_internal ON technical_signals(symbol_internal)",
    "CREATE INDEX IF NOT EXISTS idx_ts_asset_class ON technical_signals(asset_class)",
    """
    CREATE OR REPLACE VIEW v_latest_signals AS
    SELECT
      id, run_id, symbol, symbol_internal, symbol_yahoo, asset_class, exchange, currency,
      workflow_date, h1_date, d1_date, h1_status, d1_status,
      h1_closed_only, d1_closed_only, h1_dropped_open, d1_dropped_open,
      h1_dropped_invalid, d1_dropped_invalid,
      h1_action, h1_score, h1_confidence, d1_action, d1_score, d1_confidence,
      last_close, d1_rsi14, d1_macd_hist, d1_sma200, d1_bb_width, d1_adx, d1_volatility,
      data_quality_flags, data_age_h1_hours, data_age_d1_hours,
      ai_decision, ai_validated, ai_quality, ai_alignment, ai_stop_loss, ai_rr_theoretical,
      pass_ai, pass_pm, sig_hash, row_hash, strategy_version, config_version,
      prompt_version, ai_model, n8n_execution_id, created_at, updated_at
    FROM technical_signals
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY symbol ORDER BY COALESCE(workflow_date, updated_at, created_at) DESC, id DESC
    ) = 1
    """,
    """
    CREATE OR REPLACE VIEW v_ag1_summary AS
    SELECT * FROM v_latest_signals WHERE COALESCE(pass_pm, FALSE)
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


def load_active_quarantine(con):
    try:
        rows = con.execute(
            """
            SELECT UPPER(TRIM(symbol)) AS symbol
            FROM universe_quarantine
            WHERE COALESCE(active, FALSE)
              AND symbol IS NOT NULL
              AND TRIM(symbol) <> ''
            """
        ).fetchall()
        return {r[0] for r in rows if r and r[0]}
    except Exception:
        return set()


def _entry_symbol(entry):
    return str(entry.get("symbol") or entry.get("symbol_internal") or "").strip().upper()


def load_active_segments(con):
    segments = {}
    try:
        rows = con.execute(
            """
            SELECT UPPER(TRIM(symbol)) AS symbol,
                   UPPER(TRIM(segment)) AS segment
            FROM universe_segments
            WHERE COALESCE(active, TRUE)
              AND symbol IS NOT NULL
              AND TRIM(symbol) <> ''
              AND segment IS NOT NULL
              AND TRIM(segment) <> ''
            """
        ).fetchall()
        for sym, segment in rows:
            if not sym or not segment:
                continue
            segments.setdefault(sym, set()).add(segment)
    except Exception:
        segments = {}
    return segments


def apply_rotation_mode(process_queue, quarantine_symbols, segments, rotation_mode, batch_size):
    mode = str(rotation_mode or "ACTIONS_ONLY").strip().upper()
    by_symbol = {}
    ordered_symbols = []
    for entry in process_queue:
        sym = _entry_symbol(entry)
        if not sym:
            continue
        if sym not in by_symbol:
            ordered_symbols.append(sym)
        by_symbol[sym] = entry

    def not_quarantined(sym):
        return sym not in quarantine_symbols

    if mode == "HELD_CORE":
        held_symbols = {s for s, segs in segments.items() if "HELD" in segs}
        core_symbols = {
            s for s, segs in segments.items()
            if "CORE_AUTO" in segs or "CORE_MANUAL" in segs
        }
        always = [by_symbol[s] for s in ordered_symbols if s in held_symbols and s in by_symbol]
        always_seen = {_entry_symbol(e) for e in always}
        rotation = [
            by_symbol[s] for s in ordered_symbols
            if s in core_symbols and s not in always_seen and not_quarantined(s)
        ]
        return always, rotation, {
            "rotation_mode": mode,
            "held_total": len(always),
            "segment_rotation_total": len(rotation),
            "segment_symbols_total": len(held_symbols | core_symbols),
        }

    if mode == "WATCHLIST":
        watch_symbols = {s for s, segs in segments.items() if "WATCHLIST" in segs}
        rotation = [by_symbol[s] for s in ordered_symbols if s in watch_symbols and not_quarantined(s)]
        return [], rotation, {
            "rotation_mode": mode,
            "held_total": 0,
            "segment_rotation_total": len(rotation),
            "segment_symbols_total": len(watch_symbols),
        }

    rotation = [by_symbol[s] for s in ordered_symbols if not_quarantined(s)]
    return [], rotation, {
        "rotation_mode": "ACTIONS_ONLY",
        "held_total": 0,
        "segment_rotation_total": len(rotation),
        "segment_symbols_total": len(rotation),
    }

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
    "n8n_execution_id": str(first_json.get("n8n_execution_id") or ""),
    "closed_only": bool(first_json.get("closed_only", True)),
    "validated_only": bool(first_json.get("validated_only", True)),
    "universe_mode": str(first_json.get("universe_mode") or "ACTIONS_ONLY").upper(),
    "rotation_mode": str(first_json.get("rotation_mode") or "ACTIONS_ONLY").upper(),
    "batch_state_key": str(first_json.get("batch_state_key") or "last_index"),
    "universe_scope": first_json.get("universe_scope") or ["EQUITY", "ETF", "CRYPTO"],
}

with db_con() as con:
    for stmt in SCHEMA_STMTS:
        con.execute(stmt)
    for stmt in MIGRATE_STMTS:
        con.execute(stmt)
    for stmt in VIEW_STMTS:
        con.execute(stmt)
    con.execute(
        "INSERT OR IGNORE INTO schema_migrations (version, description) VALUES (?, ?)",
        ["20260726_ag2_v3_1_closed_bars", "Closed bars, OHLCV validation, lineage and workflow reliability"],
    )

    # Réconcilie les runs interrompus avant Finalize Run.
    con.execute(
        """
        UPDATE run_log
        SET finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
            status = 'STALE',
            error_detail = COALESCE(error_detail, 'Auto-reconciled: previous workflow execution did not finalize')
        WHERE (status IS NULL OR status = 'RUNNING')
          AND started_at < CURRENT_TIMESTAMP - INTERVAL '2 hours'
        """
    )

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
              isin, enabled, boursorama_ref, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
            ],
        )

    # Batch rotation (persistent), with reversible quarantine/segment filtering.
    raw_total = len(process_queue)
    quarantine_symbols = load_active_quarantine(con)
    segments = load_active_segments(con)
    always_batch, rotation_queue, rotation_meta = apply_rotation_mode(
        process_queue,
        quarantine_symbols,
        segments,
        config["rotation_mode"],
        batch_size,
    )
    eligible_symbols = {_entry_symbol(r) for r in always_batch + rotation_queue}
    quarantine_excluded = len([
        r for r in process_queue
        if _entry_symbol(r) in quarantine_symbols and _entry_symbol(r) not in eligible_symbols
    ])

    row = con.execute("SELECT value FROM batch_state WHERE key = ?", [config["batch_state_key"]]).fetchone()
    idx = int(row[0]) if row else 0
    rotation_total = len(rotation_queue)
    total = len(always_batch) + rotation_total
    if idx >= rotation_total:
        idx = 0

    rotation_batch = rotation_queue[idx : idx + batch_size]
    batch = always_batch + rotation_batch
    next_idx = 0 if (rotation_total == 0 or idx + batch_size >= rotation_total) else idx + batch_size

    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d%H%M%S%f")
    run_id = f"AG2V3_{ts}_{idx}"

    con.execute(
        """
        INSERT OR REPLACE INTO run_log (
          run_id, started_at, status, batch_start, batch_size, total_pool, version
        )
        VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?, ?, ?, ?)
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
                "exchange": str(entry.get("exchange") or ""),
                "currency": str(entry.get("currency") or "").upper(),
                "run_id": run_id,
                "db_path": DB_PATH,
                "legacy_migration": legacy_migration_report,
                "yfinance_api_base": config["yfinance_api_base"],
                "intraday": config["intraday"],
                "daily": config["daily"],
                "strategy_version": config["strategy_version"],
                "config_version": config["config_version"],
                "prompt_version": config["prompt_version"],
                "n8n_execution_id": config["n8n_execution_id"],
                "closed_only": config["closed_only"],
                "validated_only": config["validated_only"],
                "universe_mode": config["universe_mode"],
                "rotation_mode": config["rotation_mode"],
                "batch_state_key": config["batch_state_key"],
                "universe_scope": config["universe_scope"],
                "batch_info": {
                    "start": idx,
                    "size": len(batch),
                    "total": total,
                    "raw_total": raw_total,
                    "quarantine_excluded": quarantine_excluded,
                    "rotation_size": len(rotation_batch),
                    "always_included": len(always_batch),
                    "rotation_total": rotation_total,
                    "next_index": next_idx,
                    "state_key": config["batch_state_key"],
                    **rotation_meta,
                },
                "_index": i,
            }
        }
    )

if not out:
    out = [{"json": {"ok": False, "error": "EMPTY_BATCH", "run_id": run_id}}]

return out
