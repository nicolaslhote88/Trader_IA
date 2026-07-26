import duckdb
import json
import time
from datetime import datetime

AG2_DB_PATH = "/files/duckdb/ag2_v3.duckdb"
AG1_DB_PATH = "/files/duckdb/ag1_v4_consensus.duckdb"
AG3_DB_PATH = "/files/duckdb/ag3_v2.duckdb"
YF_DB_PATH = "/files/duckdb/yf_enrichment_v1.duckdb"

RULE_VERSION = "universe_quarantine_v2_closed_bars_20260726"
LOOKBACK_DAYS = 30
MIN_TECH_RUNS = 5
MIN_YF_RUNS = 3
LOW_AVG_VOLUME = 5000
LOW_MAX_VOLUME = 20000
CORE_AUTO_TARGET = 50


def db_connect(path, read_only=False, retries=8, delay=0.35):
    last_error = None
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as exc:
            last_error = exc
            if "lock" in str(exc).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
                continue
            raise
    raise last_error


def scalar_int(v, default=0):
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def scalar_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def text(v):
    if v is None:
        return ""
    return str(v).strip()


def truthy(v):
    if isinstance(v, bool):
        return v
    s = text(v).lower()
    return s in ("1", "true", "yes", "y", "oui", "ok", "enabled")


def safe_symbol(s):
    out = []
    for ch in text(s).upper():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:80]


def rows_as_dicts(con, sql, params=None):
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in (cur.description or [])]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def ensure_schema(con):
    con.execute(
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
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_quarantine_audit_runs (
          run_id VARCHAR PRIMARY KEY,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          status VARCHAR,
          rule_version VARCHAR,
          evaluated_count INTEGER,
          active_quarantine_count INTEGER,
          newly_quarantined_count INTEGER,
          released_count INTEGER,
          held_exempt_count INTEGER,
          error_detail VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_quarantine_audit_history (
          audit_id VARCHAR PRIMARY KEY,
          run_id VARCHAR,
          symbol VARCHAR,
          decision VARCHAR,
          reason VARCHAR,
          metrics_json VARCHAR,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute(
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
        """
    )
    migrations = [
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS symbol_yahoo VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS asset_class VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS reason VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS reason_detail VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT FALSE",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS first_quarantined_at TIMESTAMP",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMP",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS last_released_at TIMESTAMP",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS rule_version VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS metrics_json VARCHAR",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS manual_override BOOLEAN DEFAULT FALSE",
        "ALTER TABLE universe_quarantine ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS priority_score DOUBLE",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'auto'",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS reason VARCHAR",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS metrics_json VARCHAR",
        "ALTER TABLE universe_segments ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ]
    for stmt in migrations:
        try:
            con.execute(stmt)
        except Exception:
            pass
    con.execute("CREATE INDEX IF NOT EXISTS idx_universe_quarantine_active ON universe_quarantine(active)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_universe_quarantine_history_run ON universe_quarantine_audit_history(run_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_universe_segments_segment ON universe_segments(segment, active)")


def attach_readonly(con, path, alias):
    safe_path = str(path or "").replace("\\", "/").replace("'", "''")
    con.execute("ATTACH '" + safe_path + "' AS " + alias + " (READ_ONLY)")
    return True


def load_held_symbols(con):
    rows = con.execute(
        """
        SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
        FROM ag1.main.portfolio_positions_mtm_latest
        WHERE symbol IS NOT NULL
          AND TRIM(symbol) <> ''
          AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR', '__META__')
          AND COALESCE(quantity, 0) <> 0
        """
    ).fetchall()
    return {r[0] for r in rows if r and r[0]}


def load_yf_metrics(con):
    out = {}
    rows = rows_as_dicts(
        con,
        f"""
            SELECT
              UPPER(TRIM(symbol)) AS symbol,
              COUNT(*) AS yf_runs,
              SUM(CASE WHEN COALESCE(quote_ok, FALSE) THEN 1 ELSE 0 END) AS quote_ok_runs,
              SUM(CASE WHEN NOT COALESCE(quote_ok, FALSE) THEN 1 ELSE 0 END) AS quote_bad_runs,
              AVG(CASE WHEN volume IS NOT NULL THEN volume ELSE NULL END) AS avg_volume,
              MAX(CASE WHEN volume IS NOT NULL THEN volume ELSE NULL END) AS max_volume
            FROM yf.main.yf_symbol_enrichment_history
            WHERE symbol IS NOT NULL
              AND fetched_at >= CURRENT_TIMESTAMP - INTERVAL '{LOOKBACK_DAYS} days'
            GROUP BY 1
        """
    )
    for r in rows:
        sym = text(r.get("symbol")).upper()
        if sym:
            out[sym] = r
    return out


def refresh_universe_segments(con, held_symbols, yf_attached, ag3_attached):
    con.execute("DELETE FROM universe_segments WHERE COALESCE(source, 'auto') = 'auto'")

    universe_rows = rows_as_dicts(
        con,
        """
        SELECT
          UPPER(TRIM(u.symbol)) AS symbol,
          UPPER(TRIM(COALESCE(u.symbol_yahoo, u.symbol))) AS symbol_yahoo,
          COALESCE(u.name, u.symbol) AS name,
          UPPER(TRIM(COALESCE(u.asset_class, 'EQUITY'))) AS asset_class,
          COALESCE(u.exchange, '') AS exchange,
          COALESCE(u.country, '') AS country,
          COALESCE(q.active, FALSE) AS quarantined
        FROM universe u
        LEFT JOIN universe_quarantine q ON UPPER(TRIM(q.symbol)) = UPPER(TRIM(u.symbol))
        WHERE u.symbol IS NOT NULL
          AND TRIM(u.symbol) <> ''
          AND COALESCE(u.enabled, TRUE)
          AND UPPER(TRIM(COALESCE(u.asset_class, 'EQUITY'))) IN ('EQUITY', 'ETF', 'CRYPTO')
          AND UPPER(TRIM(COALESCE(u.symbol_yahoo, u.symbol))) NOT LIKE '%=X'
        """
    )

    latest_yf = {}
    if yf_attached:
        for r in rows_as_dicts(
                con,
                """
                SELECT
                  UPPER(TRIM(symbol)) AS symbol,
                  COALESCE(quote_ok, FALSE) AS quote_ok,
                  COALESCE(volume, 0) AS volume
                FROM yf.main.v_latest_symbol_enrichment
                WHERE symbol IS NOT NULL
                """,
        ):
            sym = text(r.get("symbol")).upper()
            if sym:
                latest_yf[sym] = r

    latest_ag3 = {}
    if ag3_attached:
        for r in rows_as_dicts(
                con,
                """
                SELECT
                  UPPER(TRIM(symbol)) AS symbol,
                  COALESCE(score, 0) AS funda_score,
                  COALESCE(risk_score, 100) AS risk_score,
                  COALESCE(quality_score, 0) AS quality_score,
                  COALESCE(health_score, 0) AS health_score,
                  COALESCE(analyst_count, 0) AS analyst_count
                FROM ag3.main.v_latest_triage
                WHERE symbol IS NOT NULL
                """,
        ):
            sym = text(r.get("symbol")).upper()
            if sym:
                latest_ag3[sym] = r

    manual_rows = rows_as_dicts(
        con,
        """
        SELECT UPPER(TRIM(symbol)) AS symbol
        FROM universe_segments
        WHERE COALESCE(active, TRUE)
          AND COALESCE(source, '') = 'manual'
          AND segment = 'CORE_MANUAL'
        """,
    )
    manual_core = {text(r.get("symbol")).upper() for r in manual_rows if text(r.get("symbol"))}

    candidates = []
    universe_symbols = set()
    available_symbols = set()
    for r in universe_rows:
        sym = text(r.get("symbol")).upper()
        if not sym:
            continue
        universe_symbols.add(sym)
        if sym in held_symbols:
            con.execute(
                """
                INSERT OR REPLACE INTO universe_segments (
                  symbol, segment, active, priority_score, source, reason, metrics_json, updated_at
                ) VALUES (?, 'HELD', TRUE, 1000, 'auto', 'portfolio_position', ?, CURRENT_TIMESTAMP)
                """,
                [sym, json.dumps(dict(r), ensure_ascii=False, default=str)],
            )

        if truthy(r.get("quarantined")):
            continue
        available_symbols.add(sym)
        if sym in held_symbols or sym in manual_core:
            continue

        yf = latest_yf.get(sym, {})
        ag3 = latest_ag3.get(sym, {})
        volume = scalar_float(yf.get("volume"), 0.0) or 0.0
        quote_ok = truthy(yf.get("quote_ok"))
        funda_score = scalar_float(ag3.get("funda_score"), 0.0) or 0.0
        risk_score = scalar_float(ag3.get("risk_score"), 100.0) or 100.0
        quality_score = scalar_float(ag3.get("quality_score"), 0.0) or 0.0
        health_score = scalar_float(ag3.get("health_score"), 0.0) or 0.0
        analyst_count = scalar_float(ag3.get("analyst_count"), 0.0) or 0.0
        name = text(r.get("name")).upper()
        symbol_yahoo = text(r.get("symbol_yahoo")).upper()
        leveraged_or_inverse = (
            "(-2X)" in name
            or "LEVERAGE" in name
            or "LEVERAGED" in name
            or "INVERSE" in name
            or symbol_yahoo.startswith("BX4")
        )
        if leveraged_or_inverse or not quote_ok or volume < 20000:
            continue
        volume_score = min(40.0, max(0.0, volume) ** 0.18)
        analyst_score = min(8.0, analyst_count * 0.35)
        priority_score = (
            volume_score
            + funda_score * 0.40
            + quality_score * 0.12
            + health_score * 0.08
            + analyst_score
            - risk_score * 0.18
        )
        metrics = dict(r)
        metrics.update(yf)
        metrics.update(ag3)
        metrics["priority_score"] = priority_score
        candidates.append((priority_score, sym, metrics))

    candidates.sort(key=lambda x: (-x[0], x[1]))
    core_auto = {sym for _, sym, _ in candidates[:CORE_AUTO_TARGET]}
    for score, sym, metrics in candidates[:CORE_AUTO_TARGET]:
        con.execute(
            """
            INSERT OR REPLACE INTO universe_segments (
              symbol, segment, active, priority_score, source, reason, metrics_json, updated_at
            ) VALUES (?, 'CORE_AUTO', TRUE, ?, 'auto', 'top_composite_liquidity_fundamental', ?, CURRENT_TIMESTAMP)
            """,
            [sym, score, json.dumps(metrics, ensure_ascii=False, default=str)],
        )

    core_all = core_auto | manual_core
    watch_count = 0
    for sym in sorted(available_symbols):
        if sym in held_symbols or sym in core_all:
            continue
        con.execute(
            """
            INSERT OR REPLACE INTO universe_segments (
              symbol, segment, active, priority_score, source, reason, metrics_json, updated_at
            ) VALUES (?, 'WATCHLIST', TRUE, 0, 'auto', 'available_not_held_not_core', '{}', CURRENT_TIMESTAMP)
            """,
            [sym],
        )
        watch_count += 1

    return {
        "held": len(held_symbols & universe_symbols),
        "core_auto": len(core_auto),
        "core_manual": len(manual_core & universe_symbols),
        "watchlist": watch_count,
        "available": len(available_symbols),
    }


def decision_for_symbol(sym, metrics, held_symbols):
    yf_runs = scalar_int(metrics.get("yf_runs"))
    quote_ok_runs = scalar_int(metrics.get("quote_ok_runs"))
    quote_bad_runs = scalar_int(metrics.get("quote_bad_runs"))
    avg_volume = scalar_float(metrics.get("avg_volume"))
    max_volume = scalar_float(metrics.get("max_volume"))
    tech_runs = scalar_int(metrics.get("tech_runs"))
    both_ok_runs = scalar_int(metrics.get("both_ok_runs"))

    tech_unusable = tech_runs >= MIN_TECH_RUNS and both_ok_runs == 0
    quote_unusable = yf_runs >= MIN_YF_RUNS and quote_ok_runs == 0 and quote_bad_runs >= MIN_YF_RUNS
    low_volume = (
        yf_runs >= MIN_YF_RUNS
        and avg_volume is not None
        and max_volume is not None
        and avg_volume < LOW_AVG_VOLUME
        and max_volume < LOW_MAX_VOLUME
    )

    if sym in held_symbols:
        return "HELD_EXEMPT", "HELD_POSITION", "Position detenue: exclue de la quarantaine automatique."
    if tech_unusable:
        return "QUARANTINE", "TECH_DATA_UNUSABLE_30D", f"{tech_runs} runs techniques sur {LOOKBACK_DAYS}j, 0 avec H1 et D1 exploitables."
    if quote_unusable:
        return "QUARANTINE", "QUOTE_UNUSABLE_30D", f"{yf_runs} runs YF sur {LOOKBACK_DAYS}j, 0 quote exploitable."
    if low_volume:
        return "QUARANTINE", "LOW_VOLUME_30D", f"Volume moyen {avg_volume:.0f}, max {max_volume:.0f} sur {yf_runs} runs YF."
    if tech_runs >= 3 and both_ok_runs >= 2:
        return "RELEASE", "RECOVERED_DATA", f"{both_ok_runs}/{tech_runs} runs techniques exploitables."
    return "KEEP", "OK_OR_INSUFFICIENT_EVIDENCE", "Pas assez de preuves pour quarantainer."


items = _items or []
now = datetime.utcnow()
run_id = "AG2_UHQ_" + now.strftime("%Y%m%d%H%M%S")

con = None
transaction_started = False
summary = {
    "run_id": run_id,
    "rule_version": RULE_VERSION,
    "lookback_days": LOOKBACK_DAYS,
    "evaluated": 0,
    "newly_quarantined": 0,
    "released": 0,
    "held_exempt": 0,
    "active_quarantine": 0,
    "yf_attached": False,
    "ag1_attached": False,
    "ag3_attached": False,
    "segments": {},
    "new_quarantine_symbols": [],
    "released_symbols": [],
}

try:
    con = db_connect(AG2_DB_PATH)
    ensure_schema(con)
    summary["yf_attached"] = attach_readonly(con, YF_DB_PATH, "yf")
    summary["ag1_attached"] = attach_readonly(con, AG1_DB_PATH, "ag1")
    summary["ag3_attached"] = attach_readonly(con, AG3_DB_PATH, "ag3")
    if not (summary["yf_attached"] and summary["ag1_attached"] and summary["ag3_attached"]):
        raise RuntimeError("UHQ_REQUIRED_DEPENDENCY_UNAVAILABLE")

    con.execute(
        """
        INSERT OR REPLACE INTO universe_quarantine_audit_runs (
          run_id, started_at, status, rule_version
        ) VALUES (?, CURRENT_TIMESTAMP, 'RUNNING', ?)
        """,
        [run_id, RULE_VERSION],
    )

    held_symbols = load_held_symbols(con) if summary["ag1_attached"] else set()
    yf_metrics = load_yf_metrics(con) if summary["yf_attached"] else {}

    universe_rows = rows_as_dicts(
        con,
        f"""
        WITH u AS (
          SELECT
            UPPER(TRIM(symbol)) AS symbol,
            UPPER(TRIM(COALESCE(symbol_yahoo, symbol))) AS symbol_yahoo,
            UPPER(TRIM(COALESCE(asset_class, 'EQUITY'))) AS asset_class,
            COALESCE(exchange, '') AS exchange,
            COALESCE(country, '') AS country
          FROM universe
          WHERE symbol IS NOT NULL
            AND TRIM(symbol) <> ''
            AND COALESCE(enabled, TRUE)
            AND UPPER(TRIM(COALESCE(asset_class, 'EQUITY'))) IN ('EQUITY', 'ETF', 'CRYPTO')
            AND UPPER(TRIM(COALESCE(symbol_yahoo, symbol))) NOT LIKE '%=X'
        ),
        tech AS (
          SELECT
            UPPER(TRIM(symbol)) AS symbol,
            SUM(CASE WHEN COALESCE(h1_closed_only, FALSE)
                       AND COALESCE(d1_closed_only, FALSE) THEN 1 ELSE 0 END) AS tech_runs,
            SUM(CASE
              WHEN COALESCE(h1_closed_only, FALSE)
               AND COALESCE(d1_closed_only, FALSE)
               AND COALESCE(h1_status, '') = 'OK'
               AND COALESCE(d1_status, '') = 'OK'
               AND date_diff('hour', h1_date, CURRENT_TIMESTAMP) <= 72
               AND date_diff('hour', d1_date, CURRENT_TIMESTAMP) <= 96
               AND COALESCE(last_close, 0) > 0
              THEN 1 ELSE 0 END) AS both_ok_runs,
            SUM(CASE WHEN COALESCE(pass_ai, FALSE) THEN 1 ELSE 0 END) AS pass_ai_runs,
            SUM(CASE WHEN COALESCE(pass_pm, FALSE) THEN 1 ELSE 0 END) AS pass_pm_runs,
            MAX(COALESCE(workflow_date, updated_at, created_at)) AS latest_tech_ts
          FROM technical_signals
          WHERE symbol IS NOT NULL
            AND COALESCE(workflow_date, updated_at, created_at) >= CURRENT_TIMESTAMP - INTERVAL '{LOOKBACK_DAYS} days'
          GROUP BY 1
        )
        SELECT
          u.symbol,
          u.symbol_yahoo,
          u.asset_class,
          u.exchange,
          u.country,
          COALESCE(tech.tech_runs, 0) AS tech_runs,
          COALESCE(tech.both_ok_runs, 0) AS both_ok_runs,
          COALESCE(tech.pass_ai_runs, 0) AS pass_ai_runs,
          COALESCE(tech.pass_pm_runs, 0) AS pass_pm_runs,
          tech.latest_tech_ts
        FROM u
        LEFT JOIN tech ON tech.symbol = u.symbol
        ORDER BY u.symbol
        """
    )

    existing_rows = rows_as_dicts(
        con,
        "SELECT symbol, active, manual_override, first_quarantined_at FROM universe_quarantine",
    )
    existing = {text(r.get("symbol")).upper(): r for r in existing_rows if text(r.get("symbol"))}

    con.execute("BEGIN TRANSACTION")
    transaction_started = True
    history_rows = []
    for idx, r in enumerate(universe_rows):
        sym = text(r.get("symbol")).upper()
        if not sym:
            continue
        metrics = dict(r)
        metrics.update(yf_metrics.get(sym, {}))
        decision, reason, detail = decision_for_symbol(sym, metrics, held_symbols)
        current = existing.get(sym, {})
        active = truthy(current.get("active"))
        manual_override = truthy(current.get("manual_override"))
        metrics_json = json.dumps(metrics, ensure_ascii=False, default=str)

        final_decision = decision
        if manual_override:
            final_decision = "MANUAL_OVERRIDE"
        elif decision == "QUARANTINE":
            if not active:
                summary["newly_quarantined"] += 1
                summary["new_quarantine_symbols"].append(sym)
            first_at = current.get("first_quarantined_at") or now
            con.execute(
                """
                INSERT OR REPLACE INTO universe_quarantine (
                  symbol, symbol_yahoo, asset_class, reason, reason_detail, active,
                  first_quarantined_at, last_evaluated_at, last_released_at,
                  rule_version, metrics_json, manual_override, updated_at
                ) VALUES (?, ?, ?, ?, ?, TRUE, ?, CURRENT_TIMESTAMP, NULL, ?, ?, FALSE, CURRENT_TIMESTAMP)
                """,
                [
                    sym,
                    text(r.get("symbol_yahoo")).upper() or sym,
                    text(r.get("asset_class")).upper() or "EQUITY",
                    reason,
                    detail,
                    first_at,
                    RULE_VERSION,
                    metrics_json,
                ],
            )
        elif active and decision in ("RELEASE", "HELD_EXEMPT"):
            summary["released"] += 1
            summary["released_symbols"].append(sym)
            con.execute(
                """
                UPDATE universe_quarantine
                   SET active = FALSE,
                       reason = ?,
                       reason_detail = ?,
                       last_evaluated_at = CURRENT_TIMESTAMP,
                       last_released_at = CURRENT_TIMESTAMP,
                       rule_version = ?,
                       metrics_json = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE symbol = ?
                """,
                [reason, detail, RULE_VERSION, metrics_json, sym],
            )
            final_decision = "RELEASE"
        elif active:
            con.execute(
                """
                UPDATE universe_quarantine
                   SET last_evaluated_at = CURRENT_TIMESTAMP,
                       rule_version = ?,
                       metrics_json = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE symbol = ?
                """,
                [RULE_VERSION, metrics_json, sym],
            )
            final_decision = "KEEP_ACTIVE"
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO universe_quarantine (
                  symbol, symbol_yahoo, asset_class, reason, reason_detail, active,
                  first_quarantined_at, last_evaluated_at, last_released_at,
                  rule_version, metrics_json, manual_override, updated_at
                ) VALUES (?, ?, ?, ?, ?, FALSE, NULL, CURRENT_TIMESTAMP, NULL, ?, ?, FALSE, CURRENT_TIMESTAMP)
                """,
                [
                    sym,
                    text(r.get("symbol_yahoo")).upper() or sym,
                    text(r.get("asset_class")).upper() or "EQUITY",
                    reason,
                    detail,
                    RULE_VERSION,
                    metrics_json,
                ],
            )

        if decision == "HELD_EXEMPT":
            summary["held_exempt"] += 1
        history_rows.append(
            [
                run_id + "_" + str(idx).zfill(4) + "_" + safe_symbol(sym),
                run_id,
                sym,
                final_decision,
                reason,
                metrics_json,
            ]
        )

    for h in history_rows:
        con.execute(
            """
            INSERT OR REPLACE INTO universe_quarantine_audit_history (
              audit_id, run_id, symbol, decision, reason, metrics_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            h,
        )

    summary["segments"] = refresh_universe_segments(
        con,
        held_symbols,
        summary["yf_attached"],
        summary["ag3_attached"],
    )

    summary["evaluated"] = len(universe_rows)
    active_row = con.execute(
        "SELECT COUNT(*) FROM universe_quarantine WHERE COALESCE(active, FALSE)"
    ).fetchone()
    summary["active_quarantine"] = scalar_int(active_row[0] if active_row else 0)
    con.execute(
        """
        UPDATE universe_quarantine_audit_runs
           SET finished_at = CURRENT_TIMESTAMP,
               status = 'OK',
               evaluated_count = ?,
               active_quarantine_count = ?,
               newly_quarantined_count = ?,
               released_count = ?,
               held_exempt_count = ?,
               error_detail = NULL
         WHERE run_id = ?
        """,
        [
            summary["evaluated"],
            summary["active_quarantine"],
            summary["newly_quarantined"],
            summary["released"],
            summary["held_exempt"],
            run_id,
        ],
    )
    con.execute("COMMIT")
    transaction_started = False
except Exception as exc:
    summary["error"] = str(exc)[:1200]
    if con is not None:
        if transaction_started:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            transaction_started = False
        try:
            con.execute(
                """
                INSERT OR REPLACE INTO universe_quarantine_audit_runs (
                  run_id, started_at, finished_at, status, rule_version, error_detail
                ) VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAILED', ?, ?)
                """,
                [run_id, RULE_VERSION, summary["error"]],
            )
        except Exception:
            pass
    raise RuntimeError("AG2_UHQ_FAILED: " + summary["error"])
finally:
    if con is not None:
        try:
            con.close()
        except Exception:
            pass

return [{"json": {"ok": True, "summary": summary}}]
