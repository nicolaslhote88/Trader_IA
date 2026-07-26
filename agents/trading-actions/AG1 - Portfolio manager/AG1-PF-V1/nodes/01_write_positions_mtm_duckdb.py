import gc
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DEFAULT_WORKFLOW_NAME = "PF Portfolio MTM Updater (DuckDB-only, AG1-V4)"


@contextmanager
def db_con(path, retries=6, base_delay=0.25):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as exc:
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg) and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            raise

    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def pick(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def to_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if not s:
        return None

    s = (
        s.replace("EUR", "")
        .replace("eur", "")
        .replace("€", "")
        .replace("\u20ac", "")
        .replace(" ", "")
        .replace("\u00a0", "")
        .replace("\u202f", "")
    )

    # French number support: 1 234,56 -> 1234.56
    s = s.replace(".", "").replace(",", ".") if ("," in s and "." in s) else s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def to_int(v):
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def to_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    s = str(v or "").strip().lower()
    if not s:
        return default
    if s in ("1", "true", "yes", "y", "on", "enabled"):
        return True
    if s in ("0", "false", "no", "n", "off", "disabled"):
        return False
    return default


def to_text(v):
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() in ("nan", "nat", "none", "null") else s.strip()


def is_legacy_ag1_db_path(v):
    s = to_text(v).lower().replace("\\", "/")
    return s.endswith("/ag1_v2.duckdb")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_run_id(items):
    for it in items:
        j = (it or {}).get("json", {}) or {}
        rid = to_text(pick(j.get("workflow_run_id"), j.get("run_id"), j.get("pf_run_id")))
        if rid:
            return rid
    return "PFMTM_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _parse_paths_candidate(v):
    if v is None:
        return []

    if isinstance(v, (list, tuple, set)):
        return [p for p in (to_text(x) for x in v) if p]

    s = to_text(v)
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [p for p in (to_text(x) for x in parsed) if p]
        except Exception:
            pass

    if "," in s or ";" in s:
        parts = [to_text(x).strip().strip('"').strip("'") for x in re.split(r"[;,]", s.strip().strip("[]"))]
        return [p for p in parts if p]

    return [s]


def _dedupe_paths(paths):
    out = []
    seen = set()
    for p in paths:
        t = to_text(p)
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def _path_alias_candidates(path_text):
    p = to_text(path_text).replace("\\", "/")
    if not p:
        return []
    out = [p]
    if p.startswith("/local-files/"):
        out.append("/files/" + p[len("/local-files/"):])
    elif p.startswith("/files/"):
        out.append("/local-files/" + p[len("/files/"):])
    return _dedupe_paths(out)


def _resolve_rw_db_path(path_text):
    cands = _path_alias_candidates(path_text)
    if not cands:
        raise ValueError(f"Impossible de resoudre le chemin de la base de donnees pour: {path_text}")
    for p in cands:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            pass
    for p in cands:
        try:
            d = os.path.dirname(p)
            if d and os.path.isdir(d):
                return p
        except Exception:
            pass
    return cands[0]


def group_items_by_db(items):
    groups = {}
    order = []
    for it in items:
        j = (it or {}).get("json", {}) or {}
        db_path = to_text(pick(j.get("portfolio_db_path"), j.get("db_path"), j.get("duckdb_path")))
        if not db_path:
            raise ValueError("Erreur FATALE: portfolio_db_path manquant. Aucun fallback autorise.")
        if is_legacy_ag1_db_path(db_path):
            raise ValueError(f"Erreur FATALE: Chemin legacy detecte et rejete ({db_path}). Aucun fallback autorise.")
        db_path = _resolve_rw_db_path(db_path)
        if db_path not in groups:
            groups[db_path] = []
            order.append(db_path)
        groups[db_path].append(it)
    return groups, order


def pick_workflow_name(items):
    for it in items:
        j = (it or {}).get("json", {}) or {}
        name = to_text(j.get("workflow_name"))
        if name:
            return name
    return DEFAULT_WORKFLOW_NAME


def to_timestamp_candidate(v):
    s = to_text(v)
    return s or None


SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_run_log (
        run_id          VARCHAR PRIMARY KEY,
        started_at      TIMESTAMP NOT NULL,
        finished_at     TIMESTAMP,
        status          VARCHAR DEFAULT 'RUNNING',
        rows_in         INTEGER DEFAULT 0,
        rows_written    INTEGER DEFAULT 0,
        rows_error      INTEGER DEFAULT 0,
        error_detail    VARCHAR,
        source          VARCHAR DEFAULT 'PF_MTM',
        workflow_name   VARCHAR,
        source_run_ids  VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_latest (
        symbol                  VARCHAR PRIMARY KEY,
        row_number              INTEGER,
        symbol_raw              VARCHAR,
        name                    VARCHAR,
        asset_class             VARCHAR,
        sector                  VARCHAR,
        industry                VARCHAR,
        isin                    VARCHAR,
        quantity                DOUBLE,
        avg_price               DOUBLE,
        last_price              DOUBLE,
        market_value            DOUBLE,
        unrealized_pnl          DOUBLE,
        updated_at              TIMESTAMP,
        source_updated_at       VARCHAR,
        run_id                  VARCHAR,
        ag1_source_run_id       VARCHAR,
        ag1_source_snapshot_ts  VARCHAR,
        mtm_status              VARCHAR,
        mtm_reason              VARCHAR,
        mtm_price_source        VARCHAR,
        mtm_price_stale         BOOLEAN,
        ingested_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_history (
        id                      VARCHAR PRIMARY KEY,
        run_id                  VARCHAR NOT NULL,
        symbol                  VARCHAR NOT NULL,
        row_number              INTEGER,
        symbol_raw              VARCHAR,
        name                    VARCHAR,
        asset_class             VARCHAR,
        sector                  VARCHAR,
        industry                VARCHAR,
        isin                    VARCHAR,
        quantity                DOUBLE,
        avg_price               DOUBLE,
        last_price              DOUBLE,
        market_value            DOUBLE,
        unrealized_pnl          DOUBLE,
        updated_at              TIMESTAMP,
        source_updated_at       VARCHAR,
        ag1_source_run_id       VARCHAR,
        ag1_source_snapshot_ts  VARCHAR,
        mtm_status              VARCHAR,
        mtm_reason              VARCHAR,
        mtm_price_source        VARCHAR,
        mtm_price_stale         BOOLEAN,
        ingested_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_latest_updated_at ON portfolio_positions_mtm_latest(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_run ON portfolio_positions_mtm_history(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_symbol ON portfolio_positions_mtm_history(symbol)",
]

ALTER_SQL = [
    "ALTER TABLE portfolio_positions_mtm_run_log ADD COLUMN IF NOT EXISTS source_run_ids VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS sector VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS industry VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS isin VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS source_updated_at VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS ag1_source_run_id VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS ag1_source_snapshot_ts VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS mtm_status VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS mtm_reason VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS mtm_price_source VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_latest ADD COLUMN IF NOT EXISTS mtm_price_stale BOOLEAN",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS ag1_source_run_id VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS ag1_source_snapshot_ts VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS mtm_status VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS mtm_reason VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS mtm_price_source VARCHAR",
    "ALTER TABLE portfolio_positions_mtm_history ADD COLUMN IF NOT EXISTS mtm_price_stale BOOLEAN",
]


def _apply_fx_eur(con, rows):
    # FIX 2026-07-13 : PF.07 calcule market_value/last_price/unrealized_pnl en devise NATIVE
    # (pas de conversion FX) -> mtm_latest/history gonfles pour les titres USD, ce qui gonfle
    # la courbe de perf du dashboard. On convertit en EUR via la source autoritaire IBKR
    # (portfolio_positions_ibkr_latest : currency + fx_rate + last_price_eur), avec garde-fou
    # d'echelle (anti double-conversion). avg_price est deja en EUR. Gardee : ne casse jamais l'ecriture.
    try:
        fx = {}
        try:
            for r in con.execute(
                "SELECT UPPER(TRIM(symbol)), UPPER(COALESCE(currency,'')), CAST(fx_rate AS DOUBLE), CAST(last_price_eur AS DOUBLE) "
                "FROM portfolio_positions_ibkr_latest"
            ).fetchall():
                if r[0]:
                    fx[r[0]] = (r[1], r[2], r[3])
        except Exception:
            return
        for row in rows:
            sym = str(row.get("symbol") or "").upper().strip()
            if not sym or sym in ("CASH_EUR", "__META__"):
                continue
            ent = fx.get(sym)
            if not ent:
                continue
            cur, rate, lp_ref = ent
            if not cur or cur == "EUR" or not rate or rate <= 0:
                continue
            last = to_float(row.get("last_price"))
            qty = to_float(row.get("quantity"))
            if last is None or last <= 0 or qty is None:
                continue
            cand = last * rate
            if lp_ref is not None and lp_ref > 0 and not (abs(cand - lp_ref) < abs(last - lp_ref)):
                continue
            row["last_price"] = cand
            row["market_value"] = qty * cand
            avgp = to_float(row.get("avg_price"))
            if avgp is not None:
                row["unrealized_pnl"] = (cand - avgp) * qty
    except Exception:
        return


def build_rows(items, run_id, now_iso):
    rows = []

    for it in items:
        j = (it or {}).get("json", {}) or {}

        symbol_raw = to_text(pick(j.get("Symbol"), j.get("symbol")))
        symbol = symbol_raw.upper()
        if not symbol:
            continue

        # Position rows only; technical rows are persisted from shared context below.
        if symbol in ("CASH_EUR", "__META__"):
            continue

        row_number = to_int(pick(j.get("row_number"), j.get("rowNumber"), j.get("row_number_src")))

        rows.append(
            {
                "id": f"{run_id}|{symbol}|{row_number if row_number is not None else 0}",
                "run_id": run_id,
                "symbol": symbol,
                "row_number": row_number,
                "symbol_raw": symbol_raw,
                "name": to_text(pick(j.get("Name"), j.get("name"))),
                "asset_class": to_text(pick(j.get("AssetClass"), j.get("assetClass"), j.get("asset_class"))),
                "sector": to_text(pick(j.get("Sector"), j.get("sector"))),
                "industry": to_text(pick(j.get("Industry"), j.get("industry"))),
                "isin": to_text(pick(j.get("ISIN"), j.get("isin"))),
                "quantity": to_float(pick(j.get("Quantity"), j.get("qty"))),
                "avg_price": to_float(pick(j.get("AvgPrice"), j.get("avgPrice"))),
                "last_price": to_float(j.get("LastPrice")),
                "market_value": to_float(j.get("MarketValue")),
                "unrealized_pnl": to_float(j.get("UnrealizedPnL")),
                "updated_at": to_timestamp_candidate(pick(j.get("UpdatedAt"), now_iso)),
                "source_updated_at": to_text(pick(j.get("mtm_price_asof"), j.get("UpdatedAt"), now_iso)),
                "ag1_source_run_id": to_text(j.get("ag1_source_run_id")) or None,
                "ag1_source_snapshot_ts": to_text(pick(j.get("ag1_source_snapshot_ts"), j.get("UpdatedAt"), now_iso)),
                "mtm_status": to_text(j.get("mtm_status")) or None,
                "mtm_reason": to_text(j.get("mtm_reason")) or None,
                "mtm_price_source": to_text(j.get("mtm_price_source")) or None,
                "mtm_price_stale": to_bool(j.get("mtm_price_stale"), default=False),
            }
        )

    # Persist technical portfolio rows (cash + meta) from shared context.
    shared = (items[0] or {}).get("json", {}) or {}
    cash_mv = to_float(pick(shared.get("pf_cash_market_value"), shared.get("cash_market_value_eur")))
    cash_updated_at = to_timestamp_candidate(pick(shared.get("pf_cash_updated_at"), now_iso))
    initial_capital = to_float(pick(shared.get("pf_initial_capital"), shared.get("initial_capital_eur")))
    meta_updated_at = to_timestamp_candidate(pick(shared.get("pf_meta_updated_at"), now_iso))
    ag1_source_run_id = to_text(shared.get("ag1_source_run_id")) or None
    ag1_source_snapshot_ts = to_text(pick(shared.get("ag1_source_snapshot_ts"), shared.get("UpdatedAt"), now_iso))

    if cash_mv is not None:
        rows.append(
            {
                "id": f"{run_id}|CASH_EUR|0",
                "run_id": run_id,
                "symbol": "CASH_EUR",
                "row_number": 0,
                "symbol_raw": "CASH_EUR",
                "name": "Cash",
                "asset_class": "Cash",
                "sector": "Cash",
                "industry": "Cash",
                "isin": "",
                "quantity": 0.0,
                "avg_price": 1.0,
                "last_price": 1.0,
                "market_value": cash_mv,
                "unrealized_pnl": 0.0,
                "updated_at": cash_updated_at,
                "source_updated_at": to_text(cash_updated_at or now_iso),
                "ag1_source_run_id": ag1_source_run_id,
                "ag1_source_snapshot_ts": ag1_source_snapshot_ts,
                "mtm_status": "TECHNICAL_ROW",
                "mtm_reason": None,
                "mtm_price_source": "portfolio_context",
                "mtm_price_stale": False,
            }
        )

    if initial_capital is not None and initial_capital > 0:
        rows.append(
            {
                "id": f"{run_id}|__META__|0",
                "run_id": run_id,
                "symbol": "__META__",
                "row_number": 0,
                "symbol_raw": "__META__",
                "name": "__META__",
                "asset_class": "Meta",
                "sector": "",
                "industry": "",
                "isin": "",
                "quantity": None,
                "avg_price": None,
                "last_price": None,
                "market_value": initial_capital,
                "unrealized_pnl": None,
                "updated_at": meta_updated_at,
                "source_updated_at": to_text(meta_updated_at or now_iso),
                "ag1_source_run_id": ag1_source_run_id,
                "ag1_source_snapshot_ts": ag1_source_snapshot_ts,
                "mtm_status": "TECHNICAL_ROW",
                "mtm_reason": None,
                "mtm_price_source": "portfolio_context",
                "mtm_price_stale": False,
            }
        )

    return rows


def ensure_schema(con):
    for stmt in SCHEMA_SQL:
        con.execute(stmt)
    for stmt in ALTER_SQL:
        try:
            con.execute(stmt)
        except Exception:
            pass


def collect_source_run_ids(rows):
    uniq = sorted({to_text(r.get("ag1_source_run_id")) for r in rows if to_text(r.get("ag1_source_run_id"))})
    return json.dumps(uniq, ensure_ascii=False)


def write_rows_to_db(
    db_path,
    rows,
    rows_in,
    run_id,
    workflow_name,
    purge_latest_before_write=True,
    write_only_if_any_price_found=False,
):
    rows_written = 0
    rows_error = 0
    error_detail = ""
    status = "SUCCESS"
    source_run_ids = collect_source_run_ids(rows)

    try:
        with db_con(db_path) as con:
            ensure_schema(con)
            _apply_fx_eur(con, rows)
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    "DELETE FROM portfolio_positions_mtm_run_log WHERE run_id = ?",
                    [run_id],
                )
                con.execute(
                    """
                    INSERT INTO portfolio_positions_mtm_run_log
                      (run_id, started_at, finished_at, status, rows_in, rows_written, rows_error, error_detail, source, workflow_name, source_run_ids)
                    VALUES
                      (?, CURRENT_TIMESTAMP, NULL, 'RUNNING', ?, 0, 0, '', 'PF_MTM', ?, ?)
                    """,
                    [run_id, rows_in, workflow_name, source_run_ids],
                )

                has_any_price = any(
                    (r.get("symbol") not in ("__META__", "CASH_EUR"))
                    and (to_float(r.get("last_price")) or 0) > 0
                    for r in rows
                )
                if write_only_if_any_price_found and not has_any_price:
                    status = "SKIPPED_NO_PRICE"
                    con.execute(
                        """
                        UPDATE portfolio_positions_mtm_run_log
                        SET finished_at = CURRENT_TIMESTAMP,
                            status = ?,
                            rows_written = 0,
                            rows_error = 0,
                            error_detail = '',
                            source_run_ids = ?
                        WHERE run_id = ?
                        """,
                        [status, source_run_ids, run_id],
                    )
                    con.execute("COMMIT")
                    return {
                        "db_path": db_path,
                        "status": status,
                        "rows_in": rows_in,
                        "rows_payload": len(rows),
                        "rows_written": 0,
                        "rows_error": 0,
                        "error_detail": "",
                        "source_run_ids": source_run_ids,
                    }

                if purge_latest_before_write:
                    con.execute(
                        """
                        DELETE FROM portfolio_positions_mtm_latest
                        WHERE UPPER(symbol) NOT IN ('__META__', 'CASH_EUR')
                        """
                    )

                for row in rows:
                    con.execute(
                        "DELETE FROM portfolio_positions_mtm_latest WHERE UPPER(symbol) = UPPER(?)",
                        [row["symbol"]],
                    )
                    con.execute(
                        """
                        INSERT INTO portfolio_positions_mtm_latest
                        (
                            symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                            quantity, avg_price, last_price, market_value, unrealized_pnl,
                            updated_at, source_updated_at, run_id,
                            ag1_source_run_id, ag1_source_snapshot_ts, mtm_status, mtm_reason,
                            mtm_price_source, mtm_price_stale, ingested_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        [
                            row["symbol"], row["row_number"], row["symbol_raw"], row["name"],
                            row["asset_class"], row["sector"], row["industry"], row["isin"],
                            row["quantity"], row["avg_price"], row["last_price"], row["market_value"],
                            row["unrealized_pnl"], row["updated_at"], row["source_updated_at"],
                            row["run_id"], row["ag1_source_run_id"], row["ag1_source_snapshot_ts"],
                            row["mtm_status"], row["mtm_reason"], row["mtm_price_source"],
                            row["mtm_price_stale"],
                        ],
                    )

                    con.execute(
                        "DELETE FROM portfolio_positions_mtm_history WHERE id = ?",
                        [row["id"]],
                    )
                    con.execute(
                        """
                        INSERT INTO portfolio_positions_mtm_history
                        (
                            id, run_id, symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                            quantity, avg_price, last_price, market_value, unrealized_pnl,
                            updated_at, source_updated_at,
                            ag1_source_run_id, ag1_source_snapshot_ts, mtm_status, mtm_reason, mtm_price_source, mtm_price_stale
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            row["id"], row["run_id"], row["symbol"], row["row_number"], row["symbol_raw"],
                            row["name"], row["asset_class"], row["sector"], row["industry"], row["isin"],
                            row["quantity"], row["avg_price"], row["last_price"], row["market_value"],
                            row["unrealized_pnl"], row["updated_at"], row["source_updated_at"],
                            row["ag1_source_run_id"], row["ag1_source_snapshot_ts"], row["mtm_status"], row["mtm_reason"], row["mtm_price_source"], row["mtm_price_stale"],
                        ],
                    )
                    rows_written += 1

                con.execute(
                    """
                    UPDATE portfolio_positions_mtm_run_log
                    SET finished_at = CURRENT_TIMESTAMP,
                        status = 'SUCCESS',
                        rows_written = ?,
                        rows_error = 0,
                        error_detail = '',
                        source_run_ids = ?
                    WHERE run_id = ?
                    """,
                    [rows_written, source_run_ids, run_id],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    except Exception as exc:
        rows_written = 0
        rows_error = max(len(rows), 1)
        error_detail = str(exc)[:900]
        status = "FAILED"
        try:
            with db_con(db_path) as con:
                ensure_schema(con)
                con.execute(
                    "DELETE FROM portfolio_positions_mtm_run_log WHERE run_id = ?",
                    [run_id],
                )
                con.execute(
                    """
                    INSERT INTO portfolio_positions_mtm_run_log
                      (run_id, started_at, finished_at, status, rows_in, rows_written,
                       rows_error, error_detail, source, workflow_name, source_run_ids)
                    VALUES
                      (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAILED', ?, 0, ?, ?,
                       'PF_MTM', ?, ?)
                    """,
                    [
                        run_id,
                        rows_in,
                        rows_error,
                        error_detail,
                        workflow_name,
                        source_run_ids,
                    ],
                )
        except Exception:
            pass

    return {
        "db_path": db_path,
        "status": status,
        "rows_in": rows_in,
        "rows_payload": len(rows),
        "rows_written": rows_written,
        "rows_error": rows_error,
        "error_detail": error_detail,
        "source_run_ids": source_run_ids,
    }


def extract_flags(items):
    purge = True
    write_only = False
    for it in items:
        j = (it or {}).get("json", {}) or {}
        if "purge_latest_before_write" in j:
            purge = to_bool(j.get("purge_latest_before_write"), default=True)
            break
    for it in items:
        j = (it or {}).get("json", {}) or {}
        if "write_only_if_any_price_found" in j:
            write_only = to_bool(j.get("write_only_if_any_price_found"), default=False)
            break
    return purge, write_only


items = _items or []
if not items:
    return []

run_id = build_run_id(items)
workflow_name = pick_workflow_name(items)
now_iso = utc_now_iso()

groups, db_paths = group_items_by_db(items)
rows_in_total = len(items)

target_results = []
target_result_by_db = {}
for target_db_path in db_paths:
    group_items = groups.get(target_db_path, [])
    purge_latest, write_only = extract_flags(group_items)
    rows = build_rows(group_items, run_id=run_id, now_iso=now_iso)
    target_results.append(
        write_rows_to_db(
            db_path=target_db_path,
            rows=rows,
            rows_in=len(group_items),
            run_id=run_id,
            workflow_name=workflow_name,
            purge_latest_before_write=purge_latest,
            write_only_if_any_price_found=write_only,
        )
    )
    target_result_by_db[target_db_path] = target_results[-1]

rows_written_total = sum(int(r.get("rows_written") or 0) for r in target_results)
rows_error_total = sum(int(r.get("rows_error") or 0) for r in target_results)
targets_ok = sum(1 for r in target_results if str(r.get("status")) in ("SUCCESS", "SKIPPED_NO_PRICE"))
targets_failed = sum(1 for r in target_results if str(r.get("status")) not in ("SUCCESS", "SKIPPED_NO_PRICE"))

errors_compact = [f"{r.get('db_path')}: {r.get('error_detail')}" for r in target_results if r.get("error_detail")]
error_detail_all = " | ".join(errors_compact)[:1800] if errors_compact else ""

if rows_error_total or targets_failed:
    raise RuntimeError(
        "PF_MTM_DUCKDB_WRITE_FAILED: "
        f"targets_failed={targets_failed}, rows_error={rows_error_total}, "
        f"detail={error_detail_all or 'unknown'}"
    )

out = []
for it in items:
    j = (it or {}).get("json", {}) or {}
    jj = dict(j)
    item_db_path = to_text(pick(j.get("portfolio_db_path"), j.get("db_path"), j.get("duckdb_path")))
    item_db_resolved = _resolve_rw_db_path(item_db_path) if item_db_path else ""
    item_res = target_result_by_db.get(item_db_resolved)
    if item_res is None:
        item_res = {
            "db_path": item_db_resolved or item_db_path,
            "rows_in": 0,
            "rows_written": 0,
            "rows_error": 1,
            "error_detail": "No target result",
            "status": "FAILED",
            "source_run_ids": "[]",
        }
    jj["pf_duckdb_run_id"] = run_id
    jj["pf_duckdb_path"] = item_res.get("db_path", item_db_resolved or item_db_path)
    jj["pf_duckdb_paths"] = db_paths
    jj["pf_duckdb_target_results"] = target_results
    jj["pf_duckdb_target_count"] = len(db_paths)
    jj["pf_duckdb_targets_ok"] = targets_ok
    jj["pf_duckdb_targets_failed"] = targets_failed
    jj["pf_duckdb_rows_in"] = int(item_res.get("rows_in") or 0)
    jj["pf_duckdb_rows_in_total"] = rows_in_total
    # Legacy fields preserved (now mapped to the item's own portfolio DB target).
    jj["pf_duckdb_rows_written"] = int(item_res.get("rows_written") or 0)
    jj["pf_duckdb_rows_error"] = int(item_res.get("rows_error") or 0)
    jj["pf_duckdb_status"] = to_text(item_res.get("status"))
    jj["pf_duckdb_error"] = to_text(item_res.get("error_detail"))
    jj["pf_duckdb_source_run_ids"] = to_text(item_res.get("source_run_ids"))
    # Aggregate fields for multi-target observability.
    jj["pf_duckdb_rows_written_total"] = rows_written_total
    jj["pf_duckdb_rows_error_total"] = rows_error_total
    jj["pf_duckdb_error_all"] = error_detail_all
    out.append({"json": jj})

return out
