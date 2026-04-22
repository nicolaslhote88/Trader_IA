import gc
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DEFAULT_DB_PATH = "/local-files/duckdb/ag1_v3_chatgpt52.duckdb"
DEFAULT_WORKFLOW_NAME = "PF Portfolio MTM Updater (DuckDB-only, Multi AG1-V3)"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=6, base_delay=0.25):
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
        .replace("â‚¬", "")
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
        rid = to_text(pick(j.get("run_id"), j.get("pf_run_id"), j.get("workflow_run_id")))
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
        return DEFAULT_DB_PATH
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
        db_path = to_text(pick(j.get("portfolio_db_path"), j.get("db_path"), j.get("duckdb_path"))) or DEFAULT_DB_PATH
        if is_legacy_ag1_db_path(db_path):
            # Ignore stale mono-DB target and force dedicated AG1-v3 default.
            db_path = DEFAULT_DB_PATH
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
        workflow_name   VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_latest (
        symbol              VARCHAR PRIMARY KEY,
        row_number          INTEGER,
        symbol_raw          VARCHAR,
        name                VARCHAR,
        asset_class         VARCHAR,
        sector              VARCHAR,
        industry            VARCHAR,
        isin                VARCHAR,
        quantity            DOUBLE,
        avg_price           DOUBLE,
        last_price          DOUBLE,
        market_value        DOUBLE,
        unrealized_pnl      DOUBLE,
        updated_at          TIMESTAMP,
        source_updated_at   VARCHAR,
        run_id              VARCHAR,
        ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_history (
        id                  VARCHAR PRIMARY KEY,
        run_id              VARCHAR NOT NULL,
        symbol              VARCHAR NOT NULL,
        row_number          INTEGER,
        symbol_raw          VARCHAR,
        name                VARCHAR,
        asset_class         VARCHAR,
        sector              VARCHAR,
        industry            VARCHAR,
        isin                VARCHAR,
        quantity            DOUBLE,
        avg_price           DOUBLE,
        last_price          DOUBLE,
        market_value        DOUBLE,
        unrealized_pnl      DOUBLE,
        updated_at          TIMESTAMP,
        source_updated_at   VARCHAR,
        ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_latest_updated_at ON portfolio_positions_mtm_latest(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_run ON portfolio_positions_mtm_history(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_pf_mtm_history_symbol ON portfolio_positions_mtm_history(symbol)",
]


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
            }
        )

    # Persist technical portfolio rows (cash + meta) from shared context.
    shared = (items[0] or {}).get("json", {}) or {}
    cash_mv = to_float(pick(shared.get("pf_cash_market_value"), shared.get("cash_market_value_eur")))
    cash_updated_at = to_timestamp_candidate(pick(shared.get("pf_cash_updated_at"), now_iso))
    initial_capital = to_float(pick(shared.get("pf_initial_capital"), shared.get("initial_capital_eur")))
    meta_updated_at = to_timestamp_candidate(pick(shared.get("pf_meta_updated_at"), now_iso))

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
            }
        )

    return rows


def write_rows_to_db(db_path, rows, rows_in, run_id, workflow_name):
    rows_written = 0
    rows_error = 0
    error_detail = ""
    status = "SUCCESS"

    try:
        with db_con(db_path) as con:
            for stmt in SCHEMA_SQL:
                con.execute(stmt)

            con.execute(
                """
                INSERT OR REPLACE INTO portfolio_positions_mtm_run_log
                  (run_id, started_at, finished_at, status, rows_in, rows_written, rows_error, error_detail, source, workflow_name)
                VALUES
                  (?, CURRENT_TIMESTAMP, NULL, 'RUNNING', ?, 0, 0, '', 'PF_MTM', ?)
                """,
                [run_id, rows_in, workflow_name],
            )

            for row in rows:
                try:
                    con.execute(
                        """
                        INSERT INTO portfolio_positions_mtm_latest
                        (
                            symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                            quantity, avg_price, last_price, market_value, unrealized_pnl,
                            updated_at, source_updated_at, run_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (symbol) DO UPDATE SET
                            row_number        = excluded.row_number,
                            symbol_raw        = excluded.symbol_raw,
                            quantity          = excluded.quantity,
                            avg_price         = excluded.avg_price,
                            last_price        = excluded.last_price,
                            market_value      = excluded.market_value,
                            unrealized_pnl    = excluded.unrealized_pnl,
                            updated_at        = excluded.updated_at,
                            source_updated_at = excluded.source_updated_at,
                            run_id            = excluded.run_id,
                            name       = CASE WHEN excluded.name       IS NOT NULL AND excluded.name       <> '' THEN excluded.name       ELSE portfolio_positions_mtm_latest.name       END,
                            asset_class= CASE WHEN excluded.asset_class IS NOT NULL AND excluded.asset_class <> '' THEN excluded.asset_class ELSE portfolio_positions_mtm_latest.asset_class END,
                            sector     = CASE WHEN excluded.sector     IS NOT NULL AND excluded.sector     <> '' THEN excluded.sector     ELSE portfolio_positions_mtm_latest.sector     END,
                            industry   = CASE WHEN excluded.industry   IS NOT NULL AND excluded.industry   <> '' THEN excluded.industry   ELSE portfolio_positions_mtm_latest.industry   END,
                            isin       = CASE WHEN excluded.isin       IS NOT NULL AND excluded.isin       <> '' THEN excluded.isin       ELSE portfolio_positions_mtm_latest.isin       END
                        """,
                        [
                            row["symbol"], row["row_number"], row["symbol_raw"], row["name"], row["asset_class"],
                            row["sector"], row["industry"], row["isin"], row["quantity"], row["avg_price"],
                            row["last_price"], row["market_value"], row["unrealized_pnl"],
                            row["updated_at"], row["source_updated_at"], row["run_id"],
                        ],
                    )

                    con.execute(
                        """
                        INSERT OR REPLACE INTO portfolio_positions_mtm_history
                        (
                            id, run_id, symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                            quantity, avg_price, last_price, market_value, unrealized_pnl,
                            updated_at, source_updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            row["id"], row["run_id"], row["symbol"], row["row_number"], row["symbol_raw"],
                            row["name"], row["asset_class"], row["sector"], row["industry"], row["isin"],
                            row["quantity"], row["avg_price"], row["last_price"], row["market_value"],
                            row["unrealized_pnl"], row["updated_at"], row["source_updated_at"],
                        ],
                    )

                    rows_written += 1
                except Exception as row_exc:
                    rows_error += 1
                    if not error_detail:
                        error_detail = str(row_exc)[:900]

            status = "SUCCESS" if rows_error == 0 else "PARTIAL"
            con.execute(
                """
                UPDATE portfolio_positions_mtm_run_log
                SET
                    finished_at = CURRENT_TIMESTAMP,
                    status = ?,
                    rows_written = ?,
                    rows_error = ?,
                    error_detail = ?
                WHERE run_id = ?
                """,
                [status, rows_written, rows_error, error_detail, run_id],
            )

    except Exception as exc:
        rows_error = max(rows_error, 1)
        error_detail = str(exc)[:900]
        status = "FAILED"

    return {
        "db_path": db_path,
        "status": status,
        "rows_in": rows_in,
        "rows_payload": len(rows),
        "rows_written": rows_written,
        "rows_error": rows_error,
        "error_detail": error_detail,
    }


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
    rows = build_rows(group_items, run_id=run_id, now_iso=now_iso)
    target_results.append(
        write_rows_to_db(
            db_path=target_db_path,
            rows=rows,
            rows_in=len(group_items),
            run_id=run_id,
            workflow_name=workflow_name,
        )
    )
    target_result_by_db[target_db_path] = target_results[-1]

rows_written_total = sum(int(r.get("rows_written") or 0) for r in target_results)
rows_error_total = sum(int(r.get("rows_error") or 0) for r in target_results)
targets_ok = sum(1 for r in target_results if str(r.get("status")) in ("SUCCESS", "PARTIAL"))
targets_failed = sum(1 for r in target_results if str(r.get("status")) == "FAILED")

errors_compact = [f"{r.get('db_path')}: {r.get('error_detail')}" for r in target_results if r.get("error_detail")]
error_detail_all = " | ".join(errors_compact)[:1800] if errors_compact else ""

out = []
for it in items:
    j = (it or {}).get("json", {}) or {}
    jj = dict(j)
    item_db_path = to_text(pick(j.get("portfolio_db_path"), j.get("db_path"), j.get("duckdb_path"))) or DEFAULT_DB_PATH
    item_res = target_result_by_db.get(item_db_path)
    if item_res is None and target_results:
        item_res = target_results[0]
    if item_res is None:
        item_res = {
            "db_path": item_db_path,
            "rows_in": 0,
            "rows_written": 0,
            "rows_error": 1,
            "error_detail": "No target result",
            "status": "FAILED",
        }
    jj["pf_duckdb_run_id"] = run_id
    jj["pf_duckdb_path"] = item_res.get("db_path", item_db_path)
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
    jj["pf_duckdb_error"] = to_text(item_res.get("error_detail"))
    # Aggregate fields for multi-target observability.
    jj["pf_duckdb_rows_written_total"] = rows_written_total
    jj["pf_duckdb_rows_error_total"] = rows_error_total
    jj["pf_duckdb_error_all"] = error_detail_all
    out.append({"json": jj})

return out
