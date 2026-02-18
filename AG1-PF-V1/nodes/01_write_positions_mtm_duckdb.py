import duckdb
import gc
import time
from contextlib import contextmanager
from datetime import datetime, timezone

DEFAULT_DB_PATH = "/files/duckdb/ag1_v2.duckdb"
DEFAULT_WORKFLOW_NAME = "PF Portfolio MTM Updater"


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
        n = float(s)
        return n
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


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_run_id(items):
    for it in items:
        j = (it or {}).get("json", {}) or {}
        rid = to_text(pick(j.get("run_id"), j.get("pf_run_id"), j.get("workflow_run_id")))
        if rid:
            return rid
    return "PFMTM_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def pick_db_path(items):
    for it in items:
        j = (it or {}).get("json", {}) or {}
        path = to_text(pick(j.get("portfolio_db_path"), j.get("db_path"), j.get("duckdb_path")))
        if path:
            return path
    return DEFAULT_DB_PATH


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


items = _items or []
if not items:
    return []

run_id = build_run_id(items)
db_path = pick_db_path(items)
workflow_name = pick_workflow_name(items)
now_iso = utc_now_iso()

rows = []
for it in items:
    j = (it or {}).get("json", {}) or {}

    symbol_raw = to_text(pick(j.get("Symbol"), j.get("symbol")))
    symbol = symbol_raw.upper()
    if not symbol:
        continue

    # This workflow is dedicated to positions MTM. Skip technical rows if they leak in.
    if symbol in ("CASH_EUR", "__META__"):
        continue

    row_number = to_int(pick(j.get("row_number"), j.get("rowNumber"), j.get("row_number_src")))

    row = {
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

    rows.append(row)

rows_in = len(items)
rows_written = 0
rows_error = 0
error_detail = ""

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
                    INSERT OR REPLACE INTO portfolio_positions_mtm_latest
                    (
                        symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                        quantity, avg_price, last_price, market_value, unrealized_pnl,
                        updated_at, source_updated_at, run_id, ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
                        updated_at, source_updated_at, ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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

        final_status = "SUCCESS" if rows_error == 0 else "PARTIAL"

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
            [final_status, rows_written, rows_error, error_detail, run_id],
        )

except Exception as exc:
    # Keep workflow running; the row update to Google Sheets can still proceed.
    rows_error = max(rows_error, 1)
    error_detail = str(exc)[:900]

out = []
for it in items:
    j = (it or {}).get("json", {}) or {}
    jj = dict(j)
    jj["pf_duckdb_run_id"] = run_id
    jj["pf_duckdb_path"] = db_path
    jj["pf_duckdb_rows_in"] = rows_in
    jj["pf_duckdb_rows_written"] = rows_written
    jj["pf_duckdb_rows_error"] = rows_error
    jj["pf_duckdb_error"] = error_detail
    out.append({"json": jj})

return out
