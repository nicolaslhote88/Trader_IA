import duckdb
import gc
import time
from contextlib import contextmanager
from datetime import datetime, date

DEFAULT_DB_PATH = "/local-files/duckdb/ag1_v3_chatgpt52.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=6, base_delay=0.25):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
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


def as_sheet_value(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    s = str(v)
    if s.lower() in ("nan", "nat", "none", "null"):
        return ""
    return s


def pick_db_path(items):
    for it in items:
        j = (it or {}).get("json", {}) or {}
        for key in ("portfolio_db_path", "db_path", "duckdb_path"):
            val = j.get(key)
            if val:
                return str(val)
    return DEFAULT_DB_PATH


items = _items or []
db_path = pick_db_path(items)

query = """
SELECT
    row_number,
    symbol,
    name,
    asset_class,
    sector,
    industry,
    isin,
    quantity,
    avg_price,
    last_price,
    market_value,
    unrealized_pnl,
    updated_at
FROM portfolio_positions_mtm_latest
ORDER BY market_value DESC NULLS LAST, symbol
"""

out = []
with db_con(db_path) as con:
    rows = con.execute(query).fetchall()

for r in rows:
    out.append(
        {
            "json": {
                "row_number": r[0],
                "Symbol": as_sheet_value(r[1]),
                "Name": as_sheet_value(r[2]),
                "AssetClass": as_sheet_value(r[3]),
                "Sector": as_sheet_value(r[4]),
                "Industry": as_sheet_value(r[5]),
                "ISIN": as_sheet_value(r[6]),
                "Quantity": as_sheet_value(r[7]),
                "AvgPrice": as_sheet_value(r[8]),
                "LastPrice": as_sheet_value(r[9]),
                "MarketValue": as_sheet_value(r[10]),
                "UnrealizedPnL": as_sheet_value(r[11]),
                "UpdatedAt": as_sheet_value(r[12]),
            }
        }
    )

return out
