
import os
import gc
import time
import duckdb
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH_DEFAULT = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v2.duckdb")

def to_num(v):
    try:
        if v is None or v == "":
            return None
        s = str(v).replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "").replace(",", ".")
        n = float(s)
        return n if n == n else None
    except Exception:
        return None

def to_iso(v):
    try:
        if v is None:
            return "unknown"
        if isinstance(v, datetime):
            dt = v
        else:
            s = str(v).strip()
            if not s:
                return "unknown"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(v) if v is not None else "unknown"

@contextmanager
def db_con(path, retries=5, delay=0.25):
    con = None
    for i in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as e:
            if ("lock" in str(e).lower() or "busy" in str(e).lower()) and i < retries - 1:
                time.sleep(delay * (2 ** i))
            else:
                con = None
                break
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()

def load_rows_from_duckdb(db_path):
    rows = []
    with db_con(db_path) as con:
        if con is None:
            return rows
        sql_with_review = """
            SELECT
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
              updated_at,
              next_review_date
            FROM portfolio_positions_mtm_latest
            ORDER BY market_value DESC NULLS LAST, symbol
        """
        sql_fallback = """
            SELECT
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
        try:
            cur = con.execute(sql_with_review)
            cols = [d[0] for d in cur.description]
            out = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception:
            try:
                cur = con.execute(sql_fallback)
                cols = [d[0] for d in cur.description]
                out = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception:
                out = []

        for idx, r in enumerate(out, start=1):
            rows.append({
                "row_number": idx,
                "Symbol": str(r.get("symbol") or "").strip().upper(),
                "Name": str(r.get("name") or "").strip(),
                "AssetClass": str(r.get("asset_class") or "").strip(),
                "Sector": str(r.get("sector") or "Unknown").strip() or "Unknown",
                "Industry": str(r.get("industry") or "").strip(),
                "ISIN": str(r.get("isin") or "").strip(),
                "Quantity": to_num(r.get("quantity")),
                "AvgPrice": to_num(r.get("avg_price")),
                "LastPrice": to_num(r.get("last_price")),
                "MarketValue": to_num(r.get("market_value")),
                "UnrealizedPnL": to_num(r.get("unrealized_pnl")),
                "UpdatedAt": to_iso(r.get("updated_at")),
                "NextReviewDate": to_iso(r.get("next_review_date")) if r.get("next_review_date") is not None else None,
            })
    return rows


incoming = _items or []

db_path = DB_PATH_DEFAULT
for it in incoming:
    j = it.get("json", {}) if isinstance(it, dict) else {}
    if isinstance(j, dict) and j.get("ag1_db_path"):
        db_path = str(j.get("ag1_db_path"))

rows = load_rows_from_duckdb(db_path)
source = "duckdb"

if not rows:
    source = "sheets_fallback"
    rows = []
    for idx, it in enumerate(incoming, start=1):
        r = it.get("json", {}) if isinstance(it, dict) else {}
        rows.append({
            "row_number": r.get("row_number", idx),
            "Symbol": r.get("Symbol", r.get("symbol", "")),
            "Name": r.get("Name", r.get("name", "")),
            "AssetClass": r.get("AssetClass", r.get("assetClass", "")),
            "Sector": r.get("Sector", r.get("sector", "Unknown")) or "Unknown",
            "Industry": r.get("Industry", r.get("industry", "")),
            "ISIN": r.get("ISIN", ""),
            "Quantity": to_num(r.get("Quantity")),
            "AvgPrice": to_num(r.get("AvgPrice")),
            "LastPrice": to_num(r.get("LastPrice")),
            "MarketValue": to_num(r.get("MarketValue")),
            "UnrealizedPnL": to_num(r.get("UnrealizedPnL")),
            "UpdatedAt": r.get("UpdatedAt", "unknown"),
            "NextReviewDate": r.get("NextReviewDate"),
        })

cash_row = next((r for r in rows if str(r.get("Symbol", "")).upper() == "CASH_EUR"), None)
meta_row = next((r for r in rows if str(r.get("Symbol", "")).upper() == "__META__"), None)

cash_eur = to_num(cash_row.get("MarketValue")) if cash_row else 0.0
cash_eur = cash_eur if cash_eur is not None else 0.0

positions = []
for r in rows:
    sym = str(r.get("Symbol", "")).upper()
    if not sym or sym in ("CASH_EUR", "__META__"):
        continue
    positions.append(r)

positions_market_value = 0.0
for p in positions:
    mv = to_num(p.get("MarketValue"))
    positions_market_value += mv if mv is not None else 0.0

total_value = cash_eur + positions_market_value
initial_capital = to_num(meta_row.get("MarketValue")) if meta_row else None
if initial_capital is None:
    initial_capital = 50000

meta = {
    "startDate": datetime.now(timezone.utc).isoformat(),
    "initialCapitalEUR": initial_capital,
    "cumFeesEUR": 0,
    "cumAiCostEUR": 0,
}

portfolio_summary = {
    "cashEUR": cash_eur,
    "positionsCount": len(positions),
    "positionsMarketValueEUR": positions_market_value,
    "totalPortfolioValueEUR": total_value,
    "positions": positions,
}

return [{
    "json": {
        "portfolioRows": rows,
        "portfolioSummary": portfolio_summary,
        "meta": meta,
        "portfolioSource": source,
    }
}]
