import os
import gc
import time
import duckdb
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH_DEFAULT = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v2.duckdb")


def to_num(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        s = str(v).replace("EUR", "").replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "").replace(",", ".")
        n = float(s)
        return n if n == n else default
    except Exception:
        return default


def round2(v):
    return round(float(v or 0.0), 2)


def norm_text(v):
    return str(v or "").strip().upper()


def is_cash_row(r):
    sym = norm_text(r.get("Symbol") or r.get("symbol"))
    name = norm_text(r.get("Name") or r.get("name"))
    asset = norm_text(r.get("AssetClass") or r.get("assetClass") or r.get("asset_class"))
    sector = norm_text(r.get("Sector") or r.get("sector"))
    return (
        sym in ("CASH_EUR", "CASH", "EUR_CASH", "LIQUIDITE", "LIQUIDITES")
        or "CASH" in name
        or "LIQUID" in name
        or asset == "CASH"
        or sector == "CASH"
    )


def is_meta_row(r):
    sym = norm_text(r.get("Symbol") or r.get("symbol"))
    name = norm_text(r.get("Name") or r.get("name"))
    return sym == "__META__" or name == "__META__"


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
    with db_con(db_path) as con:
        if con is None:
            return []
        try:
            cur = con.execute(
                """
                SELECT
                  symbol,
                  name,
                  asset_class,
                  sector,
                  industry,
                  quantity,
                  last_price,
                  market_value,
                  unrealized_pnl,
                  updated_at
                FROM portfolio_positions_mtm_latest
                ORDER BY market_value DESC NULLS LAST, symbol
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception:
            return []


incoming = _items or []
input0 = incoming[0].get("json", {}) if incoming else {}
rows_from_4b = input0.get("portfolioRows", []) if isinstance(input0, dict) else []
portfolio_summary_in = input0.get("portfolioSummary", {}) if isinstance(input0, dict) else {}
db_path = input0.get("ag1_db_path", DB_PATH_DEFAULT) if isinstance(input0, dict) else DB_PATH_DEFAULT

db_rows = load_rows_from_duckdb(str(db_path))

rows = []
source = "duckdb" if db_rows else "4b_fallback"

if db_rows:
    for r in db_rows:
        rows.append(
            {
                "Symbol": str(r.get("symbol") or "").strip().upper(),
                "Name": str(r.get("name") or "").strip(),
                "AssetClass": str(r.get("asset_class") or "Equity"),
                "Sector": str(r.get("sector") or "Unknown"),
                "Industry": str(r.get("industry") or "Unknown"),
                "Quantity": to_num(r.get("quantity"), 0.0),
                "LastPrice": to_num(r.get("last_price"), 0.0),
                "MarketValue": to_num(r.get("market_value"), 0.0),
                "UnrealizedPnL": to_num(r.get("unrealized_pnl"), 0.0),
            }
        )
else:
    for r in rows_from_4b:
        rows.append(
            {
                "Symbol": str(r.get("Symbol") or r.get("symbol") or "").strip().upper(),
                "Name": str(r.get("Name") or r.get("name") or "").strip(),
                "AssetClass": str(r.get("AssetClass") or r.get("assetClass") or "Equity"),
                "Sector": str(r.get("Sector") or r.get("sector") or "Unknown"),
                "Industry": str(r.get("Industry") or r.get("industry") or "Unknown"),
                "Quantity": to_num(r.get("Quantity"), 0.0),
                "LastPrice": to_num(r.get("LastPrice"), 0.0),
                "MarketValue": to_num(r.get("MarketValue"), 0.0),
                "UnrealizedPnL": to_num(r.get("UnrealizedPnL"), 0.0),
            }
        )

cash_value = 0.0
cash_found = False
valid_positions = []
calculated_positions_value = 0.0
fallback_cash = to_num(portfolio_summary_in.get("cashEUR"), 0.0) if isinstance(portfolio_summary_in, dict) else 0.0

for row in rows:
    sym = norm_text(row.get("Symbol"))
    if is_cash_row(row):
        cash_value = max(cash_value, round2(row.get("MarketValue")))
        cash_found = True
        continue
    if is_meta_row(row):
        continue

    qty = to_num(row.get("Quantity"), 0.0)
    price = round2(row.get("LastPrice"))
    pnl = round2(row.get("UnrealizedPnL"))
    val = round2(qty * price) if qty > 0 and price > 0 else round2(row.get("MarketValue"))

    if qty > 0 or val > 0:
        calculated_positions_value += val
        valid_positions.append(
            {
                "symbol": sym,
                "name": row.get("Name") or sym,
                "sector": row.get("Sector") or "Unknown",
                "industry": row.get("Industry") or "Unknown",
                "assetClass": row.get("AssetClass") or "Equity",
                "qty": qty,
                "price": price,
                "value": val,
                "pnl": pnl,
            }
        )

if (not cash_found) and abs(fallback_cash) > 0:
    cash_value = round2(fallback_cash)
    source = source + "+summary_cash"

total_value = round2(cash_value + calculated_positions_value)
exposure_pct = int(round((calculated_positions_value / total_value) * 100)) if total_value > 0 else 0

portfolio_text = (
    "ETAT DU PORTEFEUILLE:\n"
    f"- VALEUR TOTALE : {total_value} EUR\n"
    f"- CASH DISPONIBLE : {cash_value} EUR\n"
    f"- EXPOSITION ACTIONS : {exposure_pct}%\n\n"
    "POSITIONS ACTUELLES :"
)

if not valid_positions:
    portfolio_text += "\n(Aucune position, portefeuille liquide)"
else:
    for p in valid_positions:
        sign = "+" if p["pnl"] > 0 else ""
        portfolio_text += (
            f"\n- {p['symbol']} ({p['name']}) [{p['sector']}]: {p['qty']} titres @ {p['price']} EUR "
            f"(Val: {p['value']} EUR) | PnL: {sign}{p['pnl']} EUR"
        )

return [
    {
        "json": {
            "portfolioBrief": {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "totalValue": total_value,
                "cash": cash_value,
                "exposurePct": exposure_pct,
                "agentBriefingText": portfolio_text,
                "positions": valid_positions,
                "source": source,
            }
        }
    }
]
