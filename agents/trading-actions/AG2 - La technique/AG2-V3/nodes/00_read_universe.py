import duckdb
import os
import time
import gc
from contextlib import contextmanager

DB_PATH = os.getenv("AG2_DUCKDB_PATH", "/files/duckdb/ag2_v3.duckdb")


@contextmanager
def db_con(path=DB_PATH, retries=6, delay=0.25):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as exc:
            if ("lock" in str(exc).lower() or "busy" in str(exc).lower()) and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
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


def to_text(v):
    if v is None:
        return ""
    return str(v).strip()


def truthy(v):
    if isinstance(v, bool):
        return v
    s = to_text(v).lower()
    if s in ("", "1", "true", "yes", "y", "oui", "ok", "enabled"):
        return True
    if s in ("0", "false", "no", "n", "non", "disabled"):
        return False
    return True


def norm_asset_class(v, symbol):
    s = to_text(v).upper()
    sym = to_text(symbol).upper()
    if s in ("FX", "FOREX", "CURRENCY"):
        return "FX"
    if s in ("ETF",):
        return "ETF"
    if s in ("CRYPTO", "CRYPTOCURRENCY"):
        return "CRYPTO"
    if sym.startswith("FX:") or sym.endswith("=X"):
        return "FX"
    return s or "EQUITY"


rows = []
with db_con() as con:
    result = con.execute(
        """
        SELECT
          symbol,
          COALESCE(symbol_yahoo, symbol) AS symbol_yahoo,
          COALESCE(name, symbol) AS name,
          COALESCE(asset_class, 'EQUITY') AS asset_class,
          COALESCE(exchange, '') AS exchange,
          COALESCE(currency, '') AS currency,
          COALESCE(country, '') AS country,
          COALESCE(sector, '') AS sector,
          COALESCE(industry, '') AS industry,
          COALESCE(isin, '') AS isin,
          COALESCE(enabled, TRUE) AS enabled,
          COALESCE(boursorama_ref, '') AS boursorama_ref,
          updated_at
        FROM universe
        WHERE COALESCE(enabled, TRUE) = TRUE
        ORDER BY symbol
        """
    ).fetchall()

for r in result:
    symbol = to_text(r[0]).upper()
    if not symbol:
        continue
    asset_class = norm_asset_class(r[3], symbol)
    boursorama_ref = to_text(r[11])
    row = {
        "Symbol": symbol,
        "symbol": symbol,
        "Ticker": symbol,
        "ticker": symbol,
        "symbol_yahoo": to_text(r[1]) or symbol,
        "SymbolYahoo": to_text(r[1]) or symbol,
        "Name": to_text(r[2]) or symbol,
        "name": to_text(r[2]) or symbol,
        "companyName": to_text(r[2]) or symbol,
        "AssetClass": asset_class,
        "assetClass": asset_class,
        "asset_class": asset_class,
        "Exchange": to_text(r[4]),
        "exchange": to_text(r[4]),
        "Currency": to_text(r[5]),
        "currency": to_text(r[5]),
        "Country": to_text(r[6]),
        "country": to_text(r[6]),
        "Sector": to_text(r[7]),
        "sector": to_text(r[7]),
        "Industry": to_text(r[8]),
        "industry": to_text(r[8]),
        "ISIN": to_text(r[9]),
        "isin": to_text(r[9]),
        "Enabled": bool(r[10]),
        "enabled": bool(r[10]),
        "BoursoramaRef": boursorama_ref,
        "boursoramaRef": boursorama_ref,
        "notesJson": "{}",
        "universe_source": "duckdb.ag2_v3.universe",
        "universe_db_path": DB_PATH,
        "universe_updated_at": str(r[12]) if r[12] is not None else "",
    }
    rows.append({"json": row})

return rows
