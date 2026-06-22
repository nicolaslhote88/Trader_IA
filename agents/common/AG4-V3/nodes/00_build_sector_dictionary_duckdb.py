import duckdb
import time
from datetime import datetime

DB_PATH = "/files/duckdb/ag2_v3.duckdb"


def connect(path=DB_PATH, retries=12, delay=5):
    last = None
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as e:
            last = e
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise Exception(f"Unable to open AG2 universe DuckDB: {last}")


con = connect()
try:
    rows = con.execute(
        """
        SELECT DISTINCT TRIM(sector) AS sector
        FROM universe
        WHERE enabled = TRUE
          AND COALESCE(TRIM(sector), '') <> ''
          AND COALESCE(asset_class, '') IN ('EQUITY', 'ETF')
        ORDER BY sector
        """
    ).fetchall()
finally:
    con.close()

sector_dictionary = [str(row[0]).strip() for row in rows if row[0]]

return [{
    "json": {
        "sectorDictionary": sector_dictionary,
        "count": len(sector_dictionary),
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "source": "duckdb.ag2_v3.universe",
    }
}]
