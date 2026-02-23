import duckdb, time, gc
from contextlib import contextmanager
from datetime import datetime, date

DB_PATH = "/files/duckdb/ag4_v2.duckdb"
LOOKBACK_DAYS = 120

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
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

def to_iso(v):
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)

items = _items or []
run_id = ""
db_path = DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id", "") or "")
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

history_index = {}
history_event_index = {}
loaded = 0

with db_con(db_path) as con:
    rows = con.execute(
        f"""
        SELECT
          dedupe_key, event_key, canonical_url, published_at, title, source, feed_url,
          symbols, type, notes, impact_score, confidence, urgency, snippet,
          first_seen_at, strategy, losers, winners,
          COALESCE(sectors_bullish, winners) AS sectors_bullish,
          COALESCE(sectors_bearish, losers) AS sectors_bearish,
          theme, regime, analyzed_at
        FROM news_history
        WHERE COALESCE(last_seen_at, first_seen_at, published_at, analyzed_at, updated_at, created_at)
              >= CURRENT_TIMESTAMP - INTERVAL '{LOOKBACK_DAYS} days'
        """
    ).fetchall()
    loaded = len(rows)

for idx, row in enumerate(rows, start=1):
    rec = {
        "dedupeKey": row[0] or "",
        "eventKey": row[1] or "",
        "canonicalUrl": row[2] or "",
        "publishedAt": to_iso(row[3]),
        "title": row[4] or "",
        "source": row[5] or "",
        "feedUrl": row[6] or "",
        "symbols": row[7] or "",
        "type": row[8] or "macro",
        "notes": row[9] or "",
        "ImpactScore": row[10] if row[10] is not None else 0,
        "confidence": row[11] if row[11] is not None else 0,
        "urgency": row[12] or "low",
        "Snippet": row[13] or "",
        "firstSeenAt": to_iso(row[14]),
        "Strategy": row[15] or "",
        "Losers": row[16] or row[19] or "",
        "Winners": row[17] or row[18] or "",
        "sectors_bullish": row[18] or "",
        "sectors_bearish": row[19] or "",
        "Theme": row[20] or "Resultats/Micro",
        "Regime": row[21] or "Neutral",
        "analyzedAt": to_iso(row[22]),
        "row_number": idx,
    }

    dkey = rec["dedupeKey"]
    ekey = rec["eventKey"]
    if dkey:
        history_index[dkey] = rec
    if ekey and ekey not in history_event_index:
        history_event_index[ekey] = rec

return [{
    "json": {
        "run_id": run_id,
        "db_path": db_path,
        "historyIndex": history_index,
        "historyEventIndex": history_event_index,
        "historyStats": {
            "loadedRows": loaded,
            "indexedByDedupe": len(history_index),
            "indexedByEvent": len(history_event_index),
            "lookbackDays": LOOKBACK_DAYS,
            "indexedAt": datetime.utcnow().isoformat() + "Z",
        },
    }
}]
