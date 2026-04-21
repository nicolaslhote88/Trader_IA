import json
import duckdb
import gc
import time
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"
BATCH_SIZE = 20
STATE_KEY = "ag4_spe_v2_last_symbol_index"


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
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


def safe_json_parse(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    txt = to_text(raw)
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def normalize_ref(v):
    return to_text(v).upper()


rows = [dict(it.get("json", {}) or {}) for it in (_items or [])]

candidates = []
for r in rows:
    enabled = truthy(r.get("enabled", r.get("Enabled", True)))
    if not enabled:
        continue

    symbol = to_text(r.get("symbol", r.get("Symbol", ""))).upper()
    if not symbol:
        continue

    notes = safe_json_parse(r.get("notesJson", r.get("Notes")))
    boursorama_ref = normalize_ref(
        r.get("boursoramaRef")
        or r.get("BoursoramaRef")
        or notes.get("boursoramaRef")
        or notes.get("boursoramaCode")
        or ""
    )
    if not boursorama_ref:
        continue

    db_path = to_text(r.get("db_path")) or DB_PATH
    candidates.append(
        {
            "queueId": f"{symbol}|boursorama|actualites",
            "symbol": symbol,
            "companyName": to_text(r.get("companyName", r.get("Name", symbol))),
            "isin": r.get("isin", r.get("ISIN")),
            "assetClass": r.get("assetClass", r.get("AssetClass")),
            "exchange": r.get("exchange", r.get("Exchange")),
            "currency": r.get("currency", r.get("Currency")),
            "country": r.get("country", r.get("Country")),
            "boursoramaRef": boursorama_ref,
            "coursUrl": f"https://www.boursorama.com/cours/{boursorama_ref}/",
            "actualitesUrl": f"https://www.boursorama.com/cours/actualites/{boursorama_ref}/",
            "source": "boursorama",
            "enabled": True,
            "db_path": db_path,
        }
    )

# Keep deterministic order across runs.
candidates = sorted(candidates, key=lambda x: x.get("symbol", ""))
total_items = len(candidates)

if total_items == 0:
    return []

db_path = to_text(candidates[0].get("db_path")) or DB_PATH

with db_con(db_path) as con:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS workflow_state (
          state_key VARCHAR PRIMARY KEY,
          state_value VARCHAR,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    row = con.execute(
        "SELECT state_value FROM workflow_state WHERE state_key = ?",
        [STATE_KEY],
    ).fetchone()
    try:
        start = int(row[0]) if row and row[0] is not None else 0
    except Exception:
        start = 0

    if start < 0 or start >= total_items:
        start = 0

    end = start + BATCH_SIZE
    batch = candidates[start:end]
    next_start = 0 if end >= total_items else end

    con.execute(
        """
        INSERT OR REPLACE INTO workflow_state (state_key, state_value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        [STATE_KEY, str(next_start)],
    )

out = []
for idx, item in enumerate(batch):
    j = dict(item)
    j["_batchInfo"] = {
        "batchIndex": idx + 1,
        "globalIndex": start + idx + 1,
        "totalItems": total_items,
        "nextStart": next_start,
        "rotationStore": "duckdb.workflow_state",
    }
    out.append({"json": j})

return out

