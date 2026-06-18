import json
import duckdb
import gc
import time
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"
AG1_DB_PATH = "/files/duckdb/ag1_v4_consensus.duckdb"
BATCH_SIZE = 20
STATE_KEY = "ag4_spe_v2_last_symbol_index"

# C1/C3 (2026-06-18) - cf docs/audits/20260617_ag4_spe_v2_analysis.md.
# 1) Univers aligne sur AG1 actions : on EXCLUT les paires FX (=X / CURRENCY). Le Forex est
#    desactive et AG1 actions ne consomme jamais ces lignes -> ~44% de la base etait du gaspillage
#    LLM + scraping (et la source des 502/503 FX). Pour reactiver le FX un jour : SKIP_FX=False.
# 2) Rotation PRIORISEE portefeuille : les symboles detenus par AG1 passent a CHAQUE run
#    (fraicheur des positions), le reste tourne par offset.
SKIP_FX = True


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
                con.execute("CHECKPOINT")
            except Exception:
                pass
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


def is_fx(symbol, asset_class):
    s = to_text(symbol).upper()
    a = to_text(asset_class).upper()
    return s.endswith("=X") or a in ("FX", "FOREX", "CURRENCY")


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


def load_held_symbols(path=AG1_DB_PATH):
    held = set()
    con = None
    try:
        con = duckdb.connect(path, read_only=True)
        rows = con.execute(
            "SELECT DISTINCT UPPER(TRIM(symbol)) FROM main.portfolio_positions_mtm_latest "
            "WHERE symbol IS NOT NULL AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR', '__META__') "
            "AND COALESCE(quantity, 0) <> 0"
        ).fetchall()
        held = {r[0] for r in rows if r and r[0]}
    except Exception:
        held = set()
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return held


rows = [dict(it.get("json", {}) or {}) for it in (_items or [])]

candidates = []
for r in rows:
    if not truthy(r.get("enabled", r.get("Enabled", True))):
        continue
    symbol = to_text(r.get("symbol", r.get("Symbol", ""))).upper()
    if not symbol:
        continue
    asset_class = r.get("assetClass", r.get("AssetClass"))
    if SKIP_FX and is_fx(symbol, asset_class):
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
    candidates.append({
        "queueId": symbol + "|boursorama|actualites",
        "symbol": symbol,
        "companyName": to_text(r.get("companyName", r.get("Name", symbol))),
        "isin": r.get("isin", r.get("ISIN")),
        "assetClass": asset_class,
        "exchange": r.get("exchange", r.get("Exchange")),
        "currency": r.get("currency", r.get("Currency")),
        "country": r.get("country", r.get("Country")),
        "boursoramaRef": boursorama_ref,
        "coursUrl": "https://www.boursorama.com/cours/" + boursorama_ref + "/",
        "actualitesUrl": "https://www.boursorama.com/cours/actualites/" + boursorama_ref + "/",
        "source": "boursorama",
        "enabled": True,
        "db_path": db_path,
    })

candidates = sorted(candidates, key=lambda x: x.get("symbol", ""))
total_items = len(candidates)

if total_items == 0:
    return []

db_path = to_text(candidates[0].get("db_path")) or DB_PATH

held = load_held_symbols()
priority = [c for c in candidates if c["symbol"] in held]
rotation = [c for c in candidates if c["symbol"] not in held]
rot_total = len(rotation)

with db_con(db_path) as con:
    con.execute(
        "CREATE TABLE IF NOT EXISTS workflow_state ("
        "state_key VARCHAR PRIMARY KEY, state_value VARCHAR, "
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    row = con.execute(
        "SELECT state_value FROM workflow_state WHERE state_key = ?", [STATE_KEY]
    ).fetchone()
    try:
        start = int(row[0]) if row and row[0] is not None else 0
    except Exception:
        start = 0
    if rot_total == 0:
        start = 0
    elif start < 0 or start >= rot_total:
        start = 0
    fill = max(0, BATCH_SIZE - len(priority))
    rot_batch = rotation[start:start + fill]
    next_start = 0 if (start + fill) >= rot_total else (start + fill)
    batch = priority + rot_batch
    con.execute(
        "INSERT OR REPLACE INTO workflow_state (state_key, state_value, updated_at) "
        "VALUES (?, ?, CURRENT_TIMESTAMP)",
        [STATE_KEY, str(next_start)],
    )

out = []
for idx, item in enumerate(batch):
    j = dict(item)
    j["_isHeld"] = j["symbol"] in held
    j["_batchInfo"] = {
        "batchIndex": idx + 1,
        "heldCount": len(priority),
        "rotationFill": len(rot_batch),
        "rotationTotal": rot_total,
        "totalItems": total_items,
        "nextStart": next_start,
        "rotationStore": "duckdb.workflow_state",
    }
    out.append({"json": j})

return out
