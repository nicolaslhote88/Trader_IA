import duckdb
import json
import time
from datetime import datetime, timezone

DB = "/files/duckdb/ag4_spe_v2.duckdb"
AG1 = "/files/duckdb/ag1_v4_consensus.duckdb"
RUN_ID = "AG4IBKR_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

def connect_w(path, tries=6, delay=0.4):
    last = None
    for i in range(tries):
        try:
            return duckdb.connect(path)
        except Exception as e:
            last = e
            if "lock" in str(e).lower() and i < tries - 1:
                time.sleep(delay * (2 ** i)); continue
            raise
    raise last

def held_full():
    s = set(); c = None
    try:
        c = duckdb.connect(AG1, read_only=True)
        for r in c.execute("SELECT DISTINCT UPPER(TRIM(symbol)) FROM main.portfolio_positions_mtm_latest WHERE symbol IS NOT NULL AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR','__META__') AND COALESCE(quantity,0)<>0").fetchall():
            if r and r[0]:
                s.add(r[0])
    except Exception:
        pass
    finally:
        if c is not None:
            try: c.close()
            except Exception: pass
    return s

held = held_full()
base_to_full = {}
for h in held:
    base_to_full.setdefault(h.split(".")[0], h)

raw = []
for it in (_items or []):
    j = it.get("json", {}) or {}
    arr = j.get("items")
    if isinstance(arr, list):
        raw += arr
    elif isinstance(j, list):
        raw += j

con = connect_w(DB)
for ddl in ["ALTER TABLE news_history ADD COLUMN IF NOT EXISTS provider VARCHAR",
            "ALTER TABLE news_history ADD COLUMN IF NOT EXISTS news_article_id VARCHAR",
            "ALTER TABLE news_history ADD COLUMN IF NOT EXISTS ibkr_sentiment DOUBLE"]:
    try: con.execute(ddl)
    except Exception: pass

out = []
seen = set()
for a in raw:
    if not isinstance(a, dict):
        continue
    sym_ibkr = str(a.get("symbol_guess") or "").upper().strip()
    full = base_to_full.get(sym_ibkr)
    if not full:
        continue
    sym = full
    provider_code = str(a.get("source") or "IBKR")
    art = str(a.get("news_article_id") or "")
    if not art:
        continue
    news_id = "ibkr:" + sym + ":" + provider_code + ":" + art
    if news_id in seen:
        continue
    seen.add(news_id)
    if con.execute("SELECT 1 FROM news_history WHERE news_id=? LIMIT 1", [news_id]).fetchone():
        continue
    headline = str(a.get("headline") or "").strip()
    pub = a.get("published_at")
    payload = {"source": "ibkr", "title": headline, "date": pub, "provider": a.get("provider"), "snippet": headline, "content": headline}
    out.append({"json": {
        "run_id": RUN_ID, "db_path": DB,
        "newsId": news_id, "symbol": sym, "company_name": sym, "companyName": sym, "isin": None,
        "source": "ibkr", "provider": a.get("provider") or provider_code, "newsArticleId": art,
        "ibkrSentiment": a.get("sentiment"),
        "providerTimeContract": {k: a.get(k) for k in ["provider_published_at", "provider_time_raw", "observed_at", "published_at_source", "timestamp_quality"]},
        "url": art, "articleUrl": art, "articleCanonicalUrl": art, "canonicalUrl": art,
        "title": headline, "articleTitle": headline, "publishedAt": pub, "snippet": headline, "text": headline,
        "firstSeenAt": datetime.now(timezone.utc).isoformat(),
        "_reason": "ibkr_portfolio", "_runAI": True, "_articlesLoopReset": False,
        "llmInput": json.dumps(payload),
    }})
con.close()
return out
