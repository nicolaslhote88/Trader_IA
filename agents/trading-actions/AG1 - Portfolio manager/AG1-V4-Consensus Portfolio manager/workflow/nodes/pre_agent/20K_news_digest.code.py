import duckdb
import json
import time
from datetime import datetime, timezone

# D2 (2026-06-18) — News digest compact pour AG1 V4. cf docs/specs/ag1_v4_d2_news_digest.md
# Enrichit opportunity_pack : news[] (<=3, 14j) par row du pack + held_news pour les
# detenus hors pack. Source = vue news_analyzed (Boursorama + IBKR, deja pertinentes).
# Budget borne : pack ~20 + detenus. Pas d'appel LLM. Imports separes (sandbox).
DB = "/files/duckdb/ag4_spe_v2.duckdb"
AG1 = "/files/duckdb/ag1_v4_consensus.duckdb"
WINDOW_DAYS = 14
MAX_PER_SYMBOL = 3
TITLE_MAX = 90


def connect_ro(path, tries=6, delay=0.4):
    last = None
    for i in range(tries):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as e:
            last = e
            if "lock" in str(e).lower() and i < tries - 1:
                time.sleep(delay * (2 ** i)); continue
            raise
    raise last


def held_symbols():
    s = set(); c = None
    try:
        c = duckdb.connect(AG1, read_only=True)
        for r in c.execute(
            "SELECT DISTINCT UPPER(TRIM(symbol)), MAX(name) FROM main.portfolio_positions_mtm_latest "
            "WHERE symbol IS NOT NULL AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR','__META__') "
            "AND COALESCE(quantity,0)<>0 GROUP BY 1"
        ).fetchall():
            if r and r[0]:
                s.add(r[0])
    except Exception:
        pass
    finally:
        if c is not None:
            try: c.close()
            except Exception: pass
    return s


def norm_title(t):
    return "".join(str(t or "").lower().split())[:40]


def fetch_news(symbols):
    """Retourne {symbol: [ {date,src,provider,signal,impact,title}, ... <=3 ]}."""
    out = {}
    if not symbols:
        return out
    syms = sorted({str(s).upper() for s in symbols if s})
    placeholders = ",".join(["?"] * len(syms))
    sql = (
        "SELECT UPPER(TRIM(symbol)) sym, source, provider, suggested_signal, "
        "COALESCE(impact_score,0) impact, title, "
        "COALESCE(CASE WHEN published_at BETWEEN now()-INTERVAL '730 days' AND now()+INTERVAL '2 days' "
        "THEN published_at END, first_seen_at, analyzed_at) ts "
        "FROM news_analyzed "
        "WHERE UPPER(TRIM(symbol)) IN (" + placeholders + ") "
        "AND COALESCE(CASE WHEN published_at BETWEEN now()-INTERVAL '730 days' AND now()+INTERVAL '2 days' "
        "THEN published_at END, first_seen_at, analyzed_at) >= now() - INTERVAL '" + str(WINDOW_DAYS) + " days'"
    )
    con = connect_ro(DB)
    try:
        rows = con.execute(sql, syms).fetchall()
    finally:
        con.close()
    now = datetime.now(timezone.utc)
    by = {}
    for sym, source, provider, signal, impact, title, ts in rows:
        try:
            tsd = ts if isinstance(ts, datetime) else None
            if tsd is not None and tsd.tzinfo is None:
                tsd = tsd.replace(tzinfo=timezone.utc)
            age_days = (now - tsd).total_seconds() / 86400.0 if tsd else WINDOW_DAYS
        except Exception:
            age_days = WINDOW_DAYS
        rec = max(0.1, 1.0 - age_days / float(WINDOW_DAYS))
        score = (abs(int(impact)) + 1) * rec  # recence x |impact|
        by.setdefault(sym, []).append({
            "score": score,
            "src_pref": 0 if str(source) == "ibkr" else 1,  # IBKR prioritaire au dedup
            "date": (tsd.strftime("%Y-%m-%d") if tsd else ""),
            "src": str(source or ""),
            "provider": str(provider or ""),
            "signal": str(signal or ""),
            "impact": int(impact),
            "title": str(title or "")[:TITLE_MAX],
        })
    for sym, items in by.items():
        # dedup par titre normalise (IBKR garde la priorite)
        items.sort(key=lambda x: (x["src_pref"], -x["score"]))
        seen = set(); dedup = []
        for it in items:
            k = norm_title(it["title"])
            if k in seen:
                continue
            seen.add(k); dedup.append(it)
        dedup.sort(key=lambda x: -x["score"])
        out[sym] = [
            {"date": it["date"], "src": it["src"], "provider": it["provider"],
             "signal": it["signal"], "impact": it["impact"], "title": it["title"]}
            for it in dedup[:MAX_PER_SYMBOL]
        ]
    return out


items = _items or []
if not items:
    return []
j = dict(items[0].get("json", {}) or {})
pack = j.get("opportunity_pack") or {}
rows = pack.get("rows") or []

pack_syms = {str(r.get("symbol") or "").upper() for r in rows if r.get("symbol")}
held = held_symbols()
news_map = fetch_news(pack_syms | held)

# 1) attacher news[] a chaque row du pack
for r in rows:
    sym = str(r.get("symbol") or "").upper()
    r["news"] = news_map.get(sym, [])

# 2) held hors pack -> held_news
held_news = []
for sym in sorted(held - pack_syms):
    nl = news_map.get(sym, [])
    if nl:
        held_news.append({"symbol": sym, "news": nl})

pack["held_news"] = held_news
pack["news_legend"] = (
    "news[]: catalyseurs recents (<=14j) {date, src(ibkr/boursorama), provider, signal, impact(-10..10), title}. "
    "Usage: une news recente a fort impact peut justifier d'ajuster conviction/taille/sortie; "
    "privilegier sources premium (Reuters/Dow Jones); ne pas surreagir a une news isolee a faible impact. "
    "held_news = news des positions detenues hors pack."
)
j["opportunity_pack"] = pack

return [{"json": j}]
