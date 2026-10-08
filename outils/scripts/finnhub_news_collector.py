#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Finnhub free -> staging only. HELD + CORE; key includes issuer; errors are observable."""
import argparse, hashlib, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from free_news_common import news_key, store_batch

FINNHUB = "https://finnhub.io/api/v1/company-news"

TICKER_MAP = {
    "BRK-B": "BRK.B", "WDS.AX": "WDS",
    "SIE.DE": "SIEGY", "ALV.DE": "ALIZY", "MBG.DE": "MBGAF", "BAS.DE": "BASFY", "RHM.DE": "RNMBY",
    "PRX.AS": "PROSY", "AD.AS": "ADRNY", "NESN.SW": "NSRGY", "ROG.SW": "RHHBY", "ITX.MC": "IDEXY",
    "8035.T": "TOELY", "9984.T": "SFTBY", "7974.T": "NTDOY", "6501.T": "HTHIY", "9983.T": "FRCOY",
    "4063.T": "SHECY", "6098.T": "RCRUY", "8058.T": "MSBHF",
    "005930.KS": "SSNLF", "000660.KS": "HXSCL", "CSL.AX": "CSLLY", "WES.AX": "WFAFY",
    "D05.SI": "DBSDY", "0700.HK": "TCEHY", "3690.HK": "MPNGY", "1810.HK": "XIACY", "1211.HK": "BYDDY",
}

STAGING_DDL = """
CREATE TABLE IF NOT EXISTS news_finnhub_staging (
  news_id VARCHAR PRIMARY KEY, symbol VARCHAR, company_name VARCHAR, source VARCHAR,
  url VARCHAR, canonical_url VARCHAR, title VARCHAR, published_at TIMESTAMP, published_at_raw VARCHAR,
  snippet VARCHAR, category VARCHAR, sentiment VARCHAR, provider VARCHAR, news_article_id VARCHAR,
  status VARCHAR, first_seen_at TIMESTAMP, fetched_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def http_get_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "trader-ia-finnhub/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8")), None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(5 * (i + 1)); continue
            return None, "HTTP %d" % e.code
        except Exception as e:
            if i < retries - 1:
                time.sleep(2); continue
            return None, type(e).__name__
    return None, "retries_exhausted"


def connect(path, read_only=False, retries=20, delay=2.0):
    import duckdb
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:
            last = e
            if "lock" in str(e).lower():
                time.sleep(delay); continue
            raise
    raise last


def load_symbols(ag2_path, segments):
    c = connect(ag2_path, read_only=True)
    segs = [s.strip().upper() for s in segments.split(",") if s.strip()]
    ph = ",".join("?" * len(segs))
    rows = c.execute(
        "SELECT DISTINCT u.symbol, COALESCE(u.name, u.symbol) FROM universe u "
        "JOIN universe_segments s ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(u.symbol)) "
        "WHERE COALESCE(u.enabled, TRUE) AND COALESCE(s.active, TRUE) "
        "AND UPPER(TRIM(s.segment)) IN (" + ph + ") ORDER BY 1",
        segs,
    ).fetchall()
    c.close()
    return [(r[0], r[1]) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ag2", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--ag4", default="/local-files/duckdb/ag4_spe_v2.duckdb")
    ap.add_argument("--segments", default="HELD,CORE_MANUAL,CORE_AUTO")
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--max-per-symbol", type=int, default=12)
    ap.add_argument("--target", choices=["staging"], default="staging")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    token = os.getenv("FINNHUB_TOKEN", "").strip()
    if not token:
        print("ERREUR: FINNHUB_TOKEN manquant"); sys.exit(2)

    today = datetime.now(timezone.utc).date()
    frm = (today - timedelta(days=args.days)).isoformat()
    to = today.isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()
    run_id = "FINNHUB_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    symbols = load_symbols(args.ag2, args.segments)
    print("%d symboles (segments=%s) %dj cap=%d target=%s" % (len(symbols), args.segments, args.days, args.max_per_symbol, args.target))

    rows = []
    errors = []
    unsupported = []
    fetched_syms = 0
    for idx, (sym, name) in enumerate(symbols):
        q = TICKER_MAP.get(sym, sym)
        if "." in q and q != "BRK.B":
            unsupported.append(sym)
            continue
        url = FINNHUB + "?" + urllib.parse.urlencode({"symbol": q, "from": frm, "to": to, "token": token})
        data, err = http_get_json(url)
        if err or not isinstance(data, list):
            errors.append({"symbol": sym, "error": err or "INVALID_RESPONSE"})
        arts = data if isinstance(data, list) else []
        if args.max_per_symbol and len(arts) > args.max_per_symbol:
            arts = sorted(arts, key=lambda x: x.get("datetime", 0), reverse=True)[: args.max_per_symbol]
        if arts:
            fetched_syms += 1
        for a in arts:
            aid = str(a.get("id") or a.get("url") or "")
            if not aid:
                continue
            news_id = hashlib.sha1(("finnhub|" + aid).encode("utf-8")).hexdigest()
            ts = a.get("datetime")
            try:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            except (ValueError, TypeError, OverflowError):
                continue
            if dt > datetime.now(timezone.utc) or dt < datetime.now(timezone.utc)-timedelta(days=args.days+1):
                continue
            pub = dt.isoformat()
            rows.append({
                "news_id": news_id, "symbol": sym, "company_name": name, "source": "finnhub",
                "url": a.get("url", ""), "canonical_url": a.get("url", ""),
                "title": a.get("headline", ""), "published_at": pub, "published_at_raw": str(ts or ""),
                "snippet": a.get("summary", ""), "category": a.get("category", ""),
                "sentiment": None, "provider": a.get("source", ""), "news_article_id": aid,
                "status": "PENDING", "first_seen_at": now_iso, "fetched_at": now_iso,
            })
        time.sleep(1.1)
        if (idx + 1) % 20 == 0:
            print("  ... %d/%d" % (idx + 1, len(symbols)), flush=True)

    seen, uniq = set(), []
    for r in rows:
        if r["news_id"] in seen:
            continue
        seen.add(r["news_id"]); uniq.append(r)
    print("Collecte : %d/%d symboles avec news, %d articles uniques." % (fetched_syms, len(symbols), len(uniq)))

    if args.dry_run:
        print("[dry-run] aucune ecriture"); return

    result = store_batch(args.ag4, "finnhub", uniq, len(symbols), errors, unsupported)
    print(json.dumps(result))
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
