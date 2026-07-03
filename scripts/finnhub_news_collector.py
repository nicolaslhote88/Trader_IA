#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Collecteur news Finnhub (coeur) -> normalisation au schema `news_history` d'ag4_spe_v2.

Pattern cible (calque AG4_Spe-IBKR-V1) : COLLECTE (ce script) -> chaine LLM de pertinence
(noeuds existants AG4_Spe) -> ecriture lignes ANALYZED/SKIPPED -> vue `news_analyzed` -> AG1.

Ce script fait UNIQUEMENT la collecte + normalisation + dedup. Il ECRIT PAR DEFAUT dans une
table de STAGING `news_finnhub_staging`. Passer --target news_history pour ecrire en prod.

- Mapping symbole univers -> ticker Finnhub : US/ADR = lui-meme ; cotations locales = ADR/OTC US.
- Cap par symbole (--max-per-symbol) : borne le volume LLM (megacaps type NVDA sortent 200+/jour).
- Dedup : news_id = SHA1(source|news_article_id) ; ON CONFLICT DO NOTHING.
- Rate limit Finnhub free = 60/min -> sleep 1.1s. Stdlib + duckdb. duckdb <=1.4.3 pour ecrire ag4_spe_v2.
Cle : env FINNHUB_TOKEN.
"""
import argparse, hashlib, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

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
            return None, f"HTTP {e.code}"
        except Exception as e:
            if i < retries - 1:
                time.sleep(2); continue
            return None, str(e)[:120]
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
        f"""
        SELECT DISTINCT u.symbol, COALESCE(u.name, u.symbol)
        FROM universe u
        JOIN universe_segments s ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(u.symbol))
        WHERE COALESCE(u.enabled, TRUE)
          AND COALESCE(s.active, TRUE)
          AND UPPER(TRIM(s.segment)) IN ({ph})
        ORDER BY 1
        """,
        segs,
    ).fetchall()
    c.close()
    return [(r[0], r[1]) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ag2", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--ag4", default="/local-files/duckdb/ag4_spe_v2.duckdb")
    ap.add_argument("--segments", default="CORE_MANUAL,CORE_AUTO")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--max-per-symbol", type=int, default=12,
                    help="garde les N articles les plus recents par symbole (0=illimite)")
    ap.add_argument("--target", choices=["staging", "news_history"], default="staging")
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
    print(f"{len(symbols)} symboles (segments={args.segments}), {args.days}j, cap/symbole={args.max_per_symbol}, target={args.target}")

    rows = []
    fetched_syms = 0
    for idx, (sym, name) in enumerate(symbols):
        q = TICKER_MAP.get(sym, sym)
        url = f"{FINNHUB}?{urllib.parse.urlencode({'symbol': q, 'from': frm, 'to': to, 'token': token})}"
        data, err = http_get_json(url)
        arts = data if isinstance(data, list) else []
        if args.max_per_symbol and len(arts) > args.max_per_symbol:
            arts = sorted(arts, key=lambda x: x.get("datetime", 0), reverse=True)[: args.max_per_symbol]
        if arts:
            fetched_syms += 1
        for a in arts:
            aid = str(a.get("id") or a.get("url") or "")
            if not aid:
                continue
            news_id = hashlib.sha1(f"finnhub|{aid}".encode("utf-8")).hexdigest()
            ts = a.get("datetime")
            pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None
            rows.append({
                "news_id": news_id, "symbol": sym, "company_name": name, "source": "finnhub",
                "url": a.get("url", ""), "canonical_url": a.get("url", ""),
                "title": a.get("headline", ""), "published_at": pub, "published_at_raw": str(ts or ""),
                "snippet": a.get("summary", ""), "category": a.get("category", ""),
                "sentiment": None, "provider": a.get("source", ""), "news_article_id": str(a.get("id") or ""),
                "status": "PENDING", "first_seen_at": now_iso, "fetched_at": now_iso,
            })
        time.sleep(1.1)
        if (idx + 1) % 20 == 0:
            print(f"  ... {idx+1}/{len(symbols)}", flush=True)

    seen, uniq = set(), []
    for r in rows:
        if r["news_id"] in seen:
            continue
        seen.add(r["news_id"]); uniq.append(r)
    print(f"Collecte : {fetched_syms}/{len(symbols)} symboles avec news, {len(uniq)} articles uniques.")

    if args.dry_run:
        print("[dry-run] aucune ecriture")
        return

    con = connect(args.ag4)
    if args.target == "staging":
        con.execute(STAGING_DDL)
        table = "news_finnhub_staging"
        cols = ["news_id","symbol","company_name","source","url","canonical_url","title","published_at",
                "published_at_raw","snippet","category","sentiment","provider","news_article_id","status",
                "first_seen_at","fetched_at"]
    else:
        table = "news_history"
        cols = ["news_id","run_id","symbol","company_name","source","url","canonical_url","title","published_at",
                "published_at_raw","snippet","category","sentiment","provider","news_article_id","status",
                "first_seen_at","fetched_at","created_at","updated_at"]
    before = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    placeholders = ",".join("?" * len(cols))
    for r in uniq:
        r2 = dict(r)
        if args.target == "news_history":
            r2["run_id"] = run_id
            r2["created_at"] = now_iso
            r2["updated_at"] = now_iso
        con.execute(
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT (news_id) DO NOTHING",
            [r2.get(c0) for c0 in cols],
        )
    con.commit(); con.execute("CHECKPOINT")
    after = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    con.close()
    print(f"OK target={table} before={before} after={after} nouveaux={after-before} (dedup actif)")


if __name__ == "__main__":
    main()
