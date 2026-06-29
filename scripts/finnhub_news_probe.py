#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype collecteur news Finnhub — MESURE de couverture sur l'extension d'univers.

But : repondre empiriquement a "Finnhub suffit-il ?" AVANT tout branchement prod.
- Interroge Finnhub `company-news` (gratuit, 60 appels/min) pour chaque symbole.
- N'ECRIT PAS dans la prod `ag4_spe_v2.news_history` : sortie dans une base scratch
  `/local-files/duckdb/news_finnhub_probe.duckdb` (table `finnhub_probe`) + rapport console.
- Stdlib uniquement (urllib) -> tourne dans n'importe quel conteneur/host sans pip.

Cle : env FINNHUB_TOKEN (gratuite sur finnhub.io, ~1 min).
Usage (sur le VPS) :
  FINNHUB_TOKEN=xxxx python3 finnhub_news_probe.py \
    --ag2 /local-files/duckdb/ag2_v3.duckdb \
    --out /local-files/duckdb/news_finnhub_probe.duckdb --days 30
"""
import argparse, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

FINNHUB = "https://finnhub.io/api/v1/company-news"

# Les 100 symboles (== symbol_yahoo dans universe). Finnhub free couvre surtout l'US :
# pour les ADR/US le symbole tel quel marche ; pour les cotations locales (.DE/.SW/.T/.KS/.HK/.AX/.SI)
# Finnhub free renverra probablement 0 -> c'est precisement ce qu'on veut mesurer.
SYMBOLS = [
    "BRK-B","UNH","JNJ","PG","HD","MA","COST","KO","PEP","ABBV","MRK","CVX","BAC","CRM","NFLX",
    "AMD","ADBE","MCD","CAT","GE","BA","DIS","NKE","QCOM","TXN","PFE","VZ","UNP","GS","UBER",
    "SAP","SIE.DE","ALV.DE","MBG.DE","BAS.DE","RHM.DE","ASML","PRX.AS","AD.AS","NESN.SW","ROG.SW",
    "NVS","UBS","ABB","AZN","SHEL","HSBC","UL","RIO","BP","DEO","ITX.MC","SAN","RACE","NVO",
    "TM","SONY","MUFG","8035.T","6861.T","9984.T","7974.T","6501.T","9983.T","4063.T","6098.T",
    "8058.T","005930.KS","000660.KS","TSM","BHP","CBA.AX","CSL.AX","WDS.AX","MQG.AX","WES.AX",
    "D05.SI","O39.SI","BABA","0700.HK","3690.HK","1810.HK","1211.HK","PDD","JD","INFY","IBN",
    "HDB","VALE","PBR","ITUB","FMX","RY","TD","ENB","CNQ","CNI","SHOP","BN","SU",
]


def http_get_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "trader-ia-probe/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8")), None
        except urllib.error.HTTPError as e:
            if e.code == 429:  # rate limit
                time.sleep(5 * (i + 1)); continue
            return None, f"HTTP {e.code}"
        except Exception as e:
            if i < retries - 1:
                time.sleep(2); continue
            return None, str(e)[:120]
    return None, "retries_exhausted"


def load_universe_meta(ag2_path):
    """exchange/currency/country/sector pour annoter le rapport par region."""
    try:
        import duckdb
        c = None
        for _ in range(20):
            try:
                c = duckdb.connect(ag2_path, read_only=True); break
            except Exception as e:
                if "lock" in str(e).lower(): time.sleep(2); continue
                raise
        if c is None:
            print("WARN universe meta: base lockee, split par region indisponible")
            return {}
        rows = c.execute(
            "SELECT symbol, exchange, currency, country FROM universe WHERE symbol IN ({})".format(
                ",".join("'" + s.replace("'", "''") + "'" for s in SYMBOLS)
            )
        ).fetchall()
        c.close()
        return {r[0]: {"exchange": r[1], "currency": r[2], "country": r[3]} for r in rows}
    except Exception as e:
        print("WARN universe meta:", str(e)[:120])
        return {}


def is_us_listed(meta):
    return (meta.get("currency") == "USD") or (meta.get("exchange") in ("NYSE", "NASDAQ"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ag2", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--out", default="/local-files/duckdb/news_finnhub_probe.duckdb")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=0, help="limiter le nb de symboles (debug)")
    args = ap.parse_args()

    token = os.getenv("FINNHUB_TOKEN", "").strip()
    if not token:
        print("ERREUR: FINNHUB_TOKEN manquant. Cle gratuite sur https://finnhub.io (Register).")
        sys.exit(2)

    today = datetime.now(timezone.utc).date()
    frm = (today - timedelta(days=args.days)).isoformat()
    to = today.isoformat()
    meta = load_universe_meta(args.ag2)
    syms = SYMBOLS[: args.limit] if args.limit else SYMBOLS

    results = []   # (symbol, n_articles, last_date, sample_headline, err)
    raw_rows = []  # pour la base scratch
    for idx, sym in enumerate(syms):
        url = f"{FINNHUB}?{urllib.parse.urlencode({'symbol': sym, 'from': frm, 'to': to, 'token': token})}"
        data, err = http_get_json(url)
        if err:
            results.append((sym, 0, "", "", err))
        else:
            arts = data if isinstance(data, list) else []
            last = ""
            if arts:
                last = datetime.fromtimestamp(max(a.get("datetime", 0) for a in arts), tz=timezone.utc).isoformat()
            results.append((sym, len(arts), last, (arts[0].get("headline", "")[:90] if arts else ""), ""))
            for a in arts:
                raw_rows.append((
                    sym, str(a.get("id")), a.get("headline", ""), a.get("summary", ""),
                    a.get("source", ""), a.get("url", ""),
                    datetime.fromtimestamp(a.get("datetime", 0), tz=timezone.utc).isoformat() if a.get("datetime") else "",
                ))
        time.sleep(1.1)  # 60/min safe
        if (idx + 1) % 20 == 0:
            print(f"  ... {idx+1}/{len(syms)}", flush=True)

    # ---- Rapport ----
    us = [r for r in results if is_us_listed(meta.get(r[0], {}))]
    loc = [r for r in results if not is_us_listed(meta.get(r[0], {}))]
    def cov(group):
        n = len(group); covered = sum(1 for r in group if r[1] > 0)
        arts = sum(r[1] for r in group)
        return n, covered, arts

    print("\n========== COUVERTURE FINNHUB (free) ==========")
    for label, grp in (("US/ADR (attendu OK)", us), ("Cotations locales (attendu faible)", loc), ("TOTAL", results)):
        n, c, a = cov(grp)
        pct = (100.0 * c / n) if n else 0
        print(f"  {label:34s} : {c}/{n} symboles avec news ({pct:.0f}%), {a} articles / {args.days}j")
    print("\n--- symboles SANS news (gap a couvrir par autre source) ---")
    gap = [r[0] for r in results if r[1] == 0]
    print("  " + ", ".join(gap) if gap else "  (aucun)")
    print("\n--- echantillon (5 mieux couverts) ---")
    for r in sorted(results, key=lambda x: -x[1])[:5]:
        print(f"  {r[0]:10s} {r[1]:3d} art. | {r[3]}")

    # ---- Scratch duckdb ----
    try:
        import duckdb
        con = duckdb.connect(args.out)
        con.execute("""CREATE TABLE IF NOT EXISTS finnhub_probe(
            symbol VARCHAR, article_id VARCHAR, headline VARCHAR, summary VARCHAR,
            source VARCHAR, url VARCHAR, published_at VARCHAR, probe_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        con.executemany(
            "INSERT INTO finnhub_probe(symbol,article_id,headline,summary,source,url,published_at) VALUES (?,?,?,?,?,?,?)",
            raw_rows,
        )
        con.commit(); con.close()
        print(f"\nScratch ecrit: {args.out} ({len(raw_rows)} articles)")
    except Exception as e:
        print("WARN scratch duckdb:", str(e)[:160])


if __name__ == "__main__":
    main()
