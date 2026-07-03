#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test du GAP de couverture Finnhub : interroge les cotations locales non couvertes
via leur ticker ADR/OTC US (la news US y est souvent syndiquee, gratuitement).

Mesure combien des 34 non-couverts (probe initial) se recuperent a cout nul.
Stdlib uniquement. Cle : env FINNHUB_TOKEN.
Usage: FINNHUB_TOKEN=xxx python3 finnhub_gap_adr_probe.py --days 30
"""
import argparse, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

FINNHUB = "https://finnhub.io/api/v1/company-news"

# local_symbol (univers) -> ticker a interroger chez Finnhub. HYPOTHESES (ADR/OTC US) a verifier.
MAP = {
    "BRK-B": "BRK.B",          # format Finnhub (point)
    "ABB": "ABB",              # NYSE direct, re-test
    "WDS.AX": "WDS",           # NYSE direct
    "SIE.DE": "SIEGY", "ALV.DE": "ALIZY", "MBG.DE": "MBGAF", "BAS.DE": "BASFY", "RHM.DE": "RNMBY",
    "PRX.AS": "PROSY", "AD.AS": "ADRNY", "NESN.SW": "NSRGY", "ROG.SW": "RHHBY", "ITX.MC": "IDEXY",
    "8035.T": "TOELY", "6861.T": "KYCCY", "9984.T": "SFTBY", "7974.T": "NTDOY", "6501.T": "HTHIY",
    "9983.T": "FRCOY", "4063.T": "SHECY", "6098.T": "RCRUY", "8058.T": "MSBHF",
    "005930.KS": "SSNLF", "000660.KS": "HXSCL",
    "CBA.AX": "CMWAY", "CSL.AX": "CSLLY", "MQG.AX": "MQBKY", "WES.AX": "WFAFY",
    "D05.SI": "DBSDY", "O39.SI": "OVCHY",
    "0700.HK": "TCEHY", "3690.HK": "MPNGY", "1810.HK": "XIACY", "1211.HK": "BYDDY",
}


def http_get_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "trader-ia-probe/1.0"})
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    args = ap.parse_args()
    token = os.getenv("FINNHUB_TOKEN", "").strip()
    if not token:
        print("ERREUR: FINNHUB_TOKEN manquant"); sys.exit(2)

    today = datetime.now(timezone.utc).date()
    frm = (today - timedelta(days=args.days)).isoformat()
    to = today.isoformat()

    covered, gap = [], []
    print(f"Test {len(MAP)} cotations locales via ticker ADR/OTC ({args.days}j)\n")
    for local, adr in MAP.items():
        url = f"{FINNHUB}?{urllib.parse.urlencode({'symbol': adr, 'from': frm, 'to': to, 'token': token})}"
        data, err = http_get_json(url)
        n = len(data) if isinstance(data, list) else 0
        sample = (data[0].get("headline", "")[:70] if n else "")
        flag = "OK " if n > 0 else "-- "
        print(f"  {flag}{local:11s} -> {adr:8s} : {n:4d} art.  {sample}", flush=True)
        (covered if n > 0 else gap).append(local)
        time.sleep(1.1)

    print(f"\n========== RESULTAT GAP ==========")
    print(f"  Recuperes via ADR/OTC : {len(covered)}/{len(MAP)}")
    print(f"  Toujours sans news    : {len(gap)} -> {', '.join(gap) if gap else '(aucun)'}")
    print(f"\n  => Couverture totale estimee Finnhub : {66 + len(covered)}/100")
    print("  Mapping valide (local -> adr) pour le collecteur prod :")
    print("   {" + ", ".join(f'\"{k}\":\"{MAP[k]}\"' for k in covered) + "}")


if __name__ == "__main__":
    main()
