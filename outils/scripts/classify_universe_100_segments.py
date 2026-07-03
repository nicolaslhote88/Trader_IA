#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe les 100 nouvelles valeurs dans ag2_v3.duckdb -> table `universe_segments`.

Etat correct pour une entree neuve (sans historique YF/AG3/technique) = WATCHLIST :
  - entre dans la rotation WATCHLIST d'AG2 (technique),
  - eligible a la promotion auto CORE_AUTO une fois les donnees YF collectees,
  - reste visible (non quarantine).
Si un symbole est detenu au portefeuille -> HELD (priorite 1000).

- Idempotent : PK (symbol, segment) + ON CONFLICT DO NOTHING.
- source='auto' (coherent avec le refresh hebdo qui reconstruit les segments auto).
- Ecrire avec duckdb==1.4.4 (retro-compat lecteurs).
Usage:
  /tmp/ddb144/bin/python classify_universe_100_segments.py --db /local-files/duckdb/ag2_v3.duckdb \
      --ag1 /local-files/duckdb/ag1_v4_consensus.duckdb [--dry-run]
"""
import argparse, json, time, datetime
import duckdb

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


def connect_rw(path, retries=15, delay=2.0):
    last = None
    for i in range(retries):
        try:
            return duckdb.connect(path)
        except Exception as e:
            last = e
            if "lock" in str(e).lower():
                time.sleep(delay)
                continue
            raise
    raise last


def load_held(ag1_path):
    try:
        c = duckdb.connect(":memory:")
        c.execute(f"ATTACH '{ag1_path}' AS ag1 (READ_ONLY)")
        rows = c.execute(
            "SELECT DISTINCT UPPER(TRIM(symbol)) FROM ag1.main.portfolio_positions_mtm_latest "
            "WHERE COALESCE(quantity,0)<>0 AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR','__META__')"
        ).fetchall()
        c.close()
        return {r[0] for r in rows if r and r[0]}
    except Exception as e:
        print("WARN held introuvable:", str(e)[:160])
        return set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--ag1", default="/local-files/duckdb/ag1_v4_consensus.duckdb")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in SYMBOLS]
    assert len(syms) == 100 and len(set(syms)) == 100, f"liste invalide: {len(syms)}"

    held = load_held(args.ag1)
    held_in_lot = sorted(set(syms) & held)
    payload = []
    for s in syms:
        if s in held:
            payload.append((s, "HELD", True, 1000.0, "auto", "portfolio_position", "{}"))
        else:
            payload.append((s, "WATCHLIST", True, 0.0, "auto", "seed_global_100_not_held_not_core", "{}"))

    print(f"a inserer: {len(payload)} | HELD={len(held_in_lot)} ({held_in_lot}) | WATCHLIST={len(payload)-len(held_in_lot)}")
    if args.dry_run:
        print("[dry-run] aucune ecriture")
        return

    con = connect_rw(args.db)
    before = con.execute("SELECT count(*) FROM universe_segments").fetchone()[0]
    con.executemany(
        """
        INSERT INTO universe_segments
          (symbol, segment, active, priority_score, source, reason, metrics_json, updated_at)
        VALUES (?,?,?,?,?,?,?, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol, segment) DO NOTHING
        """,
        payload,
    )
    con.commit()
    con.execute("CHECKPOINT")
    after = con.execute("SELECT count(*) FROM universe_segments").fetchone()[0]
    ph = ",".join("?" * len(syms))
    seg_now = con.execute(
        f"SELECT segment, count(*) FROM universe_segments WHERE symbol IN ({ph}) GROUP BY 1 ORDER BY 2 DESC",
        syms,
    ).fetchall()
    present = con.execute(
        f"SELECT count(DISTINCT symbol) FROM universe_segments WHERE symbol IN ({ph})", syms
    ).fetchone()[0]
    con.close()
    print(f"OK before={before} after={after} delta={after-before} | du_lot_segmentes={present}/100 | repartition_lot={seg_now}")


if __name__ == "__main__":
    main()
