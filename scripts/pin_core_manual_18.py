#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Epingle 18 piliers en CORE_MANUAL dans ag2_v3.duckdb -> universe_segments.

- CORE_MANUAL (source='manual') : preserve des refresh hebdo (le DELETE ne vise que source='auto'),
  force la rotation prioritaire AG2/AG3 (HELD_CORE) des maintenant.
- Retire la ligne WATCHLIST auto correspondante (etat final identique a un refresh production).
- Idempotent : ON CONFLICT (symbol,segment) DO NOTHING.
- duckdb==1.4.4 (retro-compat).
Usage: /tmp/ddb144/bin/python pin_core_manual_18.py --db /local-files/duckdb/ag2_v3.duckdb [--dry-run]
"""
import argparse, time
import duckdb

CORE = [
    "ASML", "TSM", "005930.KS", "SAP", "AMD",      # semis / tech
    "NVS", "NVO", "AZN", "UNH",                      # sante
    "NESN.SW", "UL",                                # staples
    "TM", "SIE.DE", "RHM.DE",                        # auto / indus / defense
    "SHEL", "RIO", "BHP",                            # energie / materiaux
    "BABA",                                          # chine / internet
]


def connect_rw(path, retries=20, delay=2.0):
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(path)
        except Exception as e:
            last = e
            if "lock" in str(e).lower():
                time.sleep(delay)
                continue
            raise
    raise last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/local-files/duckdb/ag2_v3.duckdb")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in CORE]
    assert len(syms) == len(set(syms)) == 18, f"liste CORE invalide: {len(syms)}"
    print(f"CORE_MANUAL a epingler: {len(syms)} -> {syms}")
    if args.dry_run:
        print("[dry-run] aucune ecriture")
        return

    con = connect_rw(args.db)
    before = con.execute("SELECT segment, count(*) FROM universe_segments WHERE active GROUP BY 1 ORDER BY 2 DESC").fetchall()
    # 1) Epingler CORE_MANUAL
    con.executemany(
        """
        INSERT INTO universe_segments
          (symbol, segment, active, priority_score, source, reason, metrics_json, updated_at)
        VALUES (?, 'CORE_MANUAL', TRUE, 900, 'manual', 'seed_global_pillar_pin', '{}', CURRENT_TIMESTAMP)
        ON CONFLICT (symbol, segment) DO NOTHING
        """,
        [(s,) for s in syms],
    )
    # 2) Retirer la ligne WATCHLIST auto correspondante (etat final = refresh production)
    ph = ",".join("?" * len(syms))
    con.execute(
        f"DELETE FROM universe_segments WHERE segment='WATCHLIST' AND source='auto' AND symbol IN ({ph})",
        syms,
    )
    con.commit()
    con.execute("CHECKPOINT")
    after = con.execute("SELECT segment, count(*) FROM universe_segments WHERE active GROUP BY 1 ORDER BY 2 DESC").fetchall()
    check = con.execute(
        f"SELECT symbol, segment, source FROM universe_segments WHERE symbol IN ({ph}) AND segment='CORE_MANUAL' ORDER BY symbol",
        syms,
    ).fetchall()
    con.close()
    print("AVANT:", before)
    print("APRES:", after)
    print(f"CORE_MANUAL confirmes: {len(check)}/18")


if __name__ == "__main__":
    main()
