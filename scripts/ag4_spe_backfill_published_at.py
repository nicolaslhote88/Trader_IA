#!/usr/bin/env python3
"""
B2 (2026-06-17) — Backfill / neutralisation des published_at corrompus dans ag4_spe_v2.duckdb.

Contexte : cf docs/audits/20260617_ag4_spe_v2_analysis.md. Le parseur historique (S16) a écrit
des published_at aberrants (années 2016->2031). On NEUTRALISE (NULL) toute date hors d'une plage
plausible, de sorte que les consommateurs (AG1 V4 via R8, fix D1) retombent proprement sur
first_seen_at. On ne réécrit PAS de date inventée : NULL est volontaire.

Idempotent. Lecture seule par défaut (--dry-run). N'écrit QUE si --apply est passé.

Usage (sur le VPS, via un container qui monte /files/duckdb, ex. yf-enrichment) :
    python3 ag4_spe_backfill_published_at.py            # dry-run (compte, n'écrit rien)
    python3 ag4_spe_backfill_published_at.py --apply    # applique la neutralisation

⚠️ Faire un backup du fichier .duckdb avant --apply. Ne pas lancer pendant un run AG4_Spé (lock).
"""
import argparse
import duckdb

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"
# Plage plausible alignée sur le garde-fou B1 (normalizeDate) et la fenêtre AG1 (D1).
MIN_EXPR = "CURRENT_TIMESTAMP - INTERVAL '730 days'"
MAX_EXPR = "CURRENT_TIMESTAMP + INTERVAL '7 days'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--apply", action="store_true", help="écrit réellement (sinon dry-run)")
    args = ap.parse_args()

    where = f"published_at IS NOT NULL AND (published_at < {MIN_EXPR} OR published_at > {MAX_EXPR})"

    con = duckdb.connect(args.db, read_only=not args.apply)
    total = con.execute("SELECT count(*) FROM news_history").fetchone()[0]
    bad = con.execute(f"SELECT count(*) FROM news_history WHERE {where}").fetchone()[0]
    print(f"news_history total            : {total}")
    print(f"published_at hors plage (à NULL): {bad} ({100*bad/total:.1f}%)")

    if not args.apply:
        print("\n[DRY-RUN] aucune écriture. Relancer avec --apply pour neutraliser.")
        con.close()
        return

    con.execute(f"UPDATE news_history SET published_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE {where}")
    con.execute("CHECKPOINT")
    remaining = con.execute(f"SELECT count(*) FROM news_history WHERE {where}").fetchone()[0]
    con.close()
    print(f"\n[APPLY] neutralisées. Restantes hors plage : {remaining} (attendu 0).")


if __name__ == "__main__":
    main()
