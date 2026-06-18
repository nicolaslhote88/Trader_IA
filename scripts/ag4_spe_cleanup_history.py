#!/usr/bin/env python3
"""
Nettoyage hybride de l'historique news AG4_Spé (cf docs/audits/20260617_ag4_spe_v2_analysis.md).

Décision Nicolas 2026-06-18 (option hybride) :
  1) SUPPRIMER les placeholders sans summary (bruit pur Noise/0/WATCH, aucune valeur).
     -> les ~10 articles récents/symbole encore listés reviendront seuls comme news neuves.
  2) GARDER les lignes analysées (vrai contenu LLM) et corriger leur date :
     published_at corrompu (hors [now-730j; now+7j]) ou NULL -> first_seen_at
     (la vraie date est irrécupérable : published_at_raw est corrompu lui aussi).

Lecture seule par défaut (dry-run). N'écrit QUE si --apply.
⚠️ Backup du .duckdb AVANT --apply. Lancer hors run AG4_Spé (cron 09/12/15 UTC) pour éviter le lock.
⚠️ Exécuter avec un DuckDB de la même famille que l'écrivain (stack n8n = 1.4.x).

Usage :
    python3 ag4_spe_cleanup_history.py            # dry-run
    python3 ag4_spe_cleanup_history.py --apply     # applique
"""
import argparse
import duckdb

DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"
ANALYZED = "summary IS NOT NULL AND summary <> ''"
CORRUPT = "(published_at < now() - INTERVAL '730 days' OR published_at > now() + INTERVAL '7 days')"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=not args.apply)
    one = lambda s: con.execute(s).fetchone()[0]

    total = one("SELECT count(*) FROM news_history")
    placeholders = one(f"SELECT count(*) FROM news_history WHERE NOT ({ANALYZED})")
    analyzed = one(f"SELECT count(*) FROM news_history WHERE {ANALYZED}")
    to_fix = one(f"SELECT count(*) FROM news_history WHERE {ANALYZED} AND (published_at IS NULL OR {CORRUPT})")
    print(f"AVANT : total={total}  analysées={analyzed}  placeholders={placeholders}")
    print(f"        dates à corriger (analysées, corrompues/NULL) = {to_fix}")

    if not args.apply:
        print("\n[DRY-RUN] aucune écriture. Relancer avec --apply.")
        con.close()
        return

    del_n = con.execute(f"DELETE FROM news_history WHERE NOT ({ANALYZED})").fetchall()
    con.execute(
        f"UPDATE news_history SET published_at = first_seen_at, updated_at = now() "
        f"WHERE {ANALYZED} AND (published_at IS NULL OR {CORRUPT})"
    )
    con.execute("CHECKPOINT")

    total2 = one("SELECT count(*) FROM news_history")
    remaining_bad = one(f"SELECT count(*) FROM news_history WHERE published_at IS NOT NULL AND {CORRUPT}")
    remaining_null = one("SELECT count(*) FROM news_history WHERE published_at IS NULL")
    mn, mx = con.execute("SELECT min(published_at), max(published_at) FROM news_history").fetchone()
    con.close()
    print(f"\n[APPLY] OK. total après = {total2} (supprimées {total - total2})")
    print(f"        dates corrompues restantes = {remaining_bad} (attendu 0)")
    print(f"        published_at NULL restants = {remaining_null}")
    print(f"        plage published_at = {mn} -> {mx}")


if __name__ == "__main__":
    main()
