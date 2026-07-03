#!/usr/bin/env python3
"""
Maintenance de ag4_v3.duckdb (AG4-V3 News Watcher).

Objectifs (P2 audit 2026-06-17) :
  1. Finaliser les run_log "zombies" (status=RUNNING jamais cloturés par un crash).
  2. Rétention : purger les vieilles lignes news_history / news_errors.
  3. Stopper le bloat (CHECKPOINT). Reclaim disque réel via --rebuild (EXPORT/IMPORT).

SÉCURITÉ :
  - Lecture/écriture EXCLUSIVE : si un run AG4 tient le lock, on RÉESSAIE puis on
    SORT proprement (code 0) sans rien casser -> safe à lancer en cron.
  - --rebuild remplace le fichier (backup .bak conservé). À lancer hors des 4 runs
    AG4 (UTC 23:45 / 04:45 / 08:45 / 16:45).

Usage :
  python3 ag4_duckdb_maintenance.py                      # retention + checkpoint
  python3 ag4_duckdb_maintenance.py --rebuild            # + reclaim disque
  python3 ag4_duckdb_maintenance.py --dry-run            # n'écrit rien
"""
import argparse
import os
import shutil
import sys
import time
from datetime import datetime, timezone

import duckdb

DEFAULT_DB = "/files/duckdb/ag4_v3.duckdb"


def connect(path, read_only=False, retries=5, delay=8):
    last = None
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:  # noqa: BLE001
            last = e
            if "lock" in str(e).lower() or "Conflicting lock" in str(e):
                print(f"[lock] tenu (essai {attempt+1}/{retries}) -> attente {delay}s")
                time.sleep(delay)
            else:
                raise
    print(f"[skip] DB verrouillee par un run AG4 actif ({last}). Sortie propre.")
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("AG4_DB_PATH", DEFAULT_DB))
    ap.add_argument("--retention-news-days", type=int, default=60)
    ap.add_argument("--retention-errors-days", type=int, default=30)
    ap.add_argument("--zombie-hours", type=int, default=6)
    ap.add_argument("--rebuild", action="store_true", help="reclaim disque via EXPORT/IMPORT (remplace le fichier)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    started = datetime.now(timezone.utc).isoformat()
    size_before = os.path.getsize(args.db) if os.path.exists(args.db) else 0
    print(f"== AG4 DuckDB maintenance @ {started} ==")
    print(f"DB={args.db}  taille={size_before/1e6:.0f} MB  dry_run={args.dry_run}")

    con = connect(args.db, read_only=False)
    try:
        def scalar(sql):
            try:
                return con.execute(sql).fetchone()[0]
            except Exception:
                return None

        nh = scalar("SELECT count(*) FROM news_history")
        ne = scalar("SELECT count(*) FROM news_errors")
        zombies = scalar(
            f"SELECT count(*) FROM run_log WHERE status='RUNNING' "
            f"AND started_at < now() - INTERVAL '{args.zombie_hours} hours'"
        )
        print(f"avant: news_history={nh}  news_errors={ne}  zombies_run_log={zombies}")

        if not args.dry_run:
            # 1. Zombies
            con.execute(
                f"""UPDATE run_log SET status='CRASHED',
                       finished_at=COALESCE(finished_at, now()),
                       error_detail=COALESCE(error_detail,'finalized_by_maintenance')
                    WHERE status='RUNNING'
                      AND started_at < now() - INTERVAL '{args.zombie_hours} hours'"""
            )
            # 2. Retention
            con.execute(
                f"""DELETE FROM news_history
                    WHERE COALESCE(last_seen_at, first_seen_at, analyzed_at, updated_at, created_at)
                          < now() - INTERVAL '{args.retention_news_days} days'"""
            )
            con.execute(
                f"""DELETE FROM news_errors
                    WHERE COALESCE(occurred_at, updated_at, created_at)
                          < now() - INTERVAL '{args.retention_errors_days} days'"""
            )
            con.execute("CHECKPOINT")
            print(f"apres: news_history={scalar('SELECT count(*) FROM news_history')}  "
                  f"news_errors={scalar('SELECT count(*) FROM news_errors')}")
    finally:
        con.close()

    if args.rebuild and not args.dry_run:
        rebuild(args.db)

    size_after = os.path.getsize(args.db) if os.path.exists(args.db) else 0
    print(f"taille finale={size_after/1e6:.0f} MB (avant {size_before/1e6:.0f} MB)")
    print("== done ==")


def rebuild(db):
    """Reclaim disque : EXPORT DATABASE -> nouveau fichier -> swap (backup .bak)."""
    base = os.path.dirname(db)
    expdir = os.path.join(base, "_ag4_export_tmp")
    newdb = db + ".compact"
    bak = db + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if os.path.exists(expdir):
        shutil.rmtree(expdir)
    print("[rebuild] EXPORT DATABASE ...")
    con = connect(db, read_only=False)
    try:
        con.execute(f"EXPORT DATABASE '{expdir}' (FORMAT PARQUET)")
    finally:
        con.close()
    if os.path.exists(newdb):
        os.remove(newdb)
    print("[rebuild] IMPORT vers fichier compact ...")
    con2 = duckdb.connect(newdb)
    try:
        con2.execute(f"IMPORT DATABASE '{expdir}'")
        con2.execute("CHECKPOINT")
    finally:
        con2.close()
    shutil.move(db, bak)
    shutil.move(newdb, db)
    shutil.rmtree(expdir, ignore_errors=True)
    print(f"[rebuild] OK. Backup conserve: {bak}")


if __name__ == "__main__":
    main()
