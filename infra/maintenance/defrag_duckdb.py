#!/usr/bin/env python3
"""
Defrag one-shot des snapshots DuckDB du VPS Trader_IA.

Pourquoi ?
----------
Les writers n8n (AG1 duckdb_writer.py, AG2/AG3/AG4/AG4-SPE) font beaucoup de
`INSERT ... ON CONFLICT DO UPDATE` sans jamais appeler CHECKPOINT avant close.
Au fil des runs, les pages devenues orphelines ne sont pas recyclées et les
fichiers .duckdb gonflent (~240x pour AG4-V3, ~135x pour chaque DB AG1).

Audit du 22 avril 2026 sur snapshot local :
    14.0 GB → 110 MB après reconstruction, soit 13.46 GB récupérables.

Ce que fait ce script
---------------------
Pour chaque DB listée dans DBS :
  1. Vérifie qu'il n'y a pas de .wal (sinon, il faut arrêter n8n avant).
  2. ATTACH la DB en READ_ONLY, recrée toutes les tables chunkées dans un
     fichier `<nom>.duckdb.new` (memory_limit serré pour éviter l'OOM).
  3. Swap atomique :
        <nom>.duckdb        -> <nom>.duckdb.old   (conservé)
        <nom>.duckdb.new    -> <nom>.duckdb       (nouvelle DB propre)
  4. Garde les .old tant que l'utilisateur ne les supprime pas manuellement.

Sécurité
--------
- --dry-run par défaut : mesure uniquement, ne swap pas.
- Le swap ne touche à rien tant que le fichier .new n'est pas complet.
- En cas de crash pendant la reconstruction : le .new est à jeter, l'original
  est intact.
- Les .old ne sont PAS supprimés automatiquement : à toi de les virer après
  24-48h de runs sur la nouvelle DB.

Usage sur le VPS
----------------
    # 1. Arrêter n8n (ou au moins mettre les workflows AG* en pause)
    docker compose -f /opt/trader-ia/docker-compose.yml stop n8n task-runners

    # 2. Dry-run d'abord pour voir les gains
    python3 infra/maintenance/defrag_duckdb.py --dry-run

    # 3. Appliquer pour de vrai
    python3 infra/maintenance/defrag_duckdb.py --apply

    # 4. Redémarrer n8n
    docker compose -f /opt/trader-ia/docker-compose.yml start n8n task-runners

    # 5. Après 24-48h de runs OK, supprimer les .old
    rm /files/duckdb/*.duckdb.old
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb n'est pas installé : pip install duckdb")


# Chemin des DB sur le VPS (monté dans les conteneurs n8n).
# Override via --db-dir si besoin.
DEFAULT_DB_DIR = Path("/files/duckdb")

# Liste explicite des DB à traiter (on évite de traiter des fichiers inconnus).
DBS = [
    "ag1_v3_chatgpt52.duckdb",
    "ag1_v3_grok41_reasoning.duckdb",
    "ag1_v3_gemini30_pro.duckdb",
    "ag2_v3.duckdb",
    "ag3_v2.duckdb",
    "ag4_v3.duckdb",
    "ag4_spe_v2.duckdb",
    "yf_enrichment_v1.duckdb",
]

# Tunables DuckDB conservateurs (évite l'OOM sur petits VPS).
MEMORY_LIMIT = "1500MB"
THREADS = 2
CHUNK_ROWS = 2000


def human_mb(bytes_: int) -> str:
    return f"{bytes_ / 1024 ** 2:.1f} MB"


def defrag_one(src: Path, tmp_dir: Path, dry_run: bool) -> tuple[int, int, float, int]:
    """
    Reconstruit src vers src.new via ATTACH + CTAS chunkée.
    Retourne (taille_src, taille_new, durée_s, nb_tables).
    En dry-run, supprime le .new après mesure.
    """
    dst = src.with_suffix(src.suffix + ".new")
    # Nettoyage d'un .new orphelin d'un run précédent.
    for p in (dst, Path(str(dst) + ".wal")):
        if p.exists():
            p.unlink()

    t0 = time.time()
    con = duckdb.connect(str(dst))
    try:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute(f"SET threads={THREADS}")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory='{tmp_dir}'")
        con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")

        rows = con.execute(
            """
            SELECT schema_name, table_name FROM duckdb_tables()
            WHERE database_name='src'
            ORDER BY schema_name, table_name
            """
        ).fetchall()

        # Crée les schémas non-main.
        for sch in sorted({r[0] for r in rows}):
            if sch != "main":
                con.execute(f'CREATE SCHEMA IF NOT EXISTS "{sch}"')

        # Récupère les contraintes PK/UNIQUE de la source pour les rejouer
        # après insertion (CTAS ne préserve PAS les contraintes — régression
        # identifiée le 2026-04-23, cf infra/maintenance/fix_pk_20260423/).
        constraints = con.execute(
            """
            SELECT schema_name, table_name, constraint_type, constraint_column_names
            FROM duckdb_constraints()
            WHERE database_name='src'
              AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
              AND table_name NOT LIKE 'backup_%'
            ORDER BY schema_name, table_name, constraint_type
            """
        ).fetchall()

        # CTAS chunkée par table avec CHECKPOINT intermédiaire.
        for sch, t in rows:
            con.execute(
                f'CREATE TABLE "{sch}"."{t}" AS '
                f'SELECT * FROM src."{sch}"."{t}" LIMIT 0'
            )
            total = con.execute(
                f'SELECT COUNT(*) FROM src."{sch}"."{t}"'
            ).fetchone()[0]
            if total == 0:
                continue
            for offset in range(0, total, CHUNK_ROWS):
                con.execute(
                    f'INSERT INTO "{sch}"."{t}" '
                    f'SELECT * FROM src."{sch}"."{t}" '
                    f'LIMIT {CHUNK_ROWS} OFFSET {offset}'
                )
            con.execute("CHECKPOINT")

        # Rejoue PRIMARY KEY et UNIQUE après insertion (les données sont propres
        # si la source l'était ; échoue bruyamment si doublon/null).
        for sch, t, ctype, cols in constraints:
            col_list = ", ".join([f'"{c}"' for c in cols])
            try:
                con.execute(
                    f'ALTER TABLE "{sch}"."{t}" ADD {ctype} ({col_list})'
                )
            except Exception as e:
                raise RuntimeError(
                    f"Échec restauration contrainte {ctype} sur "
                    f'"{sch}"."{t}" ({col_list}) : {e}. '
                    f"Vérifier doublons/nulls dans la source."
                ) from e

        con.execute("DETACH src")
        con.execute("CHECKPOINT")
    finally:
        con.close()

    sz_src = src.stat().st_size
    sz_dst = dst.stat().st_size
    dt = time.time() - t0

    if dry_run:
        # En dry-run on ne swap pas, on supprime juste le .new
        dst.unlink()
        wal = Path(str(dst) + ".wal")
        if wal.exists():
            wal.unlink()

    return sz_src, sz_dst, dt, len(rows)


def swap_atomically(src: Path) -> None:
    """
    Renomme src -> src.old puis src.new -> src. Atomique côté FS (rename).
    Le .old est conservé pour rollback.
    """
    old = src.with_suffix(src.suffix + ".old")
    new = src.with_suffix(src.suffix + ".new")

    # Si un .old existe déjà d'un run précédent, on l'écrase avec le courant
    # (l'utilisateur doit nettoyer les .old à la main après validation).
    if old.exists():
        old.unlink()

    # Atomique : rename() est garanti atomique sur le même FS POSIX.
    os.rename(src, old)
    os.rename(new, src)

    # WAL : s'il reste un .wal de l'ancienne DB, il appartient à old → renomme aussi
    src_wal = Path(str(src) + ".wal")
    old_wal = Path(str(old) + ".wal")
    if src_wal.exists() and not old_wal.exists():
        # Peu probable (on a check avant), mais au cas où.
        os.rename(src_wal, old_wal)


def check_no_wal(db_dir: Path) -> list[str]:
    """Retourne la liste des DB qui ont un .wal actif (écritures en cours)."""
    busy = []
    for name in DBS:
        p = db_dir / name
        wal = Path(str(p) + ".wal")
        if wal.exists() and wal.stat().st_size > 0:
            busy.append(name)
    return busy


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Mesure uniquement, ne swap pas.")
    group.add_argument("--apply", action="store_true", help="Swap atomiquement chaque DB.")
    ap.add_argument("--db-dir", type=Path, default=DEFAULT_DB_DIR,
                    help=f"Dossier contenant les .duckdb (défaut: {DEFAULT_DB_DIR})")
    ap.add_argument("--tmp-dir", type=Path, default=Path("/tmp/duckdb_defrag"),
                    help="Dossier temp DuckDB (défaut: /tmp/duckdb_defrag)")
    ap.add_argument("--force", action="store_true",
                    help="Ignore le check .wal (à réserver aux situations désespérées).")
    args = ap.parse_args()

    db_dir: Path = args.db_dir
    if not db_dir.is_dir():
        print(f"ERREUR: {db_dir} n'existe pas ou n'est pas un dossier.", file=sys.stderr)
        return 2

    args.tmp_dir.mkdir(parents=True, exist_ok=True)

    # Check présence des DB
    missing = [n for n in DBS if not (db_dir / n).exists()]
    if missing:
        print(f"WARN: {len(missing)} DB manquantes, skipped: {', '.join(missing)}")

    present = [n for n in DBS if (db_dir / n).exists()]

    # Check .wal actifs (n8n tourne ?)
    busy = check_no_wal(db_dir)
    if busy and not args.force:
        print("ERREUR: des .wal actifs détectés (n8n écrit probablement dedans) :")
        for b in busy:
            print(f"  - {b}.wal")
        print("\nArrête n8n d'abord :")
        print("  docker compose -f /opt/trader-ia/docker-compose.yml stop n8n task-runners")
        print("\nOu passe --force si tu sais ce que tu fais.")
        return 3

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"=== Defrag DuckDB [{mode}] ===")
    print(f"DB dir   : {db_dir}")
    print(f"Tmp dir  : {args.tmp_dir}")
    print(f"DBs      : {len(present)} / {len(DBS)}")
    print()
    print(f"{'DB':<38} {'src':>10} {'new':>10} {'ratio':>8} {'tables':>7} {'dur':>6}")
    print("-" * 90)

    total_src = total_dst = 0
    failures: list[tuple[str, str]] = []

    for name in present:
        src = db_dir / name
        try:
            sz_src, sz_dst, dt, nt = defrag_one(src, args.tmp_dir, dry_run=args.dry_run)
        except Exception as e:
            print(f"{name:<38} FAILED: {e}")
            failures.append((name, str(e)))
            continue

        total_src += sz_src
        total_dst += sz_dst
        ratio = sz_src / max(sz_dst, 1)
        print(
            f"{name:<38} {human_mb(sz_src):>10} {human_mb(sz_dst):>10} "
            f"{ratio:>7.1f}x {nt:>7} {dt:>5.1f}s"
        )

        if args.apply and not failures:
            try:
                swap_atomically(src)
            except Exception as e:
                print(f"  SWAP FAILED pour {name}: {e}")
                failures.append((name, f"swap: {e}"))

    print("-" * 90)
    if total_src:
        ratio_total = total_src / max(total_dst, 1)
        gain_gb = (total_src - total_dst) / 1024 ** 3
        print(
            f"{'TOTAL':<38} {human_mb(total_src):>10} {human_mb(total_dst):>10} "
            f"{ratio_total:>7.1f}x"
        )
        print(f"\nGain : {gain_gb:.2f} GB")

    if failures:
        print(f"\n{len(failures)} échec(s):")
        for n, err in failures:
            print(f"  - {n}: {err}")
        return 1

    if args.apply:
        print("\nSwap fait. Les anciennes DB sont conservées en .duckdb.old.")
        print("Redémarre n8n :")
        print("  docker compose -f /opt/trader-ia/doc