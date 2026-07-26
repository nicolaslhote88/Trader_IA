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
  2. ATTACH la DB en READ_ONLY, recrée les tables avec leur DDL exact puis
     recopie les données dans `<nom>.duckdb.new` (memory_limit serré).
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
import stat
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
    "ag1_fx_v1_chatgpt52.duckdb",
    "ag1_fx_v1_gemini30_pro.duckdb",
    "ag1_fx_v1_grok41_reasoning.duckdb",
    "ag1_v4_consensus.duckdb",
    "ag1_v3_chatgpt52.duckdb",
    "ag1_v3_grok41_reasoning.duckdb",
    "ag1_v3_gemini30_pro.duckdb",
    "ag2_fx_v1.duckdb",
    "ag2_v3.duckdb",
    "ag3_fx_v1.duckdb",
    "ag3_v2.duckdb",
    "ag4_forex_v1.duckdb",
    "ag4_fx_v1.duckdb",
    "ag4_v3.duckdb",
    "ag4_spe_v2.duckdb",
    "broker_costs.duckdb",
    "macro_data.duckdb",
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
    Reconstruit src vers src.new via ATTACH + DDL exact + copie streamée.
    Retourne (taille_src, taille_new, durée_s, nb_tables).
    En dry-run, supprime le .new après mesure.
    """
    dst = src.with_suffix(src.suffix + ".new")
    src_stat = src.stat()
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
            SELECT schema_name, table_name, sql FROM duckdb_tables()
            WHERE database_name='src'
            ORDER BY schema_name, table_name
            """
        ).fetchall()

        # Crée les schémas non-main.
        for sch in sorted({r[0] for r in rows}):
            if sch != "main":
                con.execute(f'CREATE SCHEMA IF NOT EXISTS "{sch}"')

        # L'ordre de creation/insertion doit respecter les dependances FK.
        # Le DDL fourni par duckdb_tables() preserve types, defaults, NOT NULL,
        # PK, UNIQUE et FOREIGN KEY, contrairement a CTAS.
        fk_rows = con.execute(
            """
            SELECT schema_name, table_name, referenced_table
            FROM duckdb_constraints()
            WHERE database_name='src'
              AND constraint_type='FOREIGN KEY'
            ORDER BY schema_name, table_name, constraint_index
            """
        ).fetchall()
        table_map = {(sch, table): ddl for sch, table, ddl in rows}
        deps = {key: set() for key in table_map}
        for sch, table, referenced_table in fk_rows:
            child = (sch, table)
            parent = (sch, referenced_table)
            if parent not in table_map:
                matches = [key for key in table_map if key[1] == referenced_table]
                if len(matches) == 1:
                    parent = matches[0]
                else:
                    raise RuntimeError(
                        f'FK ambiguë/introuvable pour "{sch}"."{table}" '
                        f'-> "{referenced_table}"'
                    )
            if parent != child:
                deps[child].add(parent)

        ordered_tables = []
        remaining = set(table_map)
        while remaining:
            ready = sorted(key for key in remaining if deps[key].isdisjoint(remaining))
            if not ready:
                raise RuntimeError(f"Cycle FK non pris en charge: {sorted(remaining)}")
            ordered_tables.extend(ready)
            remaining.difference_update(ready)

        # Les index explicites ne sont ni inclus dans CTAS, ni exposes comme
        # contraintes. Les capturer separement, sinon le swap degrade les
        # performances et change silencieusement le schema logique.
        indexes = con.execute(
            """
            SELECT schema_name, index_name, table_name, sql
            FROM duckdb_indexes()
            WHERE database_name='src' AND sql IS NOT NULL
            ORDER BY schema_name, table_name, index_name
            """
        ).fetchall()

        # Cree chaque table avec son schema exact puis copie en une passe.
        for sch, t in ordered_tables:
            ddl = table_map[(sch, t)]
            if not ddl:
                raise RuntimeError(f'DDL absent pour "{sch}"."{t}"')
            con.execute(ddl)
            total = con.execute(
                f'SELECT COUNT(*) FROM src."{sch}"."{t}"'
            ).fetchone()[0]
            if total == 0:
                continue
            # Fix 2026-07-05 : INSERT unique (streame + spill via memory_limit/temp_directory).
            # L'ancien chunking LIMIT/OFFSET sans ORDER BY etait NON DETERMINISTE avec
            # preserve_insertion_order=false -> doublons/pertes (PK record_id dupliquee).
            con.execute(
                f'INSERT INTO "{sch}"."{t}" SELECT * FROM src."{sch}"."{t}"'
            )
            inserted = con.execute(f'SELECT COUNT(*) FROM "{sch}"."{t}"').fetchone()[0]
            if inserted != total:
                raise RuntimeError(
                    f'"{sch}"."{t}" : {inserted} lignes copiees vs {total} attendues'
                )
            con.execute("CHECKPOINT")

        # Rejoue les index secondaires. Les index issus des contraintes ne
        # figurent pas dans duckdb_indexes(), donc pas de doublon.
        for sch, index_name, table_name, ddl in indexes:
            try:
                con.execute(ddl)
            except Exception as e:
                raise RuntimeError(
                    f'Echec restauration index "{sch}"."{index_name}" '
                    f'sur "{table_name}" : {e}'
                ) from e

        # Recree les VUES (fix 2026-07-05) : le rebuild initial ne copiait que
        # duckdb_tables() -> les vues (v_latest_*, news_analyzed...) etaient perdues au swap.
        views = con.execute(
            """
            SELECT schema_name, view_name, sql FROM duckdb_views()
            WHERE database_name='src' AND NOT internal
            ORDER BY schema_name, view_name
            """
        ).fetchall()
        for sch, v, ddl in views:
            if not ddl:
                continue
            if sch != "main":
                con.execute(f'CREATE SCHEMA IF NOT EXISTS "{sch}"')
            try:
                con.execute(ddl.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1))
            except Exception as e:
                raise RuntimeError(f'Echec recreation vue "{sch}"."{v}" : {e}') from e

        # Verification structurelle avant tout swap : colonnes, contraintes,
        # index et vues doivent etre strictement equivalents.
        def normalized_catalog(sql, params):
            values = con.execute(sql, params).fetchall()
            normalized = [
                tuple(tuple(v) if isinstance(v, list) else v for v in row)
                for row in values
            ]
            return sorted(normalized, key=repr)

        columns_sql = """
            SELECT schema_name, table_name, column_index, column_name, data_type,
                   is_nullable, column_default
            FROM duckdb_columns()
            WHERE database_name=?
            ORDER BY schema_name, table_name, column_index
        """
        constraints_sql = """
            SELECT schema_name, table_name, constraint_type, constraint_text,
                   constraint_column_names, referenced_table, referenced_column_names
            FROM duckdb_constraints()
            WHERE database_name=?
            ORDER BY schema_name, table_name, constraint_index
        """
        indexes_sql = """
            SELECT schema_name, index_name, table_name, is_unique, sql
            FROM duckdb_indexes()
            WHERE database_name=?
            ORDER BY schema_name, table_name, index_name
        """
        views_sql = """
            SELECT schema_name, view_name
            FROM duckdb_views()
            WHERE database_name=? AND NOT internal
            ORDER BY schema_name, view_name
        """
        target_db = con.execute("SELECT current_database()").fetchone()[0]
        for label, sql in (
            ("colonnes", columns_sql),
            ("contraintes", constraints_sql),
            ("index", indexes_sql),
            ("vues", views_sql),
        ):
            source_catalog = normalized_catalog(sql, ["src"])
            target_catalog = normalized_catalog(sql, [target_db])
            if source_catalog != target_catalog:
                raise RuntimeError(f"Catalogue {label} non equivalent apres reconstruction")

        con.execute("DETACH src")
        con.execute("CHECKPOINT")
    finally:
        con.close()

    # Le fichier reconstruit est cree avec l'utilisateur/umask du processus.
    # Avant un swap live, restaurer le proprietaire et les permissions de la
    # source afin que les writers n8n conservent exactement leurs acces.
    if not dry_run:
        try:
            os.chown(dst, src_stat.st_uid, src_stat.st_gid)
            os.chmod(dst, stat.S_IMODE(src_stat.st_mode))
        except PermissionError as e:
            raise RuntimeError(
                f"Impossible de restaurer owner/mode de {src} sur {dst}; "
                "swap refuse pour eviter une DB non inscriptible."
            ) from e

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


def check_no_wal(db_dir: Path, dbs: list[str] | None = None) -> list[str]:
    """Retourne la liste des DB qui ont un .wal actif (écritures en cours)."""
    busy = []
    for name in (DBS if dbs is None else dbs):
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
    ap.add_argument("--only", nargs="*", default=None,
                    help="Ne traiter que ces DB (noms de fichiers de la liste DBS).")
    ap.add_argument("--force", action="store_true",
                    help="Ignore le check .wal (à réserver aux situations désespérées).")
    args = ap.parse_args()

    db_dir: Path = args.db_dir
    if not db_dir.is_dir():
        print(f"ERREUR: {db_dir} n'existe pas ou n'est pas un dossier.", file=sys.stderr)
        return 2

    args.tmp_dir.mkdir(parents=True, exist_ok=True)

    # Check présence des DB
    dbs = [n for n in DBS if (args.only is None or n in args.only)]
    if args.only:
        unknown = [n for n in args.only if n not in DBS]
        if unknown:
            print(f"ERREUR: --only inconnus (pas dans DBS): {unknown}", file=sys.stderr)
            return 2
    missing = [n for n in dbs if not (db_dir / n).exists()]
    if missing:
        print(f"WARN: {len(missing)} DB manquantes, skipped: {', '.join(missing)}")

    present = [n for n in dbs if (db_dir / n).exists()]

    # Check .wal actifs (n8n tourne ?)
    busy = check_no_wal(db_dir, present)
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
    print(f"DBs      : {len(present)} / {len(dbs)}")
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
        print("  docker compose -f /opt/trader-ia/docker-compose.yml start n8n task-runners")
        print("\nAprès 24-48h de runs OK, supprime les .old :")
        print(f"  rm {db_dir}/*.duckdb.old")
    else:
        print("\n(dry-run — aucune modification faite. Relance avec --apply pour committer.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
