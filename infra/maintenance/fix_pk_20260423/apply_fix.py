#!/usr/bin/env python3
"""
Applique les correctifs de PRIMARY KEY sur les DuckDB compactées.

Usage:
    python3 apply_fix.py --db-dir /local-files/duckdb [--dry-run]

Le script détecte automatiquement :
- Les .sql présents dans son propre dossier (fix_pk_*.sql)
- Les .duckdb correspondantes dans --db-dir
- Si une contrainte a déjà été appliquée (idempotent, n'échoue pas)

Pré-check : ouvre chaque DB, vérifie l'absence de .wal actif, puis applique
chaque ALTER TABLE un par un. Log explicite de ce qui réussit / ce qui skippe.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb n'est pas installé : pip install duckdb")


HERE = Path(__file__).resolve().parent


def split_statements(sql: str) -> list[str]:
    """
    Split naif par `;` qui ignore les lignes de commentaire pur.
    Suffisant pour nos fichiers fix_pk_*.sql (pas de strings avec ;).
    """
    out = []
    for raw in sql.split(";"):
        # Retire les commentaires -- ... jusqu'à fin de ligne
        cleaned = "\n".join(
            line for line in raw.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        if cleaned:
            out.append(cleaned)
    return out


def apply_one(sql_path: Path, db_path: Path, dry_run: bool) -> tuple[int, int, int]:
    """
    Applique sql_path sur db_path.
    Retourne (applied, skipped_already_exists, errors).
    """
    if not db_path.exists():
        print(f"  ✗ DB absente: {db_path}")
        return (0, 0, 1)

    wal = Path(str(db_path) + ".wal")
    if wal.exists() and wal.stat().st_size > 0:
        print(f"  ✗ WAL actif détecté ({wal.stat().st_size} B) — n8n écrit dedans ?")
        return (0, 0, 1)

    sql = sql_path.read_text(encoding="utf-8")
    statements = split_statements(sql)

    # On filtre : garde seulement les ALTER TABLE / BEGIN / COMMIT / CHECKPOINT
    # et on compte les ALTER pour le rapport.
    alter_pattern = re.compile(r"^\s*ALTER\s+TABLE", re.IGNORECASE)

    if dry_run:
        n_alter = sum(1 for s in statements if alter_pattern.match(s))
        print(f"  [dry-run] {n_alter} ALTER TABLE seraient appliqués")
        return (0, 0, 0)

    applied = skipped = errors = 0
    con = duckdb.connect(str(db_path))
    try:
        for stmt in statements:
            if stmt.upper() in ("BEGIN TRANSACTION", "COMMIT", "CHECKPOINT"):
                try:
                    con.execute(stmt)
                except Exception:
                    pass
                continue
            try:
                con.execute(stmt)
                applied += 1
            except duckdb.Error as e:
                msg = str(e)
                # "already has a PRIMARY KEY" = déjà appliqué → idempotent
                if "has a primary key" in msg.lower() or "already" in msg.lower():
                    skipped += 1
                else:
                    errors += 1
                    print(f"  ✗ ÉCHEC sur: {stmt[:80]}...")
                    print(f"     → {msg}")
        # Commit final + checkpoint pour compacter
        try:
            con.execute("COMMIT")
        except Exception:
            pass
        con.execute("CHECKPOINT")
    finally:
        con.close()

    return (applied, skipped, errors)


def verify_one(db_path: Path) -> int:
    """Compte les PRIMARY KEY / UNIQUE présentes (tables non-backup)."""
    if not db_path.exists():
        return -1
    con = duckdb.connect(str(db_path), read_only=True)
    n = con.execute("""
        SELECT COUNT(*) FROM duckdb_constraints()
        WHERE constraint_type IN ('PRIMARY KEY','UNIQUE')
          AND table_name NOT LIKE 'backup_%'
    """).fetchone()[0]
    con.close()
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-dir", type=Path, required=True,
                    help="Dossier contenant les .duckdb sur le VPS (ex: /local-files/duckdb)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Compte les ALTER à appliquer sans les exécuter.")
    args = ap.parse_args()

    if not args.db_dir.is_dir():
        print(f"ERREUR: {args.db_dir} n'existe pas ou n'est pas un dossier", file=sys.stderr)
        return 2

    sql_files = sorted(HERE.glob("fix_pk_*.sql"))
    if not sql_files:
        print(f"ERREUR: aucun fix_pk_*.sql trouvé dans {HERE}", file=sys.stderr)
        return 2

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"═══ Fix PK DuckDB [{mode}] ═══")
    print(f"DB dir:    {args.db_dir}")
    print(f"Scripts:   {len(sql_files)} fichiers .sql dans {HERE}")
    print()

    total_applied = total_skipped = total_errors = 0

    for sql_path in sql_files:
        db_name = sql_path.name.replace("fix_pk_", "").replace(".sql", ".duckdb")
        db_path = args.db_dir / db_name

        pk_before = verify_one(db_path)
        before_str = f"PK={pk_before}" if pk_before >= 0 else "DB absente"

        print(f"── {db_name:40s} (avant: {before_str}) ──")
        a, s, e = apply_one(sql_path, db_path, args.dry_run)
        total_applied += a
        total_skipped += s
        total_errors += e

        if not args.dry_run:
            pk_after = verify_one(db_path)
            print(f"    → appliqués: {a}, déjà présents: {s}, erreurs: {e}  (PK après: {pk_after})")
        print()

    print("═══ Bilan ═══")
    print(f"  ALTER appliqués:  {total_applied}")
    print(f"  Déjà présents:    {total_skipped}")
    print(f"  Erreurs:          {total_errors}")

    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
