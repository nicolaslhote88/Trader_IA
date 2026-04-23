#!/usr/bin/env python3
"""
Phase 2 du fix PK : traite les 9 tables bloquées par des index secondaires
(ou une vue pour ag2_v3.technical_signals).

Stratégie : DROP dépendances → ALTER TABLE ADD PRIMARY KEY → RECREATE dépendances.
Les DDL sont lus depuis duckdb_indexes() et duckdb_views() (pas de hardcoding).

Pré-requis : n8n + task-runners arrêtés, aucun .wal actif.
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("pip install duckdb --break-system-packages")

DB_DIR = Path("/local-files/duckdb")

# Tables qui ont échoué phase 1 à cause de duckdb dependency error
FIXES = [
    ("ag1_v3_chatgpt52.duckdb",        "main", "portfolio_positions_mtm_history", ["id"]),
    ("ag1_v3_chatgpt52.duckdb",        "main", "portfolio_positions_mtm_latest",  ["symbol"]),
    ("ag1_v3_gemini30_pro.duckdb",     "main", "portfolio_positions_mtm_history", ["id"]),
    ("ag1_v3_gemini30_pro.duckdb",     "main", "portfolio_positions_mtm_latest",  ["symbol"]),
    ("ag1_v3_grok41_reasoning.duckdb", "main", "portfolio_positions_mtm_history", ["id"]),
    ("ag1_v3_grok41_reasoning.duckdb", "main", "portfolio_positions_mtm_latest",  ["symbol"]),
    ("ag2_v3.duckdb",                  "main", "technical_signals",               ["id"]),
    ("ag4_v3.duckdb",                  "main", "ag4_fx_pairs",                    ["id"]),
    ("ag4_v3.duckdb",                  "main", "news_history",                    ["dedupe_key"]),
]


def process(db: str, sch: str, tbl: str, cols: list[str]) -> tuple[int, int]:
    """Retourne (ok, err)."""
    dbp = DB_DIR / db
    if not dbp.exists():
        print(f"   ✗ DB absente: {dbp}")
        return (0, 1)

    wal = Path(str(dbp) + ".wal")
    if wal.exists() and wal.stat().st_size > 0:
        print(f"   ✗ WAL actif ({wal.stat().st_size} B) — arrête n8n")
        return (0, 1)

    con = duckdb.connect(str(dbp))
    try:
        # Index secondaires (non-PK) sur la table
        idx = con.execute(f"""
            SELECT index_name, sql
            FROM duckdb_indexes()
            WHERE schema_name='{sch}' AND table_name='{tbl}' AND is_primary=false
        """).fetchall()

        # Vues qui mentionnent la table (par nom dans leur SQL)
        views = con.execute(f"""
            SELECT schema_name, view_name, sql
            FROM duckdb_views()
            WHERE NOT internal AND sql ILIKE '%{tbl}%'
        """).fetchall()

        # 1. Drop les vues (elles référencent la table)
        for vs, vn, _ in views:
            con.execute(f'DROP VIEW "{vs}"."{vn}"')
            print(f"   drop view    {vs}.{vn}")

        # 2. Drop les index secondaires
        for iname, _ in idx:
            con.execute(f'DROP INDEX "{sch}"."{iname}"')
            print(f"   drop index   {iname}")

        # 3. ALTER TABLE ADD PRIMARY KEY
        col_list = ", ".join(f'"{c}"' for c in cols)
        con.execute(f'ALTER TABLE "{sch}"."{tbl}" ADD PRIMARY KEY ({col_list})')
        print(f"   ✓ PK ajoutée sur ({col_list})")

        # 4. Recréer les index
        for iname, isql in idx:
            con.execute(isql)
            print(f"   recreate idx {iname}")

        # 5. Recréer les vues
        for vs, vn, vsql in views:
            con.execute(vsql)
            print(f"   recreate view {vs}.{vn}")

        con.execute("CHECKPOINT")
        return (1, 0)
    except Exception as e:
        print(f"   ✗ ÉCHEC: {e}")
        return (0, 1)
    finally:
        con.close()


def main() -> int:
    if not DB_DIR.is_dir():
        print(f"ERREUR: {DB_DIR} introuvable", file=sys.stderr)
        return 2

    total_ok = total_err = 0
    for db, sch, tbl, cols in FIXES:
        print(f"\n── {db} :: {sch}.{tbl} → PK({cols})")
        ok, err = process(db, sch, tbl, cols)
        total_ok += ok
        total_err += err

    print(f"\n═══ Phase 2: {total_ok} OK, {total_err} erreurs ═══\n")

    # Vérification finale par DB
    print("═══ PK finales par DB ═══")
    for db in sorted({f[0] for f in FIXES}):
        dbp = DB_DIR / db
        if not dbp.exists():
            continue
        con = duckdb.connect(str(dbp), read_only=True)
        n = con.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE constraint_type='PRIMARY KEY' AND table_name NOT LIKE 'backup_%'
        """).fetchone()[0]
        con.close()
        print(f"  {db:40s} PK={n}")

    return 1 if total_err else 0


if __name__ == "__main__":
    sys.exit(main())
