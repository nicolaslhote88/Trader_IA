#!/usr/bin/env python3
"""
Fix PK DuckDB — script standalone sans dépendance aux .sql.
Toutes les 74 contraintes PRIMARY KEY à restaurer sont embarquées ci-dessous.

Usage (sur le VPS, conteneurs ARRÊTÉS) :
    python3 fix_pk_standalone.py --db-dir /local-files/duckdb
    python3 fix_pk_standalone.py --db-dir /local-files/duckdb --dry-run

Pré-check validé localement : zéro doublon, zéro null sur les colonnes clés
dans les DB compactées du 2026-04-22. Idempotent (n'échoue pas si déjà appliqué).
"""
import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("pip install duckdb ou apt install python3-duckdb")

# (db_file, schema, table, [cols]) — source: snapshots originaux du 2026-04-21
PK_FIXES = [
    ("ag1_v3_chatgpt52.duckdb", "cfg", "portfolio_config", ["config_version"]),  # 1 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "ai_signals", ["signal_id"]),  # 554 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "alerts", ["alert_id"]),  # 97 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "backfill_queue", ["request_id"]),  # 0 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "cash_ledger", ["cash_tx_id"]),  # 15 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "fills", ["fill_id"]),  # 140 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "instruments", ["symbol"]),  # 57 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "market_prices", ["ts", "symbol", "source"]),  # 1556 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "orders", ["order_id"]),  # 140 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "portfolio_snapshot", ["run_id"]),  # 84 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "position_lots", ["lot_id"]),  # 66 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "positions_snapshot", ["run_id", "symbol"]),  # 1629 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "risk_metrics", ["run_id"]),  # 76 rows
    ("ag1_v3_chatgpt52.duckdb", "core", "runs", ["run_id"]),  # 85 rows
    ("ag1_v3_chatgpt52.duckdb", "main", "portfolio_positions_mtm_history", ["id"]),  # 7027 rows
    ("ag1_v3_chatgpt52.duckdb", "main", "portfolio_positions_mtm_latest", ["symbol"]),  # 20 rows
    ("ag1_v3_chatgpt52.duckdb", "main", "portfolio_positions_mtm_run_log", ["run_id"]),  # 334 rows
    ("ag1_v3_gemini30_pro.duckdb", "cfg", "portfolio_config", ["config_version"]),  # 1 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "ai_signals", ["signal_id"]),  # 405 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "alerts", ["alert_id"]),  # 42 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "backfill_queue", ["request_id"]),  # 0 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "cash_ledger", ["cash_tx_id"]),  # 56 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "fills", ["fill_id"]),  # 233 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "instruments", ["symbol"]),  # 92 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "market_prices", ["ts", "symbol", "source"]),  # 1466 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "orders", ["order_id"]),  # 233 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "portfolio_snapshot", ["run_id"]),  # 72 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "position_lots", ["lot_id"]),  # 118 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "positions_snapshot", ["run_id", "symbol"]),  # 1598 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "risk_metrics", ["run_id"]),  # 63 rows
    ("ag1_v3_gemini30_pro.duckdb", "core", "runs", ["run_id"]),  # 72 rows
    ("ag1_v3_gemini30_pro.duckdb", "main", "portfolio_positions_mtm_history", ["id"]),  # 8204 rows
    ("ag1_v3_gemini30_pro.duckdb", "main", "portfolio_positions_mtm_latest", ["symbol"]),  # 25 rows
    ("ag1_v3_gemini30_pro.duckdb", "main", "portfolio_positions_mtm_run_log", ["run_id"]),  # 335 rows
    ("ag1_v3_grok41_reasoning.duckdb", "cfg", "portfolio_config", ["config_version"]),  # 1 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "ai_signals", ["signal_id"]),  # 778 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "alerts", ["alert_id"]),  # 43 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "backfill_queue", ["request_id"]),  # 0 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "cash_ledger", ["cash_tx_id"]),  # 33 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "fills", ["fill_id"]),  # 219 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "instruments", ["symbol"]),  # 93 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "market_prices", ["ts", "symbol", "source"]),  # 1419 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "orders", ["order_id"]),  # 219 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "portfolio_snapshot", ["run_id"]),  # 82 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "position_lots", ["lot_id"]),  # 115 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "positions_snapshot", ["run_id", "symbol"]),  # 1514 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "risk_metrics", ["run_id"]),  # 71 rows
    ("ag1_v3_grok41_reasoning.duckdb", "core", "runs", ["run_id"]),  # 83 rows
    ("ag1_v3_grok41_reasoning.duckdb", "main", "portfolio_positions_mtm_history", ["id"]),  # 6890 rows
    ("ag1_v3_grok41_reasoning.duckdb", "main", "portfolio_positions_mtm_latest", ["symbol"]),  # 21 rows
    ("ag1_v3_grok41_reasoning.duckdb", "main", "portfolio_positions_mtm_run_log", ["run_id"]),  # 334 rows
    ("ag2_v3.duckdb", "main", "ai_dedup_cache", ["symbol", "interval_key"]),  # 73 rows
    ("ag2_v3.duckdb", "main", "batch_state", ["key"]),  # 3 rows
    ("ag2_v3.duckdb", "main", "run_log", ["run_id"]),  # 664 rows
    ("ag2_v3.duckdb", "main", "technical_signals", ["id"]),  # 7756 rows
    ("ag2_v3.duckdb", "main", "universe", ["symbol"]),  # 463 rows
    ("ag3_v2.duckdb", "main", "analyst_consensus_history", ["record_id"]),  # 3006 rows
    ("ag3_v2.duckdb", "main", "batch_state", ["key"]),  # 1 rows
    ("ag3_v2.duckdb", "main", "fundamental_metrics_history", ["record_id"]),  # 95559 rows
    ("ag3_v2.duckdb", "main", "fundamentals_snapshot", ["snapshot_id"]),  # 3006 rows
    ("ag3_v2.duckdb", "main", "fundamentals_triage_history", ["record_id"]),  # 3006 rows
    ("ag3_v2.duckdb", "main", "run_log", ["run_id"]),  # 56 rows
    ("ag4_spe_v2.duckdb", "main", "news_errors", ["error_id"]),  # 8 rows
    ("ag4_spe_v2.duckdb", "main", "news_history", ["news_id"]),  # 13908 rows
    ("ag4_spe_v2.duckdb", "main", "run_log", ["run_id"]),  # 289 rows
    ("ag4_spe_v2.duckdb", "main", "universe_symbols", ["symbol"]),  # 0 rows
    ("ag4_spe_v2.duckdb", "main", "workflow_state", ["state_key"]),  # 1 rows
    ("ag4_v3.duckdb", "main", "ag4_fx_macro", ["run_id"]),  # 305 rows
    ("ag4_v3.duckdb", "main", "ag4_fx_pairs", ["id"]),  # 0 rows
    ("ag4_v3.duckdb", "main", "news_errors", ["dedupe_key"]),  # 8557 rows
    ("ag4_v3.duckdb", "main", "news_history", ["dedupe_key"]),  # 12027 rows
    ("ag4_v3.duckdb", "main", "run_log", ["run_id"]),  # 456 rows
    ("yf_enrichment_v1.duckdb", "main", "run_log", ["run_id"]),  # 64 rows
    ("yf_enrichment_v1.duckdb", "main", "yf_symbol_enrichment_history", ["row_id"]),  # 4630 rows
]

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.db_dir.is_dir():
        print(f"ERREUR: {args.db_dir} introuvable", file=sys.stderr)
        return 2

    # Group by DB
    by_db = {}
    for db, schema, table, cols in PK_FIXES:
        by_db.setdefault(db, []).append((schema, table, cols))

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"═══ Fix PK DuckDB [{mode}] — {len(PK_FIXES)} contraintes sur {len(by_db)} DB ═══")
    print(f"DB dir: {args.db_dir}\n")

    total_applied = total_skipped = total_errors = 0

    for db, items in sorted(by_db.items()):
        db_path = args.db_dir / db
        if not db_path.exists():
            print(f"── {db:40s} ✗ fichier absent")
            continue

        # Check .wal actif
        wal = Path(str(db_path) + ".wal")
        if wal.exists() and wal.stat().st_size > 0:
            print(f"── {db:40s} ✗ WAL actif ({wal.stat().st_size} B) — arrête n8n d'abord")
            total_errors += 1
            continue

        # Compter PK avant
        con = duckdb.connect(str(db_path), read_only=True)
        pk_before = con.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE constraint_type='PRIMARY KEY' AND table_name NOT LIKE 'backup_%'
        """).fetchone()[0]
        con.close()

        print(f"── {db:40s} (avant: PK={pk_before})")
        if args.dry_run:
            print(f"   [dry-run] {len(items)} ALTER TABLE seraient appliqués\n")
            continue

        applied = skipped = errors = 0
        con = duckdb.connect(str(db_path))
        try:
            for schema, table, cols in items:
                col_list = ", ".join([f'"{c}"' for c in cols])
                sql = f'ALTER TABLE "{schema}"."{table}" ADD PRIMARY KEY ({col_list})'
                try:
                    con.execute(sql)
                    applied += 1
                except duckdb.Error as e:
                    msg = str(e).lower()
                    if "has a primary key" in msg or "already" in msg:
                        skipped += 1
                    else:
                        errors += 1
                        print(f"   ✗ {schema}.{table}: {e}")
            con.execute("CHECKPOINT")
        finally:
            con.close()

        # Compter PK après
        con = duckdb.connect(str(db_path), read_only=True)
        pk_after = con.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE constraint_type='PRIMARY KEY' AND table_name NOT LIKE 'backup_%'
        """).fetchone()[0]
        con.close()

        print(f"   → appliqués: {applied}, déjà présents: {skipped}, erreurs: {errors}  (PK après: {pk_after})\n")
        total_applied += applied
        total_skipped += skipped
        total_errors += errors

    print("═══ Bilan ═══")
    print(f"  ALTER appliqués:  {total_applied}")
    print(f"  Déjà présents:    {total_skipped}")
    print(f"  Erreurs:          {total_errors}")
    return 1 if total_errors else 0

if __name__ == "__main__":
    sys.exit(main())
