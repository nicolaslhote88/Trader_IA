-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage recommandé:
--   python3 apply_fix.py --db-dir /local-files/duckdb
-- Usage alternatif (si CLI duckdb installé):
--   duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: yf_enrichment_v1.duckdb
-- 2 PRIMARY KEY à restaurer

ALTER TABLE "main"."run_log" ADD PRIMARY KEY ("run_id");  -- 64 rows
ALTER TABLE "main"."yf_symbol_enrichment_history" ADD PRIMARY KEY ("row_id");  -- 4630 rows

CHECKPOINT;
