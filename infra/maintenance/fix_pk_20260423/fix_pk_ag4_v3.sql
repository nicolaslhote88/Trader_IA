-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage recommandé:
--   python3 apply_fix.py --db-dir /local-files/duckdb
-- Usage alternatif (si CLI duckdb installé):
--   duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag4_v3.duckdb
-- 5 PRIMARY KEY à restaurer

ALTER TABLE "main"."ag4_fx_macro" ADD PRIMARY KEY ("run_id");  -- 305 rows
ALTER TABLE "main"."ag4_fx_pairs" ADD PRIMARY KEY ("id");  -- 0 rows
ALTER TABLE "main"."news_errors" ADD PRIMARY KEY ("dedupe_key");  -- 8557 rows
ALTER TABLE "main"."news_history" ADD PRIMARY KEY ("dedupe_key");  -- 12027 rows
ALTER TABLE "main"."run_log" ADD PRIMARY KEY ("run_id");  -- 456 rows

CHECKPOINT;
