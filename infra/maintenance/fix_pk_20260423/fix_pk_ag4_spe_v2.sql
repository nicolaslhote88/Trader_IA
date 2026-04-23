-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage:  duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag4_spe_v2.duckdb
-- 5 PRIMARY KEY à restaurer

BEGIN TRANSACTION;

ALTER TABLE "main"."news_errors" ADD PRIMARY KEY ("error_id");  -- 8 rows
ALTER TABLE "main"."news_history" ADD PRIMARY KEY ("news_id");  -- 13908 rows
ALTER TABLE "main"."run_log" ADD PRIMARY KEY ("run_id");  -- 289 rows
ALTER TABLE "main"."universe_symbols" ADD PRIMARY KEY ("symbol");  -- 0 rows
ALTER TABLE "main"."workflow_state" ADD PRIMARY KEY ("state_key");  -- 1 rows

COMMIT;
CHECKPOINT;
