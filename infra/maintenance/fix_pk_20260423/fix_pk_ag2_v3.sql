-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage:  duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag2_v3.duckdb
-- 5 PRIMARY KEY à restaurer

BEGIN TRANSACTION;

ALTER TABLE "main"."ai_dedup_cache" ADD PRIMARY KEY ("symbol", "interval_key");  -- 73 rows
ALTER TABLE "main"."batch_state" ADD PRIMARY KEY ("key");  -- 3 rows
ALTER TABLE "main"."run_log" ADD PRIMARY KEY ("run_id");  -- 664 rows
ALTER TABLE "main"."technical_signals" ADD PRIMARY KEY ("id");  -- 7756 rows
ALTER TABLE "main"."universe" ADD PRIMARY KEY ("symbol");  -- 463 rows

COMMIT;
CHECKPOINT;
