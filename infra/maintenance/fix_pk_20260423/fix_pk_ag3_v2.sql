-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage:  duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag3_v2.duckdb
-- 6 PRIMARY KEY à restaurer

BEGIN TRANSACTION;

ALTER TABLE "main"."analyst_consensus_history" ADD PRIMARY KEY ("record_id");  -- 3006 rows
ALTER TABLE "main"."batch_state" ADD PRIMARY KEY ("key");  -- 1 rows
ALTER TABLE "main"."fundamental_metrics_history" ADD PRIMARY KEY ("record_id");  -- 95559 rows
ALTER TABLE "main"."fundamentals_snapshot" ADD PRIMARY KEY ("snapshot_id");  -- 3006 rows
ALTER TABLE "main"."fundamentals_triage_history" ADD PRIMARY KEY ("record_id");  -- 3006 rows
ALTER TABLE "main"."run_log" ADD PRIMARY KEY ("run_id");  -- 56 rows

COMMIT;
CHECKPOINT;
