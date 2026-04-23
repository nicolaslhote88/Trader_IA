-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage:  duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag1_v3_gemini30_pro.duckdb
-- 17 PRIMARY KEY à restaurer

BEGIN TRANSACTION;

ALTER TABLE "cfg"."portfolio_config" ADD PRIMARY KEY ("config_version");  -- 1 rows
ALTER TABLE "core"."ai_signals" ADD PRIMARY KEY ("signal_id");  -- 405 rows
ALTER TABLE "core"."alerts" ADD PRIMARY KEY ("alert_id");  -- 42 rows
ALTER TABLE "core"."backfill_queue" ADD PRIMARY KEY ("request_id");  -- 0 rows
ALTER TABLE "core"."cash_ledger" ADD PRIMARY KEY ("cash_tx_id");  -- 56 rows
ALTER TABLE "core"."fills" ADD PRIMARY KEY ("fill_id");  -- 233 rows
ALTER TABLE "core"."instruments" ADD PRIMARY KEY ("symbol");  -- 92 rows
ALTER TABLE "core"."market_prices" ADD PRIMARY KEY ("ts", "symbol", "source");  -- 1466 rows
ALTER TABLE "core"."orders" ADD PRIMARY KEY ("order_id");  -- 233 rows
ALTER TABLE "core"."portfolio_snapshot" ADD PRIMARY KEY ("run_id");  -- 72 rows
ALTER TABLE "core"."position_lots" ADD PRIMARY KEY ("lot_id");  -- 118 rows
ALTER TABLE "core"."positions_snapshot" ADD PRIMARY KEY ("run_id", "symbol");  -- 1598 rows
ALTER TABLE "core"."risk_metrics" ADD PRIMARY KEY ("run_id");  -- 63 rows
ALTER TABLE "core"."runs" ADD PRIMARY KEY ("run_id");  -- 72 rows
ALTER TABLE "main"."portfolio_positions_mtm_history" ADD PRIMARY KEY ("id");  -- 8204 rows
ALTER TABLE "main"."portfolio_positions_mtm_latest" ADD PRIMARY KEY ("symbol");  -- 25 rows
ALTER TABLE "main"."portfolio_positions_mtm_run_log" ADD PRIMARY KEY ("run_id");  -- 335 rows

COMMIT;
CHECKPOINT;
