-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage:  duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag1_v3_chatgpt52.duckdb
-- 17 PRIMARY KEY à restaurer

BEGIN TRANSACTION;

ALTER TABLE "cfg"."portfolio_config" ADD PRIMARY KEY ("config_version");  -- 1 rows
ALTER TABLE "core"."ai_signals" ADD PRIMARY KEY ("signal_id");  -- 554 rows
ALTER TABLE "core"."alerts" ADD PRIMARY KEY ("alert_id");  -- 97 rows
ALTER TABLE "core"."backfill_queue" ADD PRIMARY KEY ("request_id");  -- 0 rows
ALTER TABLE "core"."cash_ledger" ADD PRIMARY KEY ("cash_tx_id");  -- 15 rows
ALTER TABLE "core"."fills" ADD PRIMARY KEY ("fill_id");  -- 140 rows
ALTER TABLE "core"."instruments" ADD PRIMARY KEY ("symbol");  -- 57 rows
ALTER TABLE "core"."market_prices" ADD PRIMARY KEY ("ts", "symbol", "source");  -- 1556 rows
ALTER TABLE "core"."orders" ADD PRIMARY KEY ("order_id");  -- 140 rows
ALTER TABLE "core"."portfolio_snapshot" ADD PRIMARY KEY ("run_id");  -- 84 rows
ALTER TABLE "core"."position_lots" ADD PRIMARY KEY ("lot_id");  -- 66 rows
ALTER TABLE "core"."positions_snapshot" ADD PRIMARY KEY ("run_id", "symbol");  -- 1629 rows
ALTER TABLE "core"."risk_metrics" ADD PRIMARY KEY ("run_id");  -- 76 rows
ALTER TABLE "core"."runs" ADD PRIMARY KEY ("run_id");  -- 85 rows
ALTER TABLE "main"."portfolio_positions_mtm_history" ADD PRIMARY KEY ("id");  -- 7027 rows
ALTER TABLE "main"."portfolio_positions_mtm_latest" ADD PRIMARY KEY ("symbol");  -- 20 rows
ALTER TABLE "main"."portfolio_positions_mtm_run_log" ADD PRIMARY KEY ("run_id");  -- 334 rows

COMMIT;
CHECKPOINT;
