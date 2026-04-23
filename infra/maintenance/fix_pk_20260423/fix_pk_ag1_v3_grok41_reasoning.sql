-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Restauration PRIMARY KEY perdues lors de la compaction 2026-04-22
-- Généré automatiquement depuis les snapshots originaux.
-- Pré-check: zéro doublon, zéro null sur les colonnes clés (validé).
-- Usage recommandé:
--   python3 apply_fix.py --db-dir /local-files/duckdb
-- Usage alternatif (si CLI duckdb installé):
--   duckdb <db.duckdb> < fix_pk_<db>.sql
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DB: ag1_v3_grok41_reasoning.duckdb
-- 17 PRIMARY KEY à restaurer

ALTER TABLE "cfg"."portfolio_config" ADD PRIMARY KEY ("config_version");  -- 1 rows
ALTER TABLE "core"."ai_signals" ADD PRIMARY KEY ("signal_id");  -- 778 rows
ALTER TABLE "core"."alerts" ADD PRIMARY KEY ("alert_id");  -- 43 rows
ALTER TABLE "core"."backfill_queue" ADD PRIMARY KEY ("request_id");  -- 0 rows
ALTER TABLE "core"."cash_ledger" ADD PRIMARY KEY ("cash_tx_id");  -- 33 rows
ALTER TABLE "core"."fills" ADD PRIMARY KEY ("fill_id");  -- 219 rows
ALTER TABLE "core"."instruments" ADD PRIMARY KEY ("symbol");  -- 93 rows
ALTER TABLE "core"."market_prices" ADD PRIMARY KEY ("ts", "symbol", "source");  -- 1419 rows
ALTER TABLE "core"."orders" ADD PRIMARY KEY ("order_id");  -- 219 rows
ALTER TABLE "core"."portfolio_snapshot" ADD PRIMARY KEY ("run_id");  -- 82 rows
ALTER TABLE "core"."position_lots" ADD PRIMARY KEY ("lot_id");  -- 115 rows
ALTER TABLE "core"."positions_snapshot" ADD PRIMARY KEY ("run_id", "symbol");  -- 1514 rows
ALTER TABLE "core"."risk_metrics" ADD PRIMARY KEY ("run_id");  -- 71 rows
ALTER TABLE "core"."runs" ADD PRIMARY KEY ("run_id");  -- 83 rows
ALTER TABLE "main"."portfolio_positions_mtm_history" ADD PRIMARY KEY ("id");  -- 6890 rows
ALTER TABLE "main"."portfolio_positions_mtm_latest" ADD PRIMARY KEY ("symbol");  -- 21 rows
ALTER TABLE "main"."portfolio_positions_mtm_run_log" ADD PRIMARY KEY ("run_id");  -- 334 rows

CHECKPOINT;
