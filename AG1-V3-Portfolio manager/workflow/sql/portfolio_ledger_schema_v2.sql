CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS cfg;

CREATE TABLE IF NOT EXISTS core.runs (
  run_id VARCHAR PRIMARY KEY,
  ts_start TIMESTAMPTZ NOT NULL,
  ts_end TIMESTAMPTZ,
  tz VARCHAR DEFAULT 'Europe/Paris',
  strategy_version VARCHAR,
  config_version VARCHAR,
  prompt_version VARCHAR,
  model VARCHAR,
  n8n_execution_id VARCHAR,
  decision_summary VARCHAR,
  data_ok_for_trading BOOLEAN,
  price_coverage_pct DOUBLE,
  news_count INTEGER,
  ai_cost_eur DECIMAL(18,2),
  expected_fees_eur DECIMAL(18,2),
  warnings_json JSON,
  agent_output_json JSON,
  risk_gate_json JSON,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.instruments (
  symbol VARCHAR PRIMARY KEY,
  name VARCHAR,
  asset_class VARCHAR,
  exchange VARCHAR,
  currency VARCHAR(3),
  isin VARCHAR,
  sector VARCHAR,
  industry VARCHAR,
  is_active BOOLEAN DEFAULT TRUE,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS core.market_prices (
  ts TIMESTAMPTZ NOT NULL,
  symbol VARCHAR NOT NULL REFERENCES core.instruments(symbol),
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  adj_close DOUBLE,
  volume BIGINT,
  source VARCHAR,
  asof TIMESTAMPTZ,
  PRIMARY KEY (ts, symbol, source)
);

CREATE TABLE IF NOT EXISTS core.orders (
  order_id VARCHAR PRIMARY KEY,
  run_id VARCHAR NOT NULL REFERENCES core.runs(run_id),
  ts_created TIMESTAMPTZ NOT NULL,
  symbol VARCHAR NOT NULL REFERENCES core.instruments(symbol),
  side VARCHAR NOT NULL,
  intent VARCHAR NOT NULL,
  order_type VARCHAR NOT NULL,
  qty DECIMAL(18,8) NOT NULL,
  limit_price DOUBLE,
  stop_price DOUBLE,
  time_in_force VARCHAR DEFAULT 'DAY',
  status VARCHAR NOT NULL,
  broker VARCHAR,
  broker_order_id VARCHAR,
  reason VARCHAR,
  rationale_json JSON
);

CREATE TABLE IF NOT EXISTS core.fills (
  fill_id VARCHAR PRIMARY KEY,
  order_id VARCHAR NOT NULL REFERENCES core.orders(order_id),
  run_id VARCHAR NOT NULL REFERENCES core.runs(run_id),
  ts_fill TIMESTAMPTZ NOT NULL,
  qty DECIMAL(18,8) NOT NULL,
  price DOUBLE NOT NULL,
  fees_eur DECIMAL(18,2) DEFAULT 0,
  slippage_bps DOUBLE,
  liquidity VARCHAR,
  raw_fill_json JSON
);

CREATE TABLE IF NOT EXISTS core.cash_ledger (
  cash_tx_id VARCHAR PRIMARY KEY,
  run_id VARCHAR REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  currency VARCHAR(3) NOT NULL DEFAULT 'EUR',
  amount DECIMAL(18,2) NOT NULL,
  type VARCHAR NOT NULL,
  symbol VARCHAR,
  ref_id VARCHAR,
  notes VARCHAR,
  payload_json JSON
);

CREATE TABLE IF NOT EXISTS core.position_lots (
  lot_id VARCHAR PRIMARY KEY,
  symbol VARCHAR NOT NULL REFERENCES core.instruments(symbol),
  open_fill_id VARCHAR NOT NULL REFERENCES core.fills(fill_id),
  open_ts TIMESTAMPTZ NOT NULL,
  open_qty DECIMAL(18,8) NOT NULL,
  open_price DOUBLE NOT NULL,
  open_fees_eur DECIMAL(18,2) DEFAULT 0,
  remaining_qty DECIMAL(18,8) NOT NULL,
  status VARCHAR NOT NULL,
  close_ts TIMESTAMPTZ,
  close_fill_id VARCHAR REFERENCES core.fills(fill_id),
  realized_pnl_eur DECIMAL(18,2),
  close_method VARCHAR DEFAULT 'FIFO',
  meta_json JSON
);

CREATE TABLE IF NOT EXISTS core.positions_snapshot (
  run_id VARCHAR NOT NULL REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  symbol VARCHAR NOT NULL REFERENCES core.instruments(symbol),
  qty DECIMAL(18,8) NOT NULL,
  avg_cost DOUBLE,
  last_price DOUBLE,
  market_value_eur DECIMAL(18,2),
  unrealized_pnl_eur DECIMAL(18,2),
  weight_pct DOUBLE,
  PRIMARY KEY (run_id, symbol)
);

CREATE TABLE IF NOT EXISTS core.portfolio_snapshot (
  run_id VARCHAR PRIMARY KEY REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  cash_eur DECIMAL(18,2) NOT NULL,
  equity_eur DECIMAL(18,2) NOT NULL,
  total_value_eur DECIMAL(18,2) NOT NULL,
  cum_fees_eur DECIMAL(18,2) NOT NULL,
  cum_ai_cost_eur DECIMAL(18,2) NOT NULL,
  trades_this_run INTEGER NOT NULL,
  total_pnl_eur DECIMAL(18,2),
  roi DOUBLE,
  drawdown_pct DOUBLE,
  meta_json JSON
);

CREATE TABLE IF NOT EXISTS core.ai_signals (
  signal_id VARCHAR PRIMARY KEY,
  run_id VARCHAR NOT NULL REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  symbol VARCHAR NOT NULL REFERENCES core.instruments(symbol),
  signal VARCHAR NOT NULL,
  confidence INTEGER,
  horizon VARCHAR,
  entry_zone VARCHAR,
  stop_loss DOUBLE,
  take_profit DOUBLE,
  risk_score INTEGER,
  catalyst VARCHAR,
  rationale VARCHAR,
  payload_json JSON
);

CREATE TABLE IF NOT EXISTS core.risk_metrics (
  run_id VARCHAR PRIMARY KEY REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  cash_pct DOUBLE,
  top1_pos_pct DOUBLE,
  top1_sector_pct DOUBLE,
  var95_est_eur DECIMAL(18,2),
  positions_count INTEGER,
  risk_status VARCHAR,
  limits_json JSON
);

CREATE TABLE IF NOT EXISTS core.alerts (
  alert_id VARCHAR PRIMARY KEY,
  run_id VARCHAR REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  severity VARCHAR NOT NULL,
  category VARCHAR NOT NULL,
  symbol VARCHAR DEFAULT 'GLOBAL',
  message VARCHAR NOT NULL,
  code VARCHAR,
  payload_json JSON
);

CREATE TABLE IF NOT EXISTS core.backfill_queue (
  request_id VARCHAR PRIMARY KEY,
  run_id VARCHAR REFERENCES core.runs(run_id),
  ts TIMESTAMPTZ NOT NULL,
  symbol VARCHAR NOT NULL,
  needs VARCHAR NOT NULL,
  severity VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  why VARCHAR,
  completed_at TIMESTAMPTZ,
  response_json JSON,
  notes VARCHAR
);

CREATE TABLE IF NOT EXISTS cfg.portfolio_config (
  config_version VARCHAR PRIMARY KEY,
  initial_capital_eur DECIMAL(18,2),
  lot_close_method VARCHAR DEFAULT 'FIFO',
  default_fee_bps DOUBLE,
  kill_switch_active BOOLEAN,
  max_pos_pct DOUBLE,
  max_sector_pct DOUBLE,
  max_daily_drawdown_pct DOUBLE,
  updated_at TIMESTAMPTZ,
  payload_json JSON
);

CREATE INDEX IF NOT EXISTS idx_runs_ts_start ON core.runs(ts_start);
CREATE INDEX IF NOT EXISTS idx_orders_run_id ON core.orders(run_id);
CREATE INDEX IF NOT EXISTS idx_fills_run_id ON core.fills(run_id);
CREATE INDEX IF NOT EXISTS idx_cash_ledger_run_id ON core.cash_ledger(run_id);
CREATE INDEX IF NOT EXISTS idx_position_lots_symbol_status ON core.position_lots(symbol, status);
CREATE INDEX IF NOT EXISTS idx_market_prices_symbol_ts ON core.market_prices(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_alerts_run_id ON core.alerts(run_id);
