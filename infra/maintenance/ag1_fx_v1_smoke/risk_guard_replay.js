#!/usr/bin/env node

const fs = require('fs');

const riskNodePath = process.argv[2] || '/tmp/ag1_risk.js';
const code = fs.readFileSync(riskNodePath, 'utf8');
const env = {};

const baseBrief = {
  config: { capital_eur: 10000, leverage_max: 1, portfolio_base_ccy: 'EUR' },
  limits: { max_pair_pct: 0.25, max_currency_exposure_pct: 0.8, max_daily_drawdown_pct: 0.05 },
  universe: {
    pairs: ['EURUSD', 'USDCAD', 'EURCAD'],
    metadata: [
      { pair: 'EURUSD', base_ccy: 'EUR', quote_ccy: 'USD', pip_size: 0.0001 },
      { pair: 'USDCAD', base_ccy: 'USD', quote_ccy: 'CAD', pip_size: 0.0001 },
      { pair: 'EURCAD', base_ccy: 'EUR', quote_ccy: 'CAD', pip_size: 0.0001 },
    ],
  },
  technical_signals: [
    { pair: 'EURUSD', last_close: 1.08 },
    { pair: 'USDCAD', last_close: 1.351 },
    { pair: 'EURCAD', last_close: 1.46 },
  ],
  portfolio_state: {
    equity_eur: 10000,
    margin_free_eur: 10000,
    drawdown_day_frac: 0,
    open_lots: [],
    reconciliation: { cash_balances: { ibkr_cash_by_currency: { EUR: 10000 } } },
  },
};

function eurusdProfile() {
  return {
    pair: 'EURUSD',
    decision: {
      trade_permission: 'ALLOW',
      preferred_action: 'OPEN_OR_ADD',
      decision_alignment: 'risk-guard-replay',
      cube: {
        structural_data_complete: true,
        structural_data_quality: 0.95,
        structural_confidence_floor: 0.85,
        crowded_warning: false,
        event_risk_score: 0.1,
        cube_zone: 'convergence_multi_horizon_confirmed',
        cube_direction: 'BUY_BASE',
        x_technical: 0.4,
        y_news_event: 0.2,
        z_three_pillars: 0.5,
        portfolio_action_hint: 'OPEN_OR_ADD',
      },
    },
  };
}

function makeJson(runId, decisions, openLots) {
  const brief = JSON.parse(JSON.stringify(baseBrief));
  brief.portfolio_state.open_lots = openLots || [];
  return {
    run_id: runId,
    brief,
    decision_json: { decisions },
    llm_brief: { market_watch: [eurusdProfile()], pair_matrix: [] },
    ibkr_reconciliation: { cash_balances: { ibkr_cash_by_currency: { EUR: 10000 } } },
  };
}

function runCase(label, decisions, openLots) {
  const json = makeJson(label, decisions, openLots);
  const result = Function('$json', '_items', '$env', code)(json, [{ json }], env);
  return result[0].json.executable_orders.map((order) => ({
    pair: order.pair,
    decision: order.decision,
    status: order.status,
    rejection_reason: order.rejection_reason,
    expected_gross_profit_eur: order.economics_check?.expected_gross_profit_eur,
    required_gross_profit_eur: order.economics_check?.required_gross_profit_eur,
    net_after_exit_fee_eur: order.economics_check?.net_after_exit_fee_eur,
    hard_exit_reason: order.economics_check?.hard_exit_reason,
  }));
}

const recent = new Date(Date.now() - 60 * 60 * 1000).toISOString();

const cases = {
  micro_trade: runCase('UT_MICRO', [
    { decision: 'open_long', pair: 'EURUSD', size_lots: 0.005, stop_loss_price: 1.07, take_profit_price: 1.10, conviction: 0.8, horizon: 'swing', rationale: 'unit test' },
  ]),
  reward_too_small: runCase('UT_REWARD', [
    { decision: 'open_long', pair: 'EURUSD', size_lots: 0.02, stop_loss_price: 1.07, take_profit_price: 1.081, conviction: 0.8, horizon: 'swing', rationale: 'unit test' },
  ]),
  self_prefund_conflict: runCase('UT_PREFUND', [
    { decision: 'open_long', pair: 'EURUSD', size_lots: 0.02, stop_loss_price: 1.07, take_profit_price: 1.10, conviction: 0.8, horizon: 'swing', rationale: 'unit test' },
  ]),
  tiny_close_net_negative: runCase('UT_CLOSE', [
    { decision: 'close', lot_id_to_close: 'LOT_TINY', pair: 'USDCAD', conviction: 0.6, horizon: 'intraday', rationale: 'take tiny gross profit' },
  ], [
    { lot_id: 'LOT_TINY', pair: 'USDCAD', side: 'long', size_lots: 0.0012, open_price: 1.35, stop_loss_price: 1.34, take_profit_price: 1.37, open_at: recent },
  ]),
  duplicate_close_hard_exit: runCase('UT_DUP', [
    { decision: 'close', lot_id_to_close: 'LOT_DUP', pair: 'USDCAD', conviction: 0.6, horizon: 'intraday', rationale: 'take profit touched' },
    { decision: 'close', lot_id_to_close: 'LOT_DUP', pair: 'USDCAD', conviction: 0.6, horizon: 'intraday', rationale: 'take profit touched again' },
  ], [
    { lot_id: 'LOT_DUP', pair: 'USDCAD', side: 'long', size_lots: 0.0012, open_price: 1.34, stop_loss_price: 1.33, take_profit_price: 1.3505, open_at: recent },
  ]),
};

const expected = {
  micro_trade: ['TRADE_ECONOMICS_NOTIONAL_TOO_SMALL'],
  reward_too_small: ['TRADE_ECONOMICS_REWARD_TOO_SMALL'],
  self_prefund_conflict: ['PREFUNDING_SELF_PAIR_CONFLICT'],
  tiny_close_net_negative: ['CLOSE_ECONOMICS_NEGATIVE_NET'],
  duplicate_close_hard_exit: ['', 'DUPLICATE_CLOSE_LOT'],
};

const failures = [];
for (const [name, expectedReasons] of Object.entries(expected)) {
  const actualReasons = cases[name].map((order) => order.rejection_reason || '');
  if (JSON.stringify(actualReasons) !== JSON.stringify(expectedReasons)) {
    failures.push({ name, expected: expectedReasons, actual: actualReasons });
  }
}

console.log(JSON.stringify({ cases, failures }, null, 2));
if (failures.length > 0) {
  process.exitCode = 1;
}
