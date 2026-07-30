#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const root = process.env.AG1_V4_EXPORT_ROOT || path.resolve(__dirname, "..");
const outPath = process.env.AG1_V4_SMOKE_BUNDLE_OUT || "";
const smokeDbPath = process.env.AG1_V4_SMOKE_DB_PATH || "/files/duckdb/ag1_v4_consensus_smoke.duckdb";

function readCode(relPath) {
  return fs.readFileSync(path.join(root, relPath), "utf8");
}

async function runNode(relPath, json, inputItems = null, env = {}) {
  const code = readCode(relPath);
  const input = {
    all: () => (inputItems || [{ json }]),
  };
  const fn = new Function("$json", "$input", "$env", `return (async () => {\n${code}\n})()`);
  return await fn(json, input, env);
}

function action(symbol, confidence, rationale, overrides = {}) {
  return {
    symbol,
    action: "OPEN",
    confidence,
    targetWeightPct: 10,
    rationale,
    nextReviewDays: 3,
    ...overrides,
  };
}

async function runPostAgentChain(context, proposals, env = {}) {
  const consensus = (await runNode(
    "nodes/post_agent/06_build_consensus_v4.code.js",
    {},
    [{ json: context }, ...proposals.map((json) => ({ json }))]
  ))[0].json;
  const safety = (await runNode("nodes/post_agent/07_validate_enforce_safety_v5.code.js", consensus))[0].json;
  const ibkr = (await runNode("nodes/post_agent/07b_ibkr_send_orders.js", safety, null, {
    IBKR_DRY_RUN: "true",
    IBKR_SEND_DRY_RUN_TO_BROKER: "false",
    AG1_ACTIONS_LIVE_ORDERS_ENABLED: "false",
    AG1_V4_ACTIONS_IBKR_ENABLED_MODELS: "ag1_v4_consensus",
    ...env,
  }))[0].json;
  const bundle = (await runNode("nodes/post_agent/08_build_duckdb_bundle.code.js", ibkr))[0].json;
  return { consensus, safety, ibkr, bundle };
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${expected}, got ${actual}`);
  }
}

async function main() {
  const context = {
    run: {
      runId: "RUN_SMOKE_AG1_V4",
      timestampParis: "2026-06-10T12:00:00+02:00",
      timestampUtc: "2026-06-10T10:00:00.000Z",
      strategyVersion: "strategy_v4_consensus",
      configVersion: "ag1_v4_consensus_v1",
      promptVersion: "prompt_v4_consensus",
      model: "ag1_v4_consensus",
      db_path: smokeDbPath,
    },
    config: {
      default_fee_bps: 10,
      max_pos_pct: 25,
      max_sector_pct: 40,
      max_order_value_pct: 25,
      max_spread_pct: 1.5,
      require_limit_buys: true,
      kill_switch_active: false,
    },
    meta: { initialCapitalEUR: 10000 },
    portfolioBrief: {
      cash: 10000,
      totalValue: 10000,
      marketValue: 0,
      exposurePct: 0,
      positions: [],
      summary: {
        cash: 10000,
        totalValue: 10000,
        marketValue: 0,
        exposurePct: 0,
        positionsCount: 0,
      },
    },
    opportunity_pack: {
      rows: [{
        symbol: "AAPL",
        symbol_yahoo: "AAPL",
        asset_class: "EQUITY",
        sector: "Technology",
        decision: "Entrer / Renforcer",
        gates: "OK",
        entry: 200,
        stop: 190,
        tp: 225,
        spread_pct: 0.08,
        liquidity: {
          status: "OK",
          contractResolved: true,
          estimatedOrderToVolumePct: 0.001,
        },
      }],
    },
    db_path: smokeDbPath,
  };

  const buyProposals = [
    {
      modelKey: "chatgpt52",
      modelName: "OpenAI GPT-5.6 Sol",
      modelId: "gpt-5.6-sol",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("AAPL", 72, "Smoke GPT buy")] },
    },
    {
      modelKey: "grok41_reasoning",
      modelName: "DeepSeek V4 Pro",
      modelId: "deepseek-v4-pro",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("AAPL", 68, "Smoke DeepSeek buy")] },
    },
    {
      modelKey: "claude_sonnet46",
      modelName: "Anthropic Claude Opus 4.8",
      modelId: "claude-opus-4-8",
      extractorStatus: "OK_OBJECT",
      output: { actions: [{ symbol: "AAPL", action: "WATCH", confidence: 55, targetWeightPct: null, rationale: "Wait", nextReviewDays: 3 }] },
    },
  ];

  const buy = await runPostAgentChain(context, buyProposals);
  if (buy.safety.decision !== "TRADE") {
    console.error("buy safety diagnostic", JSON.stringify(buy.safety, null, 2));
  }
  assertEqual(buy.consensus.decision, "TRADE", "buy consensus decision");
  assertEqual(buy.safety.decision, "TRADE", "buy safety decision");
  assertEqual(Array.isArray(buy.safety.orders) ? buy.safety.orders.length : 0, 1, "buy order count");

  const sellContext = JSON.parse(JSON.stringify(context));
  sellContext.run = { ...context.run, runId: "RUN_SMOKE_AG1_V4_SELL_HELD" };
  sellContext.portfolioBrief = {
    cash: 9240,
    totalValue: 10000,
    marketValue: 760,
    exposurePct: 7.6,
    positions: [{
      Symbol: "ELEC.PA",
      Quantity: 8,
      LastPrice: 95,
      MarketValue: 760,
      Sector: "Utilities",
      AssetClass: "EQUITY",
    }],
    summary: {
      cash: 9240,
      totalValue: 10000,
      marketValue: 760,
      exposurePct: 7.6,
      positionsCount: 1,
    },
  };
  sellContext.opportunity_pack.rows = [];

  const sellProposals = [
    {
      modelKey: "chatgpt52",
      modelName: "OpenAI GPT-5.6 Sol",
      modelId: "gpt-5.6-sol",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("ELEC.PA", 65, "Reduce held utility", { action: "DECREASE", targetWeightPct: 4, assetClass: "EQUITY", sector: "Utilities" })] },
    },
    {
      modelKey: "claude_sonnet46",
      modelName: "Anthropic Claude Opus 4.8",
      modelId: "claude-opus-4-8",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("ELEC.PA", 66, "Reduce held utility", { action: "DECREASE", targetWeightPct: 4, assetClass: "EQUITY", sector: "Utilities" })] },
    },
    {
      modelKey: "grok41_reasoning",
      modelName: "DeepSeek V4 Pro",
      modelId: "deepseek-v4-pro",
      extractorStatus: "OK_OBJECT",
      output: { actions: [{ symbol: "ELEC.PA", action: "WATCH", confidence: 55, targetWeightPct: null, rationale: "Wait", nextReviewDays: 3 }] },
    },
  ];

  const sell = await runPostAgentChain(sellContext, sellProposals);
  assertEqual(sell.consensus.decision, "TRADE", "held sell consensus decision");
  assertEqual(sell.consensus.agentDecision.actions[0].targetQty, 4, "held sell final target qty");
  assertEqual(sell.safety.decision, "TRADE", "held sell safety decision");
  assertEqual(sell.safety.orders[0].side, "SELL", "held sell order side");
  assertEqual(sell.safety.orders[0].quantity, 4, "held sell order quantity");

  const summary = {
    buyDecision: buy.consensus.decision,
    buyConsensusActions: buy.consensus.agentDecision.actions.length,
    buySafetyDecision: buy.safety.decision,
    buyOrders: Array.isArray(buy.safety.orders) ? buy.safety.orders.length : 0,
    sellHeldDecision: sell.consensus.decision,
    sellHeldConsensusActions: sell.consensus.agentDecision.actions.length,
    sellHeldSafetyDecision: sell.safety.decision,
    sellHeldOrders: Array.isArray(sell.safety.orders) ? sell.safety.orders.length : 0,
    sellHeldQuantity: sell.safety.orders[0]?.quantity,
    ibkrDryRun: buy.ibkr.ibkr?.dryRun && sell.ibkr.ibkr?.dryRun,
    bundleOrders: buy.bundle.bundle.orders.length + sell.bundle.bundle.orders.length,
    bundleFills: buy.bundle.bundle.fills.length + sell.bundle.bundle.fills.length,
    modelProposals: buy.bundle.bundle.model_proposals.length + sell.bundle.bundle.model_proposals.length,
    consensusVotes: buy.bundle.bundle.consensus_votes.length + sell.bundle.bundle.consensus_votes.length,
    consensusDecisions: buy.bundle.bundle.consensus_decisions.length + sell.bundle.bundle.consensus_decisions.length,
    dbPath: buy.bundle.db_path,
  };

  if (outPath) {
    fs.writeFileSync(outPath, JSON.stringify({ buy: buy.bundle.bundle, sell: sell.bundle.bundle }, null, 2));
  }
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : err);
  process.exit(1);
});
