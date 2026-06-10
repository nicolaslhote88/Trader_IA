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

function action(symbol, qty, limitPrice, confidence, rationale) {
  return {
    symbol,
    symbol_internal: symbol,
    symbol_yahoo: symbol,
    assetClass: "EQUITY",
    sector: "Technology",
    action: "OPEN",
    confidence,
    targetQty: qty,
    entryPlan: { orderType: "LIMIT", limitPrice, timeInForce: "DAY" },
    riskPlan: { stopLossPct: -5, takeProfitPct: 12, maxLossEUR: 90 },
    rationale,
    Data_Age_H1_Hours: 1,
    Data_Age_D1_Hours: 4,
    SpreadPct: 0.08,
    dataQualityFlags: "",
  };
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
    db_path: smokeDbPath,
  };

  const proposals = [
    {
      modelKey: "chatgpt52",
      modelName: "OpenAI GPT-5.2",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("AAPL", 10, 180, 72, "Smoke GPT buy")] },
    },
    {
      modelKey: "grok41_reasoning",
      modelName: "xAI Grok 4.1 Reasoning",
      extractorStatus: "OK_OBJECT",
      output: { actions: [action("AAPL", 8, 179, 68, "Smoke Grok buy")] },
    },
    {
      modelKey: "gemini30_pro",
      modelName: "Google Gemini 3.0 Pro",
      extractorStatus: "OK_OBJECT",
      output: { actions: [{ symbol: "AAPL", assetClass: "EQUITY", action: "WATCH", confidence: 55 }] },
    },
  ];

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
  }))[0].json;
  const bundle = (await runNode("nodes/post_agent/08_build_duckdb_bundle.code.js", ibkr))[0].json;

  const summary = {
    decision: consensus.decision,
    consensusActions: consensus.agentDecision.actions.length,
    safetyDecision: safety.decision,
    orders: Array.isArray(safety.orders) ? safety.orders.length : 0,
    ibkrDryRun: ibkr.ibkr?.dryRun,
    bundleOrders: bundle.bundle.orders.length,
    bundleFills: bundle.bundle.fills.length,
    modelProposals: bundle.bundle.model_proposals.length,
    consensusVotes: bundle.bundle.consensus_votes.length,
    consensusDecisions: bundle.bundle.consensus_decisions.length,
    dbPath: bundle.db_path,
  };

  if (outPath) {
    fs.writeFileSync(outPath, JSON.stringify(bundle.bundle, null, 2));
  }
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err && err.stack ? err.stack : err);
  process.exit(1);
});
