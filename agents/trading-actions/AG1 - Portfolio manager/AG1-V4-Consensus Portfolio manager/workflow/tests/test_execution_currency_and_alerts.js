#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");

async function runNode(relPath, json, inputItems = null) {
  const code = fs.readFileSync(path.join(root, relPath), "utf8");
  const input = { all: () => (inputItems || [{ json }]) };
  const fn = new Function("$json", "$input", "$env", `return (async () => {\n${code}\n})()`);
  return await fn(json, input, {});
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function proposal(modelKey, targetWeightPct) {
  return {
    modelKey,
    modelName: modelKey,
    modelId: modelKey,
    extractorStatus: "OK_OBJECT",
    output: {
      actions: [{
        symbol: "CRM",
        action: "OPEN",
        assetClass: "EQUITY",
        targetWeightPct,
        confidence: 70,
        rationale: "currency regression",
      }],
    },
  };
}

function context(runId) {
  const fx = 0.8657187;
  const nativePrice = 165.02;
  const priceEUR = nativePrice * fx;
  return {
    run: { runId, model: "ag1_v4_consensus", db_path: "/tmp/ag1-test.duckdb" },
    config: {
      min_order_value_eur: 1000,
      max_pos_pct: 25,
      max_sector_pct: 40,
      max_order_value_pct: 25,
      max_open_positions: 10,
      require_limit_buys: true,
    },
    meta: { initialCapitalEUR: 10000 },
    portfolioBrief: { cash: 10000, totalValue: 10000, positions: [] },
    opportunity_pack: {
      rows: [{
        symbol: "CRM",
        symbol_yahoo: "CRM",
        asset_class: "EQUITY",
        sector: "Technology",
        decision: "Entrer / Renforcer",
        gates: "OK",
        entry: nativePrice,
        stop: 155,
        tp: 190,
        spread_pct: 0.1,
        currency: "USD",
        fx_rate_to_eur: fx,
        price_eur: priceEUR,
        execution_constraints: {
          feasible: true,
          reason: "OK",
          minOrderValueEUR: 1000,
          minTargetWeightPct: 10.01,
          maxTargetWeightPct: 25,
        },
        liquidity: {
          status: "OK",
          contractResolved: true,
          currency: "USD",
          fxRateToEUR: fx,
          priceEUR,
        },
      }],
    },
  };
}

async function consensusFor(targetWeightPct, runId) {
  const ctx = context(runId);
  const items = [
    { json: ctx },
    { json: proposal("gpt-5.6-sol", targetWeightPct) },
    { json: proposal("deepseek-v4-pro", targetWeightPct) },
    { json: { ...proposal("claude-opus-4-8", null), output: { actions: [] } } },
  ];
  return (await runNode("nodes/post_agent/06_build_consensus_v4.code.js", {}, items))[0].json;
}

async function main() {
  const executable = await consensusFor(11, "RUN_CURRENCY_OK");
  assert(executable.decision === "TRADE", "EUR-normalized CRM consensus should trade");
  const action = executable.agentDecision.actions[0];
  assert(action.currency === "USD", "contract currency must propagate");
  assert(action.fxRateToEUR === 0.8657187, "FX rate must propagate");
  assert(action.targetQty === 7, `EUR sizing expected 7 final shares, got ${action.targetQty}`);
  assert(action.riskPlan.maxLossEUR > 0 && action.riskPlan.maxLossEUR < 100, "max loss must be EUR-normalized");

  const safety = (await runNode("nodes/post_agent/07_validate_enforce_safety_v5.code.js", executable))[0].json;
  assert(safety.orders.length === 1, "EUR-normalized order must pass safety");
  assert(safety.orders[0].estNotionalEUR >= 1000, "EUR minimum ticket must use normalized price");
  assert(safety.orders[0].estNotionalEUR < 1100, "native USD notional must not be mislabeled EUR");

  const belowMinimum = await consensusFor(8, "RUN_CURRENCY_MIN_BLOCK");
  assert(belowMinimum.decision === "NO_TRADE", "sub-minimum target must be removed before consensus");
  const rejectedVotes = belowMinimum.consensusVotes.filter((vote) => vote.intent === "BUY");
  assert(rejectedVotes.length === 2 && rejectedVotes.every((vote) => vote.executable === false), "sub-minimum votes must be non executable");
  assert(rejectedVotes.every((vote) => vote.feasibility.reason === "MIN_ORDER_VALUE_EUR"), "vote rejection must be structured");

  const alertInput = {
    run: { runId: "RUN_ALERT_CODES", model: "ag1_v4_consensus" },
    warnings: [
      "ORDER_REJECT:MIN_ORDER_VALUE_EUR:CRM:714<1000",
      "IBKR_ORDER_REJECTED:TKO.PA:error:Order price is not in increments of the minimum price variation",
    ],
    decision: "NO_TRADE",
    orders: [],
    portfolioSummary: { cashEUR: 10000, totalPortfolioValueEUR: 10000, positions: [] },
  };
  const bundle = (await runNode("nodes/post_agent/08_build_duckdb_bundle.code.js", alertInput))[0].json.bundle;
  assert(bundle.alerts[0].code === "MIN_ORDER_VALUE_EUR", "safety warning must become a reason code");
  assert(bundle.alerts[0].symbol === "CRM", "structured safety symbol must be exact");
  assert(bundle.alerts[1].code === "IBKR_MIN_PRICE_VARIATION", "IBKR tick rejection must have a specific code");
  assert(bundle.run.risk_gate_json.safety_reject_count === 1, "run risk gate counts must be persisted");

  console.log(JSON.stringify({
    crmTargetQty: action.targetQty,
    crmNotionalEUR: safety.orders[0].estNotionalEUR,
    belowMinimumVoteReason: rejectedVotes[0].feasibility.reason,
    alertCodes: bundle.alerts.map((alert) => alert.code),
  }, null, 2));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
});
