#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const files = [
  "nodes/pre_agent/ag1_v4_liquidity_preflight.code.js",
  "nodes/post_agent/06_build_consensus_v4.code.js",
  "nodes/post_agent/07_validate_enforce_safety_v5.code.js",
  "nodes/post_agent/07b_ibkr_send_orders.js",
  "nodes/post_agent/08_build_duckdb_bundle.code.js",
];

for (const relative of files) {
  const source = fs.readFileSync(path.join(root, relative), "utf8");
  new Function("$json", "$input", "$env", `return (async function () {\n${source}\n}).call(this)`);
  console.log(`OK ${relative}`);
}
