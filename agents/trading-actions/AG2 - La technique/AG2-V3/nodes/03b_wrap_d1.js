// AG2.16 - Wrap D1 response, preserving context from loop.
const resp = JSON.parse(JSON.stringify($json || {}));
const ctx = $("Loop Symbols").item.json || {};

return [
  {
    json: {
      symbol: ctx.symbol || "",
      symbol_internal: ctx.symbol_internal || ctx.symbol || "",
      symbol_yahoo: ctx.symbol_yahoo || resp.symbol || ctx.symbol || "",
      asset_class: ctx.asset_class || "EQUITY",
      run_id: ctx.run_id || "",
      strategy_version: ctx.strategy_version || "strategy_v3",
      config_version: ctx.config_version || "config_v3",
      prompt_version: ctx.prompt_version || "prompt_v3",
      universe_scope: ctx.universe_scope || ["EQUITY", "ETF", "CRYPTO"],
      batch_info: ctx.batch_info || {},
      d1_response: resp,
    },
  },
];
