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
      exchange: ctx.exchange || "",
      currency: ctx.currency || "",
      run_id: ctx.run_id || "",
      strategy_version: ctx.strategy_version || "strategy_v3",
      config_version: ctx.config_version || "config_v3",
      prompt_version: ctx.prompt_version || "prompt_v3",
      n8n_execution_id: ctx.n8n_execution_id || "",
      closed_only: ctx.closed_only !== false,
      validated_only: ctx.validated_only !== false,
      universe_scope: ctx.universe_scope || ["EQUITY", "ETF", "CRYPTO"],
      batch_info: ctx.batch_info || {},
      d1_response: resp,
    },
  },
];
