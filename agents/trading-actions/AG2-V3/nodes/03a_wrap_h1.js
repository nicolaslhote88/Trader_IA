// AG2.11 - Wrap H1 response, preserving context from loop.
const resp = JSON.parse(JSON.stringify($json || {}));
const ctx = $("Loop Symbols").item.json || {};

return [
  {
    json: {
      symbol: ctx.symbol || "",
      symbol_internal: ctx.symbol_internal || ctx.symbol || "",
      symbol_yahoo: ctx.symbol_yahoo || resp.symbol || ctx.symbol || "",
      asset_class: ctx.asset_class || "EQUITY",
      base_ccy: ctx.base_ccy ?? null,
      quote_ccy: ctx.quote_ccy ?? null,
      pip_size: ctx.pip_size ?? null,
      price_decimals: ctx.price_decimals ?? null,
      trading_hours: ctx.trading_hours ?? null,
      run_id: ctx.run_id || "",
      strategy_version: ctx.strategy_version || "strategy_v3",
      config_version: ctx.config_version || "config_v3",
      prompt_version: ctx.prompt_version || "prompt_v3",
      enable_fx: !!ctx.enable_fx,
      universe_scope: ctx.universe_scope || ["EQUITY", "CRYPTO"],
      batch_info: ctx.batch_info || {},
      h1_response: resp,
    },
  },
];
