// AG2.11 — Wrap H1 response, preserving context from Loop
const resp = JSON.parse(JSON.stringify($json || {}));
const ctx = $('Loop Symbols').item.json;

return [{
  json: {
    symbol: resp.symbol || ctx.symbol || '',
    run_id: ctx.run_id || '',
    batch_info: ctx.batch_info || {},
    h1_response: resp,
  }
}];
