// AG2-V2 — Prepare structured text for Qdrant vectorization
const d = $input.item.json;
const symbol = d.symbol || '';
const run_id = d.run_id || '';
const signal_id = `${run_id}|${symbol}`;

const lines = [
  `[ENTITY] ${symbol}`,
  `[DATE] ${d.workflow_date || new Date().toISOString()}`,
  ``,
  `[DAILY CONTEXT]`,
  `Trend: ${d.ai_bias_sma200 || 'N/A'} | Regime: ${d.ai_regime_d1 || 'N/A'}`,
  `SMA200: ${d.d1_sma200 || 'N/A'} | SMA50: ${d.d1_sma50 || 'N/A'} | Close: ${d.d1_last_close || 'N/A'}`,
  `RSI14: ${d.d1_rsi14 || 'N/A'} | MACD_Hist: ${d.d1_macd_hist || 'N/A'} | ADX: ${d.d1_adx || 'N/A'}`,
  `Bollinger Width: ${d.d1_bb_width || 'N/A'} | Volatility: ${d.d1_volatility || 'N/A'}`,
  ``,
  `[H1 SIGNAL]`,
  `Action: ${d.h1_action || 'N/A'} | Score: ${d.h1_score || 'N/A'} | Confidence: ${d.h1_confidence || 'N/A'}`,
  `Rationale: ${d.h1_rationale || 'N/A'}`,
  `RSI14: ${d.h1_rsi14 || 'N/A'} | Stochastic: ${d.h1_stoch_k || 'N/A'}/${d.h1_stoch_d || 'N/A'}`,
  `Support: ${d.h1_support || 'N/A'} | Resistance: ${d.h1_resistance || 'N/A'}`,
  ``,
  `[AI VALIDATION]`,
  `Decision: ${d.ai_decision || 'N/A'} | Quality: ${d.ai_quality || 'N/A'}/10`,
  `Alignment: ${d.ai_alignment || 'N/A'} | Stop: ${d.ai_stop_loss || 'N/A'}`,
  `Reasoning: ${d.ai_reasoning || 'N/A'}`,
];

return [{
  json: {
    text: lines.join('\n'),
    metadata: {
      symbol, run_id, signal_id,
      h1_action: d.h1_action || '',
      d1_action: d.d1_action || '',
      ai_decision: d.ai_decision || '',
      ai_quality: d.ai_quality || 0,
      ai_alignment: d.ai_alignment || '',
      pass_pm: d.pass_pm || false,
      workflow_date: d.workflow_date || '',
    },
    id: signal_id.replace(/[^a-zA-Z0-9_-]/g, '_'),
  }
}];
