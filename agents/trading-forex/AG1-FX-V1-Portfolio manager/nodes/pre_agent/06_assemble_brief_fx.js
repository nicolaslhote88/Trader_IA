const j = $json;
const cfg = j.config || {};
const brief = {
  run: {
    run_id: j.run_id,
    as_of: j.as_of,
    llm_model: j.llm_model,
  },
  config: {
    capital_eur: Number(cfg.initial_capital_eur || 10000),
    leverage_max: Number(cfg.leverage_max || 1),
    kill_switch_active: Boolean(cfg.kill_switch_active),
  },
  portfolio_state: j.portfolio_state || {},
  universe: {
    pairs: (j.universe_fx || []).map((x) => x.pair),
    metadata: j.universe_fx || [],
  },
  technical_signals: j.technical_signals || [],
  macro_news: j.macro_news || { top_news: [], pair_focus: {}, macro_regime: {} },
  limits: {
    max_pair_pct: Number(cfg.max_pair_pct || cfg.max_pos_pct || 0.20),
    max_currency_exposure_pct: Number(cfg.max_currency_exposure_pct || 0.50),
    max_daily_drawdown_pct: Number(cfg.max_daily_drawdown_pct || 0.05),
  },
};

return [{
  json: {
    ...j,
    brief,
    system_prompt_vars: {
      llm_model: j.llm_model,
      leverage_max: brief.config.leverage_max,
    },
    user_prompt: `Use this AG1-FX-V1 briefing JSON:\n\n\`\`\`json\n${JSON.stringify(brief, null, 2)}\n\`\`\``,
  },
}];
