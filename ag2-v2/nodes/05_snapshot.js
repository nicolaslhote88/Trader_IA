// AG2-V2 — Build AI Validation Context
// P1.3: Always provide entry_plan with ATR-based SL/TP fallback
const item = $input.item.json;

const d1 = item.d1_indicators || {};
const h1 = item.h1_signal || {};
const h1Ind = item.h1_indicators || {};

// D1 trend verdict
const price = d1.last_close || 0;
const sma200 = d1.sma200 || 0;
const trendVerdict = (price > sma200) ? 'BULLISH' : 'BEARISH';

// ── Entry Plan: always provide SL/TP/RR (ATR-based fallback) ──
const entry = h1Ind.last_close || 0;
const atr = h1Ind.atr || 0;
const resistance = h1Ind.resistance || 0;
const support = h1Ind.support || 0;

let rr = null;
let rrMeta = {};
let suggestedSl = null;
let suggestedTp = null;

if (entry > 0) {
  // Stop Loss: prefer swing low (support), fallback to ATR-based
  const slDistAtr = atr > 0 ? Math.max(atr * 2, entry * 0.015) : entry * 0.02;
  const slDistSwing = support > 0 ? Math.max(entry - support, entry * 0.005) : 0;

  // Use swing if it's reasonable (not too far, not too tight)
  let slDist;
  let slMethod;
  if (slDistSwing > 0 && slDistSwing < entry * 0.05 && slDistSwing >= entry * 0.005) {
    slDist = slDistSwing + entry * 0.002; // swing + small buffer
    slMethod = 'SWING_H1';
  } else {
    slDist = slDistAtr;
    slMethod = 'ATR_BASED';
  }

  suggestedSl = Math.round((entry - slDist) * 100) / 100;

  // Take Profit: prefer resistance, fallback to ATR-based
  let tpDist;
  let tpMethod;
  if (h1.action === 'BUY') {
    if (resistance > entry) {
      tpDist = resistance - entry;
      tpMethod = 'RESISTANCE';
    } else {
      tpDist = atr > 0 ? atr * 3 : entry * 0.03;
      tpMethod = 'ATR_BASED';
    }
    suggestedTp = Math.round((entry + tpDist) * 100) / 100;
  } else if (h1.action === 'SELL') {
    if (support > 0 && support < entry) {
      tpDist = entry - support;
      tpMethod = 'SUPPORT';
    } else {
      tpDist = atr > 0 ? atr * 3 : entry * 0.03;
      tpMethod = 'ATR_BASED';
    }
    suggestedTp = Math.round((entry - tpDist) * 100) / 100;
  } else {
    tpDist = 0;
    tpMethod = 'NONE';
  }

  if (tpDist > 0 && slDist > 0) {
    rr = Math.round(tpDist / slDist * 100) / 100;
  }

  rrMeta = {
    method: tpMethod,
    sl_method: slMethod,
    entry,
    tp: suggestedTp,
    sl: suggestedSl,
    tp_dist: Math.round(tpDist * 100) / 100,
    sl_dist: Math.round(slDist * 100) / 100,
    atr: atr || null,
    resistance: resistance || null,
    support: support || null,
  };
}

// Last 60 H1 candles
const h1Bars = (item.h1_response || {}).bars || [];
const recentBars = h1Bars.slice(-60).map(b => ({
  t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v
}));

return [{
  json: {
    ...item,
    ai_context: {
      symbol: item.symbol,
      signal_tactical_H1: {
        action: h1.action, score: h1.score,
        confidence: h1.confidence, rationale: h1.rationale,
        indicators: h1Ind,
      },
      primary_context_D1: {
        trend_verdict: trendVerdict, indicators: d1,
      },
      entry_plan: {
        entry,
        suggested_sl: suggestedSl,
        suggested_tp: suggestedTp,
        sl_method: rrMeta.sl_method || 'NONE',
        tp_method: rrMeta.method || 'NONE',
      },
      bars: recentBars,
      rr_theoretical: rr,
      rr_meta: rrMeta,
      run_context: { run_id: item.run_id, timestamp: new Date().toISOString() },
    }
  }
}];
