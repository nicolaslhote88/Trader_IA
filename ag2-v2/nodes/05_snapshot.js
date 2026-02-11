// AG2-V2 — Build AI Validation Context
const item = $input.item.json;

const d1 = item.d1_indicators || {};
const h1 = item.h1_signal || {};
const h1Ind = item.h1_indicators || {};

// D1 trend verdict
const price = d1.last_close || 0;
const sma200 = d1.sma200 || 0;
const trendVerdict = (price > sma200) ? 'BULLISH' : 'BEARISH';

// RR Theoretical
let rr = null;
let rrMeta = {};
if (h1.action === 'BUY' && h1Ind.resistance && h1Ind.atr) {
  const entry = h1Ind.last_close || 0;
  const tp = h1Ind.resistance;
  const slDist = Math.max(h1Ind.atr * 2, entry * 0.02);
  const sl = entry - slDist;
  const tpDist = tp - entry;
  rr = tpDist > 0 && slDist > 0 ? Math.round(tpDist / slDist * 100) / 100 : null;
  rrMeta = { method: 'pivot_resistance', entry, tp, sl, tp_dist: tpDist, sl_dist: slDist, atr: h1Ind.atr };
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
      bars: recentBars,
      rr_theoretical: rr,
      rr_meta: rrMeta,
      run_context: { run_id: item.run_id, timestamp: new Date().toISOString() },
    }
  }
}];
