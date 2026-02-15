// AG2-V2 - Build AI Validation Context (V2: strict contract + real H1 bars)

const item = $input.item.json;

const d1 = item.d1_indicators || {};
const h1 = item.h1_signal || {};
const h1Ind = item.h1_indicators || {};

const toNum = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const pickNum = (...vals) => {
  for (const v of vals) {
    const n = toNum(v);
    if (n !== null) return n;
  }
  return null;
};

// Canonical D1 fields
const d1_price  = pickNum(d1.last_close, item.d1_last_close);
const d1_sma200 = pickNum(d1.sma200, item.d1_sma200);
const d1_sma50  = pickNum(d1.sma50, item.d1_sma50);

// bias_sma200 + regime_d1 (deterministic)
let bias_sma200 = "UNKNOWN";
if (d1_price !== null && d1_sma200 !== null) bias_sma200 = (d1_price > d1_sma200) ? "BULLISH" : "BEARISH";

let regime_d1 = "UNKNOWN";
if (d1_price !== null && d1_sma200 !== null && d1_sma50 !== null) {
  if (d1_price > d1_sma200 && d1_sma50 > d1_sma200) regime_d1 = "BULLISH";
  else if (d1_price < d1_sma200 && d1_sma50 < d1_sma200) regime_d1 = "BEARISH";
  else {
    const pct = Math.abs(d1_price - d1_sma200) / Math.max(1e-9, Math.abs(d1_sma200));
    regime_d1 = (pct <= 0.01) ? "NEUTRAL_RANGE" : "TRANSITION";
  }
}

// Canonical H1 fields
const h1_entry = pickNum(h1Ind.last_close, item.h1_last_close, item.last_close);
const h1_atr   = pickNum(h1Ind.atr, item.h1_atr);
const h1_sup   = pickNum(h1Ind.support, item.h1_support);
const h1_res   = pickNum(h1Ind.resistance, item.h1_resistance);

// Stop suggestion (conservative: choose wider stop to avoid RR gaming)
let stop_loss_suggested = null;
let stop_loss_basis = "NONE";
let stop_meta = {};

if (h1_entry !== null && (h1.action === "BUY" || h1.action === "SELL")) {
  if (h1.action === "BUY") {
    const sl_atr = (h1_atr !== null) ? (h1_entry - Math.max(2 * h1_atr, h1_entry * 0.02)) : null;
    const sl_sup = (h1_sup !== null) ? (h1_sup * (1 - 0.002)) : null;

    if (sl_atr !== null || sl_sup !== null) {
      // Wider stop => smaller RR => more conservative approvals
      stop_loss_suggested = Math.min(
        sl_atr !== null ? sl_atr : Infinity,
        sl_sup !== null ? sl_sup : Infinity
      );
      if (!Number.isFinite(stop_loss_suggested)) stop_loss_suggested = (sl_atr !== null ? sl_atr : sl_sup);
      stop_loss_basis = (sl_atr !== null && sl_sup !== null) ? "ATR2X+SUPPORT" : (sl_atr !== null ? "ATR2X" : "SUPPORT");
      stop_meta = { sl_atr, sl_sup, entry: h1_entry };
    }
  }

  if (h1.action === "SELL") {
    const sl_atr = (h1_atr !== null) ? (h1_entry + Math.max(2 * h1_atr, h1_entry * 0.02)) : null;
    const sl_res = (h1_res !== null) ? (h1_res * (1 + 0.002)) : null;

    if (sl_atr !== null || sl_res !== null) {
      stop_loss_suggested = Math.max(
        sl_atr !== null ? sl_atr : -Infinity,
        sl_res !== null ? sl_res : -Infinity
      );
      if (!Number.isFinite(stop_loss_suggested)) stop_loss_suggested = (sl_atr !== null ? sl_atr : sl_res);
      stop_loss_basis = (sl_atr !== null && sl_res !== null) ? "ATR2X+RESIST" : (sl_atr !== null ? "ATR2X" : "RESIST");
      stop_meta = { sl_atr, sl_res, entry: h1_entry };
    }
  }
}

// RR theoretical (use stop_loss_suggested if available)
let rr_theoretical = null;
let rr_meta = {};

if (h1_entry !== null && h1_res !== null && h1.action === "BUY") {
  const tp = h1_res;
  const sl = (stop_loss_suggested !== null) ? stop_loss_suggested : null;
  if (sl !== null) {
    const tpDist = tp - h1_entry;
    const slDist = h1_entry - sl;
    rr_theoretical = (tpDist > 0 && slDist > 0) ? Math.round((tpDist / slDist) * 100) / 100 : null;
    rr_meta = { method: "resistance_vs_stop", entry: h1_entry, tp, sl, tp_dist: tpDist, sl_dist: slDist };
  }
}

// Real H1 bars (from compute node)
const bars = Array.isArray(item.h1_bars_60) ? item.h1_bars_60 : [];
const recentBars = bars.map(b => ({
  t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v
}));

return [{
  json: {
    ...item,
    ai_context: {
      schema_version: "ag2_ai_context_v2",
      symbol: item.symbol,
      run_context: { run_id: item.run_id, timestamp: new Date().toISOString() },

      // Canonical facts (the prompt will use THESE names)
      d1: {
        price: d1_price,
        sma200: d1_sma200,
        sma50: d1_sma50,
        bias_sma200,
        regime_d1,
        bars_count: item.d1_bars_count ?? null,
        indicators_raw: d1,
      },

      h1: {
        action: h1.action, score: h1.score, confidence: h1.confidence, rationale: h1.rationale,
        entry: h1_entry,
        atr: h1_atr,
        support: h1_sup,
        resistance: h1_res,
        indicators_raw: h1Ind,
        bars_count: item.h1_bars_count ?? recentBars.length,
      },

      bars_h1: recentBars,
      rr_theoretical,
      rr_meta,
      stop_loss_suggested,
      stop_loss_basis,
      stop_meta,
    }
  }
}];
