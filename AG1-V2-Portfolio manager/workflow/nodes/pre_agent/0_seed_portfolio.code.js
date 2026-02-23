// --- SEED PORTFOLIO : MIGRATION HISTORIQUE COMPLETE ---

const run_id = "RUN_MIGRATION_FULL_001";
const now_ts = new Date().toISOString();

// L'historique complet déduit de vos logs
const history = [
  // Initialisation (les achats d'origine avant le 17 janvier)
  { ts: "2026-01-15T12:00:00.000Z", sym: "AI.PA", side: "BUY", qty: 5, price: 170.00 },
  { ts: "2026-01-15T12:00:00.000Z", sym: "BNP.PA", side: "BUY", qty: 20, price: 58.00 },
  { ts: "2026-01-15T12:00:00.000Z", sym: "OR.PA", side: "BUY", qty: 3, price: 430.00 },
  { ts: "2026-01-15T12:00:00.000Z", sym: "MC.PA", side: "BUY", qty: 1, price: 700.00 },
  
  // Vos logs de transactions exacts
  { ts: "2026-01-17T06:25:44.184Z", sym: "VIE.PA", side: "BUY", qty: 67, price: 29.82 },
  { ts: "2026-01-17T10:32:54.801Z", sym: "OR.PA", side: "SELL", qty: 2, price: 384.35 },
  { ts: "2026-01-17T10:32:54.801Z", sym: "BNP.PA", side: "SELL", qty: 9, price: 86.82 },
  { ts: "2026-01-17T11:29:28.565Z", sym: "AMUN.PA", side: "BUY", qty: 13, price: 75.05 },
  { ts: "2026-01-17T11:29:28.565Z", sym: "ALO.PA", side: "BUY", qty: 19, price: 26.08 },
  { ts: "2026-01-19T21:06:03.771Z", sym: "BNP.PA", side: "BUY", qty: 12, price: 86.82 },
  { ts: "2026-01-19T21:06:03.771Z", sym: "AMUN.PA", side: "BUY", qty: 7, price: 75.05 },
  { ts: "2026-01-23T13:05:52.746Z", sym: "ELIOR.PA", side: "BUY", qty: 172, price: 2.89 },
  { ts: "2026-01-28T13:05:14.145Z", sym: "ACA.PA", side: "BUY", qty: 193, price: 18.20 },
  { ts: "2026-02-01T13:05:05.798Z", sym: "ACA.PA", side: "BUY", qty: 84, price: 18.21 },
  { ts: "2026-02-03T13:05:51.706Z", sym: "ACA.PA", side: "BUY", qty: 100, price: 18.82 },
  { ts: "2026-02-03T13:05:51.706Z", sym: "TTE.PA", side: "BUY", qty: 86, price: 58.53 },
  { ts: "2026-02-04T13:05:33.160Z", sym: "AMUN.PA", side: "BUY", qty: 18, price: 80.40 },
  { ts: "2026-02-05T13:06:41.075Z", sym: "ACA.PA", side: "BUY", qty: 71, price: 18.19 },
  { ts: "2026-02-05T13:06:41.075Z", sym: "TTE.PA", side: "BUY", qty: 43, price: 63.02 },
  { ts: "2026-02-08T13:05:50.704Z", sym: "AMUN.PA", side: "BUY", qty: 12, price: 80.50 },
  { ts: "2026-02-08T13:05:50.704Z", sym: "ACA.PA", side: "BUY", qty: 57, price: 18.19 },
  { ts: "2026-02-14T13:04:39.082Z", sym: "ACA.PA", side: "SELL", qty: 92, price: 17.78 },
  { ts: "2026-02-15T13:05:06.067Z", sym: "MC.PA", side: "SELL", qty: 1, price: 513.50 },
  { ts: "2026-02-15T13:05:06.067Z", sym: "ELIOR.PA", side: "SELL", qty: 172, price: 2.68 },
  { ts: "2026-02-15T13:05:06.067Z", sym: "ACA.PA", side: "SELL", qty: 128, price: 17.78 },
  { ts: "2026-02-15T13:05:06.067Z", sym: "AMUN.PA", side: "SELL", qty: 17, price: 75.85 }
];

const fills = [];
const cash_ledger = [];
const orders = [];

// 1. Dépôt Initial de 50 000 € au 1er Janvier
cash_ledger.push({
  cash_tx_id: "TX_SEED_DEPOSIT",
  run_id: run_id,
  ts: "2026-01-01T00:00:00.000Z",
  currency: "EUR",
  amount: 50000.0,
  type: "DEPOSIT",
  symbol: null,
  ref_id: null,
  notes: "Dépôt initial du capital"
});

// 2. Traitement de l'historique
let idx = 1;
for (const h of history) {
  const order_id = `ORD_HIST_${idx}`;
  const fill_id = `FIL_HIST_${idx}`;
  const notional = h.qty * h.price;
  
  // Si Achat -> impact cash négatif. Si Vente -> impact cash positif
  const cashImpact = h.side === "BUY" ? -notional : notional;

  orders.push({
    order_id: order_id,
    run_id: run_id,
    ts_created: h.ts,
    symbol: h.sym,
    side: h.side,
    intent: h.side === "BUY" ? "OPEN" : "CLOSE",
    order_type: "MARKET",
    qty: h.qty,
    status: "FILLED",
    broker: "MIGRATION",
    reason: "Importation historique complète"
  });

  fills.push({
    fill_id: fill_id,
    order_id: order_id,
    run_id: run_id,
    ts_fill: h.ts,
    qty: h.qty,
    price: h.price,
    fees_eur: 0,
    raw_fill_json: { mode: "MIGRATION" }
  });

  cash_ledger.push({
    cash_tx_id: `TX_HIST_${idx}`,
    run_id: run_id,
    ts: h.ts,
    currency: "EUR",
    amount: cashImpact,
    type: "TRADE_NOTIONAL",
    symbol: h.sym,
    ref_id: fill_id,
    notes: `${h.side} historique ${h.qty} ${h.sym} @ ${h.price}€`
  });

  idx++;
}

// 3. Derniers prix de marché pour le snapshot final
const market_prices = [
  { ts: now_ts, symbol: "TTE.PA", close: 65.51, source: "migration" },
  { ts: now_ts, symbol: "ACA.PA", close: 18.56, source: "migration" },
  { ts: now_ts, symbol: "AMUN.PA", close: 79.05, source: "migration" },
  { ts: now_ts, symbol: "VIE.PA", close: 34.48, source: "migration" },
  { ts: now_ts, symbol: "BNP.PA", close: 94.55, source: "migration" },
  { ts: now_ts, symbol: "ALO.PA", close: 29.95, source: "migration" },
  { ts: now_ts, symbol: "OR.PA", close: 398.65, source: "migration" },
  { ts: now_ts, symbol: "AI.PA", close: 174.78, source: "migration" }
];

// Construction du Bundle
const bundle = {
  run: {
    run_id: run_id,
    ts_start: now_ts,
    ts_end: now_ts,
    tz: "Europe/Paris",
    decision_summary: "MIGRATION_HISTORIQUE",
    model: "migration"
  },
  orders: orders,
  fills: fills,
  cash_ledger: cash_ledger,
  market_prices: market_prices
};

return [{
  json: {
    db_path: "/files/duckdb/ag1_v2.duckdb",
    bundle: bundle,
    summary: { commentary: "Importation historique complète" }
  }
}];
