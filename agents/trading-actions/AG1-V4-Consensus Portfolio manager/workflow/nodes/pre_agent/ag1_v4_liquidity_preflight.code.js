// AG1 V4 - Read-only liquidity and IBKR contract preflight.
// This node never calls an order endpoint.

const input = $json || {};
const ctx = this;
const brokerUrl = String($env.IBKR_BROKER_URL || "http://ibkr-broker:8080").replace(/\/+$/, "");
const yfUrl = String($env.YFINANCE_API_URL || "http://yfinance-api:8080").replace(/\/+$/, "");
const minVolume = Number($env.AG1_LIQUIDITY_MIN_DAILY_VOLUME || 5000);
const maxOrderVolumePct = Number($env.AG1_LIQUIDITY_MAX_ORDER_TO_VOLUME_PCT || 1);
const maxSpreadPct = Number(input.config?.max_spread_pct ?? 1.5);
const maxEntryQuoteDeviationPct = Number($env.AG1_LIQUIDITY_MAX_ENTRY_QUOTE_DEVIATION_PCT || 3);
const maxQuoteAgeSeconds = Number($env.IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS || 28800);
const defaultWeightPct = Math.min(5, Number(input.config?.max_pos_pct ?? 25));

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }
function num(v) { const n = Number(v); return Number.isFinite(n) ? n : null; }
function gateList(value) {
  return String(value || "").split("|").map((x) => x.trim()).filter((x) => x && x !== "OK");
}
function parseTime(v) {
  const ms = Date.parse(String(v || ""));
  return Number.isFinite(ms) ? ms : null;
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function getJson(url) {
  return await ctx.helpers.httpRequest({
    method: "GET",
    url,
    headers: { Accept: "application/json" },
    json: true,
    timeout: 15000,
  });
}
function snapshotHasPrice(row) {
  return num(row?.["31"] ?? row?.lastPrice ?? row?.price) !== null;
}
function mergeSnapshotRows(batches) {
  const out = new Map();
  for (const batch of batches) {
    for (const row of Array.isArray(batch) ? batch : []) {
      const conid = row?.conid ?? row?.conidEx;
      if (conid === null || conid === undefined) continue;
      const key = String(conid);
      const current = out.get(key) || {};
      const merged = { ...current, ...row };
      if (snapshotHasPrice(row) || !snapshotHasPrice(current)) out.set(key, merged);
      else out.set(key, { ...row, ...current });
    }
  }
  return Array.from(out.values());
}
async function getIbkrSnapshots(conids) {
  const batches = [];
  const fields = "31,84,86,85,88,55,6509";
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (attempt > 0) await sleep(750);
    const response = await getJson(`${brokerUrl}/marketdata/snapshot?conids=${encodeURIComponent(conids.join(","))}&fields=${fields}`);
    batches.push(Array.isArray(response) ? response : []);
    const merged = mergeSnapshotRows(batches);
    const priced = merged.filter(snapshotHasPrice).length;
    if (priced >= conids.length) return { snapshots: merged, attempts: attempt + 1 };
  }
  return { snapshots: mergeSnapshotRows(batches), attempts: batches.length };
}

const pack = isObj(input.opportunity_pack) ? { ...input.opportunity_pack } : { rows: [] };
const rows = Array.isArray(pack.rows) ? pack.rows.map((row) => ({ ...row })) : [];
const candidates = rows.filter((row) => ["Entrer / Renforcer", "Surveiller", "Reduire / Sortir"].includes(String(row.decision || "")));
const yahooSymbols = candidates.map((row) => normSymbol(row.symbol_yahoo || row.symbol)).filter(Boolean);
const internalSymbols = candidates.map((row) => normSymbol(row.symbol)).filter(Boolean);

let quotes = [];
let resolution = { results: [], errors: [] };
let ibkrSnapshots = [];
let ibkrSnapshotAttempts = 0;
const preflightWarnings = [];

if (yahooSymbols.length) {
  try {
    const response = await getJson(`${yfUrl}/quote?symbols=${encodeURIComponent(yahooSymbols.join(","))}&side=BUY`);
    quotes = Array.isArray(response?.quotes) ? response.quotes : [];
  } catch (err) {
    preflightWarnings.push(`YFINANCE_QUOTE_UNAVAILABLE:${err?.message || err}`);
  }
  try {
    resolution = await getJson(`${brokerUrl}/contracts/equity/resolve?symbols=${encodeURIComponent(internalSymbols.join(","))}`);
  } catch (err) {
    preflightWarnings.push(`IBKR_CONTRACT_PREFLIGHT_UNAVAILABLE:${err?.message || err}`);
  }
  const conids = (resolution.results || []).map((row) => row.conid).filter((x) => x !== null && x !== undefined);
  if (conids.length) {
    try {
      const response = await getIbkrSnapshots(conids);
      ibkrSnapshots = response.snapshots;
      ibkrSnapshotAttempts = response.attempts;
    } catch (err) {
      preflightWarnings.push(`IBKR_SNAPSHOT_UNAVAILABLE:${err?.message || err}`);
    }
  }
}

const quoteMap = new Map(quotes.map((q) => [normSymbol(q.symbol || q.resolvedSymbol), q]));
const conidMap = new Map((resolution.results || []).map((r) => [normSymbol(r.symbol), r.conid]));
const unresolved = new Set((resolution.errors || []).map((r) => normSymbol(r.symbol)));
const snapshotMap = new Map(ibkrSnapshots.map((r) => [String(r.conid), r]));
const totalValue = num(input.portfolio_pack?.totalValueEUR ?? input.portfolioBrief?.totalValue) || 0;
const now = Date.now();

for (const row of rows) {
  if (!["Entrer / Renforcer", "Surveiller", "Reduire / Sortir"].includes(String(row.decision || ""))) continue;
  const symbol = normSymbol(row.symbol);
  const yahoo = normSymbol(row.symbol_yahoo || row.symbol);
  const quote = quoteMap.get(yahoo) || quoteMap.get(symbol) || {};
  const conid = conidMap.get(symbol) ?? null;
  const snapshot = conid !== null ? (snapshotMap.get(String(conid)) || {}) : {};
  const ibkrPrice = num(snapshot["31"] ?? snapshot.lastPrice ?? snapshot.price);
  const ibkrBid = num(snapshot["84"] ?? snapshot.bid);
  const ibkrAsk = num(snapshot["86"] ?? snapshot.ask);
  const ibkrHasFreshPrice = ibkrPrice !== null && ibkrPrice > 0;
  const yahooPrice = num(quote.regularMarketPrice);
  const price = num((ibkrHasFreshPrice ? ibkrPrice : yahooPrice) ?? row.entry);
  const volume = num(quote.volume ?? row.volume);
  const spreadFromIbkr = ibkrBid !== null && ibkrAsk !== null && ibkrBid > 0 && ibkrAsk > 0
    ? ((ibkrAsk - ibkrBid) / ((ibkrAsk + ibkrBid) / 2)) * 100
    : null;
  const spreadPct = num((ibkrHasFreshPrice ? spreadFromIbkr : null) ?? quote.spreadPct ?? row.spread_pct);
  const quoteTime = ibkrHasFreshPrice
    ? new Date().toISOString()
    : (quote.regularMarketTime || row.regular_market_time || quote.fetchedAt || row.quote_fetched_at || null);
  const quoteAgeMinutes = parseTime(quoteTime) === null ? null : Math.max(0, (now - parseTime(quoteTime)) / 60000);
  const targetQty = totalValue > 0 && price > 0 ? Math.floor((totalValue * defaultWeightPct / 100) / price) : null;
  const orderVolumePct = targetQty !== null && volume > 0 ? targetQty / volume * 100 : null;
  const entry = num(row.entry);
  const priceDivergencePct = entry && price ? Math.abs(price - entry) / entry * 100 : null;
  const gates = new Set(gateList(row.gates));

  if (unresolved.has(symbol) || conid === null) gates.add("IBKR_CONTRACT_UNRESOLVED");
  if (volume === null || spreadPct === null || quoteAgeMinutes === null) gates.add("LIQUIDITY_UNKNOWN");
  if (volume !== null && volume < minVolume) gates.add("LIQUIDITY_STRESS");
  if (spreadPct !== null && spreadPct > maxSpreadPct) gates.add("LIQUIDITY_STRESS");
  if (orderVolumePct !== null && orderVolumePct > maxOrderVolumePct) gates.add("LIQUIDITY_STRESS");
  if (priceDivergencePct !== null && priceDivergencePct > maxEntryQuoteDeviationPct) gates.add("PRICE_DIVERGENCE");
  if (quoteAgeMinutes === null || quoteAgeMinutes > maxQuoteAgeSeconds / 60) gates.add("STALE_QUOTE");

  if (price !== null && price > 0) {
    row.matrix_entry = row.matrix_entry ?? row.entry;
    row.entry = Math.round(price * 10000) / 10000;
  }
  row.quote_source = ibkrHasFreshPrice ? "ibkr_cpapi_snapshot" : (quote.source || row.quote_source || null);
  row.quote_fetched_at = quoteTime;
  row.regular_market_time = quoteTime;
  row.market_state = ibkrHasFreshPrice ? "IBKR_SNAPSHOT" : (quote.marketState || row.market_state || null);

  row.gates = gates.size ? Array.from(gates).sort().join("|") : "OK";
  row.spread_pct = spreadPct;
  row.volume = volume;
  row.liquidity = {
    status: gates.has("LIQUIDITY_STRESS") ? "STRESS" : (gates.has("LIQUIDITY_UNKNOWN") ? "UNKNOWN" : "OK"),
    conid,
    contractResolved: conid !== null,
    quoteSource: row.quote_source,
    quoteTime,
    quoteAgeMinutes: quoteAgeMinutes === null ? null : Math.round(quoteAgeMinutes * 10) / 10,
    marketState: row.market_state,
    price,
    bid: ibkrHasFreshPrice ? ibkrBid : num(quote.bid),
    ask: ibkrHasFreshPrice ? ibkrAsk : num(quote.ask),
    spreadPct,
    volume,
    estimatedTargetWeightPct: defaultWeightPct,
    estimatedTargetQty: targetQty,
    estimatedOrderToVolumePct: orderVolumePct === null ? null : Math.round(orderVolumePct * 10000) / 10000,
    entryQuoteDivergencePct: priceDivergencePct === null ? null : Math.round(priceDivergencePct * 10000) / 10000,
    originalMatrixEntry: entry,
    ibkrSnapshotPrice: ibkrPrice,
    ibkrSnapshotAttempts,
    readOnlyChecks: ["yfinance_quote", "ibkr_contract_resolution", "ibkr_market_snapshot"],
  };
}

pack.rows = rows;
pack.liquidityPreflight = {
  generatedAt: new Date().toISOString(),
  candidateCount: candidates.length,
  contractResolved: conidMap.size,
  quoteCount: quotes.length,
  ibkrSnapshotCount: ibkrSnapshots.length,
  ibkrSnapshotPricedCount: ibkrSnapshots.filter(snapshotHasPrice).length,
  ibkrSnapshotAttempts,
  warnings: preflightWarnings,
  orderEndpointsCalled: false,
};

return [{ json: { ...input, opportunity_pack: pack } }];
