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
const minOrderValueEUR = Number(input.config?.min_order_value_eur ?? input.config?.minOrderValueEUR ?? 1000);
const maxPositionPct = Number(input.config?.max_pos_pct ?? input.config?.maxPositionPct ?? 25);
// When the instantaneous bid/ask is unavailable (e.g. Euronext outside RTH, or a
// momentary quoting gap) but the name is demonstrably liquid, treat the spread as
// "unquoted" rather than "unknown" so a LIMIT entry is not hard-rejected. Reversible.
const allowUnquotedSpread = String($env.AG1_LIQUIDITY_ALLOW_UNQUOTED_SPREAD ?? "true").trim().toLowerCase() !== "false";

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }
function normSymbol(v) { return String(v ?? "").trim().toUpperCase(); }
function num(v) { if (v === null || v === undefined || v === "") return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function positive(v) { const n = num(v); return n !== null && n > 0 ? n : null; }
function round4(v) { return v === null ? null : Math.round(v * 10000) / 10000; }
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
function snapshotHasBidAsk(row) {
  const b = num(row?.["84"] ?? row?.bid);
  const a = num(row?.["86"] ?? row?.ask);
  return b !== null && a !== null && b > 0 && a > 0;
}
function historyBarPrice(row) {
  return num(row?.c ?? row?.close ?? row?.o ?? row?.open);
}
function historyBarTime(row) {
  const t = num(row?.t ?? row?.time ?? row?.timestamp);
  if (t !== null) return new Date(t).toISOString();
  return row?.date || row?.datetime || row?.ts || null;
}
// Field-by-field merge across polls: IBKR snapshots stream incrementally, so
// the last price (31) often arrives one poll before bid/ask (84/86). Keep the
// most complete value seen for every field instead of choosing one row.
function mergeSnapshotRows(batches) {
  const out = new Map();
  for (const batch of batches) {
    for (const row of Array.isArray(batch) ? batch : []) {
      const conid = row?.conid ?? row?.conidEx;
      if (conid === null || conid === undefined) continue;
      const key = String(conid);
      const merged = { ...(out.get(key) || {}) };
      for (const k of Object.keys(row)) {
        const v = row[k];
        if (v !== null && v !== undefined && v !== "") merged[k] = v;
      }
      out.set(key, merged);
    }
  }
  return Array.from(out.values());
}
async function getIbkrSnapshots(conids) {
  const fields = "31,84,86,85,88,55,6509";
  // Robust warm-up: IBKR streams snapshots incrementally and a large batch can
  // take several polls to populate bid/ask for every name. Process in sub-batches
  // so each chunk warms up reliably, and poll each chunk up to maxAttempts.
  const maxAttempts = Math.max(2, Number($env.AG1_LIQUIDITY_SNAPSHOT_MAX_ATTEMPTS || 8));
  const chunkSize = Math.max(5, Number($env.AG1_LIQUIDITY_SNAPSHOT_CHUNK || 20));
  const all = new Map();
  let totalAttempts = 0;
  for (let i = 0; i < conids.length; i += chunkSize) {
    const chunk = conids.slice(i, i + chunkSize);
    const batches = [];
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      if (attempt > 0) await sleep(750);
      const response = await getJson(`${brokerUrl}/marketdata/snapshot?conids=${encodeURIComponent(chunk.join(","))}&fields=${fields}`);
      batches.push(Array.isArray(response) ? response : []);
      totalAttempts += 1;
      const merged = mergeSnapshotRows(batches);
      const quoted = merged.filter(snapshotHasBidAsk).length;
      const last = attempt === maxAttempts - 1;
      if (quoted >= chunk.length || last) {
        for (const r of merged) all.set(String(r.conid ?? r.conidEx), r);
        break;
      }
    }
  }
  return { snapshots: Array.from(all.values()), attempts: totalAttempts };
}
async function getIbkrHistoryPrice(conid) {
  const plans = [
    { period: "2d", bar: "1h" },
    { period: "1w", bar: "1d" },
  ];
  for (const plan of plans) {
    const url = `${brokerUrl}/marketdata/history?conid=${encodeURIComponent(String(conid))}`
      + `&period=${encodeURIComponent(plan.period)}&bar=${encodeURIComponent(plan.bar)}&outside_rth=true`;
    const response = await getJson(url);
    const bars = Array.isArray(response?.data) ? response.data : [];
    for (let i = bars.length - 1; i >= 0; i -= 1) {
      const bar = bars[i] || {};
      const price = historyBarPrice(bar);
      const time = historyBarTime(bar);
      if (price !== null && price > 0 && time) {
        return { conid, price, time, bar, period: plan.period, barSize: plan.bar };
      }
    }
  }
  return null;
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
const ibkrHistoryMap = new Map();
let ibkrHistoryAttempts = 0;
const preflightWarnings = [];
const fxRateToEUR = new Map([["EUR", 1]]);
const fxProvenance = new Map([["EUR", {source:"identity", asOf:null}]]);
let accountLedgerLoaded = false;

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
  try {
    const ledger = await getJson(`${brokerUrl}/account/ledger`);
    for (const [key, value] of Object.entries(isObj(ledger) ? ledger : {})) {
      const currency = normSymbol(value?.currency || key);
      const rate = positive(value?.exchangerate);
      if (currency && currency !== "BASE" && rate !== null) {
        fxRateToEUR.set(currency, rate);
        fxProvenance.set(currency, {source:"ibkr_ledger", observedAt:new Date().toISOString()});
      }
    }
    accountLedgerLoaded = true;
  } catch (err) {
    preflightWarnings.push(`IBKR_ACCOUNT_LEDGER_UNAVAILABLE:${err?.message || err}`);
  }

  const missingCurrencies = Array.from(new Set(
    (resolution.results || [])
      .map((row) => normSymbol(row.currency))
      .filter((currency) => currency && currency !== "EUR" && !fxRateToEUR.has(currency))
  ));
  if (missingCurrencies.length) {
    try {
      const pairs = missingCurrencies.map((currency) => `${currency}EUR`);
      const response = await getJson(`${brokerUrl}/marketdata/fx/snapshot?pairs=${encodeURIComponent(pairs.join(","))}`);
      for (const quote of (response?.quotes || [])) {
        const pair = normSymbol(quote?.pair);
        if (!pair.endsWith("EUR") || pair.length !== 6) continue;
        const rate = positive(quote?.mid ?? quote?.last);
        if (rate !== null) {
          fxRateToEUR.set(pair.slice(0, 3), rate);
          fxProvenance.set(pair.slice(0, 3), {source:"ibkr_fx_snapshot", observedAt:new Date().toISOString()});
        }
      }
      for (const error of (response?.errors || [])) {
        preflightWarnings.push(`IBKR_FX_RATE_UNAVAILABLE:${error?.pair || "UNKNOWN"}:${error?.error || "UNKNOWN"}`);
      }
    } catch (err) {
      preflightWarnings.push(`IBKR_FX_PREFLIGHT_UNAVAILABLE:${err?.message || err}`);
    }
  }
  // CPAPI does not expose every non-G10 cross. A dated Yahoo cross is a
  // valuation fallback only; it never submits a currency order.
  const unresolvedFx = missingCurrencies.filter((ccy) => !fxRateToEUR.has(ccy));
  if (unresolvedFx.length) {
    try {
      const response = await getJson(`${yfUrl}/quote?symbols=${encodeURIComponent(unresolvedFx.map((ccy) => `EUR${ccy}=X`).join(","))}`);
      for (const quote of response.quotes || []) {
        const match = /^EUR([A-Z]{3})=X$/.exec(normSymbol(quote.symbol || quote.resolvedSymbol));
        const rate = positive(quote.regularMarketPrice);
        const asOf = parseTime(quote.regularMarketTime);
        if (match && rate && asOf !== null && Date.now() - asOf >= 0 && Date.now() - asOf <= maxQuoteAgeSeconds * 1000) {
          fxRateToEUR.set(match[1], 1 / rate);
          fxProvenance.set(match[1], {source:"yfinance_dated_EUR_cross", asOf:quote.regularMarketTime, symbol:quote.symbol});
        }
      }
    } catch (err) { preflightWarnings.push(`YFINANCE_FX_UNAVAILABLE:${err?.message || err}`); }
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
    const pricedConids = new Set(
      ibkrSnapshots
        .filter(snapshotHasPrice)
        .map((row) => String(row.conid ?? row.conidEx))
    );
    const missingConids = conids.filter((conid) => !pricedConids.has(String(conid)));
    for (const conid of missingConids) {
      try {
        ibkrHistoryAttempts += 1;
        const history = await getIbkrHistoryPrice(conid);
        if (history) ibkrHistoryMap.set(String(conid), history);
      } catch (err) {
        preflightWarnings.push(`IBKR_HISTORY_UNAVAILABLE:${conid}:${err?.message || err}`);
      }
    }
  }
}

const quoteMap = new Map(quotes.map((q) => [normSymbol(q.symbol || q.resolvedSymbol), q]));
const resolutionMap = new Map((resolution.results || []).map((r) => [normSymbol(r.symbol), r]));
const conidMap = new Map((resolution.results || []).map((r) => [normSymbol(r.symbol), r.conid]));
const unresolved = new Set((resolution.errors || []).map((r) => normSymbol(r.symbol)));
const snapshotMap = new Map(ibkrSnapshots.map((r) => [String(r.conid), r]));
const totalValue = num(input.portfolio_pack?.totalValueEUR ?? input.portfolioBrief?.totalValue) || 0;
const cashEUR = num(input.portfolio_pack?.cashEUR ?? input.portfolioBrief?.cash) || 0;
const positionMap = new Map(
  (Array.isArray(input.portfolio_pack?.positions) ? input.portfolio_pack.positions : [])
    .map((position) => [normSymbol(position?.symbol || position?.Symbol), position])
    .filter(([symbol]) => Boolean(symbol))
);
const now = Date.now();

for (const row of rows) {
  if (!["Entrer / Renforcer", "Surveiller", "Reduire / Sortir"].includes(String(row.decision || ""))) continue;
  const symbol = normSymbol(row.symbol);
  const yahoo = normSymbol(row.symbol_yahoo || row.symbol);
  const quote = quoteMap.get(yahoo) || quoteMap.get(symbol) || {};
  const contract = resolutionMap.get(symbol) || {};
  const conid = contract.conid ?? null;
  const currency = normSymbol(contract.currency || row.currency || "EUR") || "EUR";
  const fxRate = currency === "EUR" ? 1 : positive(fxRateToEUR.get(currency));
  const snapshot = conid !== null ? (snapshotMap.get(String(conid)) || {}) : {};
  const ibkrPrice = num(snapshot["31"] ?? snapshot.lastPrice ?? snapshot.price);
  const ibkrBid = num(snapshot["84"] ?? snapshot.bid);
  const ibkrAsk = num(snapshot["86"] ?? snapshot.ask);
  const availability = String(snapshot["6509"] || "");
  const frozenSnapshot = ["Z", "Y"].includes(availability[0]);
  const ibkrHasFreshPrice = ibkrPrice !== null && ibkrPrice > 0 && !frozenSnapshot;
  const ibkrHistory = conid !== null ? (ibkrHistoryMap.get(String(conid)) || null) : null;
  const ibkrHistoryPrice = num(ibkrHistory?.price);
  const ibkrHasHistoryPrice = ibkrHistoryPrice !== null && ibkrHistoryPrice > 0;
  const yahooPrice = num(quote.regularMarketPrice);
  const price = num((ibkrHasFreshPrice ? ibkrPrice : (ibkrHasHistoryPrice ? ibkrHistoryPrice : yahooPrice)) ?? row.entry);
  const priceEUR = price !== null && price > 0 && fxRate !== null ? price * fxRate : null;
  const volume = num(quote.volume ?? row.volume);
  const spreadFromIbkr = ibkrBid !== null && ibkrAsk !== null && ibkrBid > 0 && ibkrAsk > 0
    ? ((ibkrAsk - ibkrBid) / ((ibkrAsk + ibkrBid) / 2)) * 100
    : null;
  const spreadPct = num((ibkrHasFreshPrice ? spreadFromIbkr : null) ?? quote.spreadPct ?? row.spread_pct);
  const snapshotUpdated = positive(snapshot._updated);
  const snapshotAsOf = snapshotUpdated !== null
    ? new Date(snapshotUpdated - (availability.startsWith("D") ? 20 * 60000 : 0)).toISOString() : null;
  const quoteTime = ibkrHasFreshPrice
    ? (snapshotAsOf || quote.regularMarketTime || null)
    : (ibkrHasHistoryPrice
      ? ibkrHistory.time
      : (quote.regularMarketTime || row.regular_market_time || quote.fetchedAt || row.quote_fetched_at || null));
  const quoteAgeMinutes = parseTime(quoteTime) === null || parseTime(quoteTime) > now + 60000 ? null : Math.max(0, (now - parseTime(quoteTime)) / 60000);
  const targetQty = totalValue > 0 && priceEUR > 0 ? Math.floor((totalValue * defaultWeightPct / 100) / priceEUR) : null;
  const orderVolumePct = targetQty !== null && volume > 0 ? targetQty / volume * 100 : null;
  const entry = num(row.entry);
  const priceDivergencePct = entry && price ? Math.abs(price - entry) / entry * 100 : null;
  // The preflight is the AUTHORITATIVE liquidity gate (fresh IBKR data). Discard
  // the matrix's stale liquidity verdict (built from yfinance, unreliable for US)
  // and recompute it below; non-liquidity gates from the matrix are preserved.
  const STALE_LIQ = new Set(["LIQUIDITY_UNKNOWN", "LIQUIDITY_STRESS", "SPREAD_UNQUOTED", "STALE_QUOTE", "PRICE_DIVERGENCE", "IBKR_CONTRACT_UNRESOLVED"]);
  const gates = new Set(gateList(row.gates).filter((g) => !STALE_LIQ.has(g)));

  if (unresolved.has(symbol) || conid === null) gates.add("IBKR_CONTRACT_UNRESOLVED");
  if (currency !== "EUR" && fxRate === null) gates.add("FX_RATE_UNAVAILABLE");

  if (contract.rules_error) gates.add("IBKR_CONTRACT_RULES_UNAVAILABLE");
  if (contract.account_trade_allowed === false) gates.add("IBKR_TRADING_PERMISSION_MISSING");
  const currentPosition = positionMap.get(symbol) || {};
  const currentQty = Math.max(0, num(currentPosition.quantity ?? currentPosition.Quantity ?? currentPosition.qty) || 0);
  const quantityIncrement = positive(contract.quantity_increment) || 1;
  const minQuantity = positive(contract.min_quantity) || quantityIncrement;
  const quantityRuleKnown = contract.quantity_rule_known !== false;
  const feeBps = Math.max(0, num(input.config?.default_fee_bps) ?? 10);
  const maxOrderPct = positive(input.config?.max_order_value_pct) || 15;
  const maxOpenPositions = positive(input.config?.max_open_positions) || 10;
  const roundQtyUp = (qty) => Math.ceil((qty - 1e-9) / quantityIncrement) * quantityIncrement;
  const roundQtyDown = (qty) => Math.floor((qty + 1e-9) / quantityIncrement) * quantityIncrement;
  const minAdditionalQty = priceEUR !== null && priceEUR > 0
    ? roundQtyUp(Math.max(minQuantity, minOrderValueEUR / priceEUR)) : null;
  const minFinalQty = minAdditionalQty === null ? null : currentQty + minAdditionalQty;
  const sector = String(row.sector || currentPosition.sector || "UNKNOWN").toUpperCase();
  const sectorValueEUR = Array.from(positionMap.values())
    .filter((p) => String(p.sector || "UNKNOWN").toUpperCase() === sector)
    .reduce((sum, p) => sum + (num(p.marketValue) || 0), 0);
  const sectorRoomEUR = sector === "UNKNOWN" ? totalValue : Math.max(0,
    totalValue * (positive(input.config?.max_sector_pct) || 40) / 100 - sectorValueEUR);
  const cashRoomEUR = Math.max(0, cashEUR) / (1 + feeBps / 10000);
  const positionRoomEUR = Math.max(0, totalValue * maxPositionPct / 100 - currentQty * (priceEUR || 0));
  const maxAdditionalQty = priceEUR > 0 ? roundQtyDown(Math.min(cashRoomEUR, positionRoomEUR,
    sectorRoomEUR, totalValue * maxOrderPct / 100) / priceEUR) : null;
  const maxFinalQty = maxAdditionalQty === null ? null : currentQty + maxAdditionalQty;
  const minTargetWeightPct = minFinalQty !== null && totalValue > 0 ? minFinalQty * priceEUR / totalValue * 100 : null;
  const minCashRequiredEUR = minAdditionalQty === null ? null : minAdditionalQty * priceEUR * (1 + feeBps / 10000);
  let feasibilityReason = "OK";
  if (!quantityRuleKnown) feasibilityReason = "QUANTITY_RULE_UNAVAILABLE";
  else if (minAdditionalQty === null) feasibilityReason = "FX_RATE_OR_EUR_PRICE_UNAVAILABLE";
  else if (currentQty <= 0 && positionMap.size >= maxOpenPositions) feasibilityReason = "MAX_OPEN_POSITIONS";
  else if (minAdditionalQty * priceEUR > positionRoomEUR + 1e-8) feasibilityReason = "MIN_TICKET_EXCEEDS_POSITION_CAP";
  else if (minCashRequiredEUR > cashEUR + 1e-8) feasibilityReason = "INSUFFICIENT_CASH_FOR_MIN_TICKET";
  else if (minAdditionalQty * priceEUR > totalValue * maxOrderPct / 100 + 1e-8) feasibilityReason = "MIN_TICKET_EXCEEDS_ORDER_CAP";
  else if (minAdditionalQty * priceEUR > sectorRoomEUR + 1e-8) feasibilityReason = "MIN_TICKET_EXCEEDS_SECTOR_CAP";
  const minOrderFeasible = feasibilityReason === "OK";
  if (String(row.decision || "") === "Entrer / Renforcer" && !minOrderFeasible) gates.add("MIN_ORDER_UNFEASIBLE");
  // Strong liquidity evidence: resolved contract, fresh non-stale price, daily
  // volume above floor, and a target order within the volume cap. BUYs are
  // LIMIT-only downstream, so a missing instantaneous spread on such a name is a
  // quoting gap, not a tradability risk.
  const highVolumeBar = Number($env.AG1_LIQUIDITY_HIGH_VOLUME || 1000000);
  const orderWithinCap = (orderVolumePct === null || orderVolumePct <= maxOrderVolumePct);
  const contractOk = conid !== null && !unresolved.has(symbol);
  const pricedOk = price !== null && price > 0;
  // A name trading >= highVolumeBar shares/day with a resolved contract and a
  // usable price IS liquid, even if the instantaneous bid/ask is momentarily
  // missing or the quote is a touch stale.
  const highVolumeLiquid = volume !== null && volume >= highVolumeBar && contractOk && pricedOk && orderWithinCap;
  const liquidityEvidenceOk = (volume !== null && volume >= minVolume
    && pricedOk && contractOk
    && quoteAgeMinutes !== null && quoteAgeMinutes <= maxQuoteAgeSeconds / 60
    && orderWithinCap) || highVolumeLiquid;
  if (volume === null || quoteAgeMinutes === null) {
    gates.add("LIQUIDITY_UNKNOWN");
  } else if (spreadPct === null) {
    if (allowUnquotedSpread && liquidityEvidenceOk) gates.add("SPREAD_UNQUOTED");
    else gates.add("LIQUIDITY_UNKNOWN");
  }
  if (volume !== null && volume < minVolume) gates.add("LIQUIDITY_STRESS");
  if (spreadPct !== null && spreadPct > maxSpreadPct) gates.add("LIQUIDITY_STRESS");
  if (orderVolumePct !== null && orderVolumePct > maxOrderVolumePct) gates.add("LIQUIDITY_STRESS");
  if (priceDivergencePct !== null && priceDivergencePct > maxEntryQuoteDeviationPct) gates.add("PRICE_DIVERGENCE");
  if (quoteAgeMinutes === null || quoteAgeMinutes > maxQuoteAgeSeconds / 60) gates.add("STALE_QUOTE");

  if (price !== null && price > 0) {
    row.matrix_entry = row.matrix_entry ?? row.entry;
    row.entry = Math.round(price * 10000) / 10000;
  }
  row.currency = currency;
  row.fx_rate_to_eur = fxRate;
  row.price_eur = priceEUR === null ? null : round4(priceEUR);
  row.min_price_increment = positive(contract.increment);
  row.increment_rules = Array.isArray(contract.increment_rules) ? contract.increment_rules : [];
  row.execution_constraints = {
    feasible: minOrderFeasible,
    reason: feasibilityReason,
    minOrderValueEUR,
    currentQty,
    minAdditionalQty,
    minFinalQty,
    maxFinalQty,
    minTargetWeightPct: minTargetWeightPct === null ? null : Math.ceil((minTargetWeightPct - 1e-10) * 10000) / 10000,
    maxTargetWeightPct: maxFinalQty !== null && totalValue > 0 ? Math.floor((maxFinalQty * priceEUR / totalValue * 100 + 1e-10) * 10000) / 10000 : null,
    minCashRequiredEUR: round4(minCashRequiredEUR),
    quantityIncrement, minQuantity, quantityRuleKnown,
    cashSource: "confirmed_portfolio_snapshot",
    cashEUR: round4(cashEUR),
    priceEUR: priceEUR === null ? null : round4(priceEUR),
    currency,
    fxRateToEUR: fxRate,
  };
  row.buy_feasibility = { ...row.execution_constraints, side: "BUY" };
  row.sell_feasibility = {
    side: "SELL", feasible: currentQty > 0 && conid !== null && !unresolved.has(symbol),
    reason: currentQty <= 0 ? "NO_HELD_POSITION" : (conid === null || unresolved.has(symbol) ? "IBKR_CONTRACT_UNRESOLVED" : "OK"),
    maxQuantity: currentQty, cashRequiredEUR: 0,
    liquidityWarning: Array.from(gates).filter((gate) => ["LIQUIDITY_STRESS", "STALE_QUOTE"].includes(gate)),
    quantityIncrement, minQuantity,
  };
  row.execution_constraints.side = "BUY"; // legacy field, never a SELL veto
  row.quantity_increment = quantityIncrement;
  row.min_quantity = minQuantity;
  row.quote_source = ibkrHasFreshPrice
    ? "ibkr_cpapi_snapshot"
    : (ibkrHasHistoryPrice ? `ibkr_cpapi_history_${ibkrHistory.barSize}` : (quote.source || row.quote_source || null));
  row.quote_fetched_at = quoteTime;
  row.regular_market_time = quoteTime;
  row.market_state = ibkrHasFreshPrice ? (availability.startsWith("D") ? "IBKR_DELAYED" : "IBKR_SNAPSHOT") : (ibkrHasHistoryPrice ? "IBKR_HISTORY" : (quote.marketState || row.market_state || null));

  row.gates = gates.size ? Array.from(gates).sort().join("|") : "OK";
  row.spread_pct = spreadPct;
  row.volume = volume;
  row.liquidity = {
    status: gates.has("LIQUIDITY_STRESS") ? "STRESS" : (gates.has("LIQUIDITY_UNKNOWN") ? "UNKNOWN" : "OK"),
    conid,
    contractResolved: conid !== null,
    quoteSource: row.quote_source,
    quoteTime,
    quoteTimestampSource: ibkrHasFreshPrice ? (snapshotAsOf ? "ibkr_update_conservative_delay" : "yfinance_market_time") : (ibkrHasHistoryPrice ? "ibkr_history_bar" : "yfinance_market_time"),
    ibkrDataAvailability: availability || null,
    quoteAgeMinutes: quoteAgeMinutes === null ? null : Math.round(quoteAgeMinutes * 10) / 10,
    marketState: row.market_state,
    price,
    priceEUR: priceEUR === null ? null : round4(priceEUR),
    currency,
    fxRateToEUR: fxRate,
    fxProvenance: fxProvenance.get(currency) || null,
    minPriceIncrement: positive(contract.increment),
    incrementDigits: num(contract.increment_digits),
    incrementRules: Array.isArray(contract.increment_rules) ? contract.increment_rules : [],
    bid: ibkrHasFreshPrice ? ibkrBid : num(quote.bid),
    ask: ibkrHasFreshPrice ? ibkrAsk : num(quote.ask),
    spreadPct,
    spreadObserved: spreadPct !== null,
    spreadUnquoted: gates.has("SPREAD_UNQUOTED"),
    volume,
    estimatedTargetWeightPct: defaultWeightPct,
    estimatedTargetQty: targetQty,
    estimatedOrderToVolumePct: orderVolumePct === null ? null : Math.round(orderVolumePct * 10000) / 10000,
    entryQuoteDivergencePct: priceDivergencePct === null ? null : Math.round(priceDivergencePct * 10000) / 10000,
    originalMatrixEntry: entry,
    ibkrSnapshotPrice: ibkrPrice,
    ibkrSnapshotAttempts,
    ibkrHistoryPrice,
    ibkrHistoryTime: ibkrHistory?.time || null,
    ibkrHistoryBar: ibkrHistory ? { period: ibkrHistory.period, barSize: ibkrHistory.barSize } : null,
    executionConstraints: row.execution_constraints,
    readOnlyChecks: ["yfinance_quote", "ibkr_contract_resolution", "ibkr_contract_rules", "ibkr_account_ledger", "ibkr_fx_snapshot", "yfinance_dated_fx_cross", "ibkr_market_snapshot", "ibkr_market_history"],
  };
}

const entryRows = rows.filter((row) => row.decision === "Entrer / Renforcer");
const entryEligible = entryRows.filter((row) => row.buy_feasibility?.feasible === true
  && gateList(row.gates).every((gate) => gate === "SPREAD_UNQUOTED"));
const finalEntries = entryEligible.slice(0, 10);
const heldRows = rows.filter((row) => positionMap.has(normSymbol(row.symbol)));
const watchRows = rows.filter((row) => row.decision === "Surveiller" && !positionMap.has(normSymbol(row.symbol))).slice(0, 4);
const selected = new Map([...finalEntries, ...heldRows, ...watchRows].map((row) => [normSymbol(row.symbol), row]));
pack.rows = Array.from(selected.values());
pack.unfunded_candidates = entryRows.filter((row) => !selected.has(normSymbol(row.symbol)))
  .slice(0, 5).map((row) => ({ symbol: row.symbol, grade: row.grade, score: row.p_win_pct,
    reason: row.buy_feasibility?.reason, gates: row.gates, eligibleForOrder: false }));
pack.selection = { stage: "after_preflight", screenedEntries: entryRows.length,
  eligibleEntries: entryEligible.length, selectedEntries: finalEntries.length,
  preservedHeld: heldRows.length, reserveLimit: 50 };
pack.score_legend = "p_win_pct and ev_r are uncalibrated heuristic scores, not measured probabilities or expected returns.";
const executionAudit = rows.map((row) => ({ symbol: row.symbol, rank: row.matrix_rank,
  selected: selected.has(normSymbol(row.symbol)), decision: row.decision, gates: row.gates,
  buy: row.buy_feasibility, sell: row.sell_feasibility, score: row.p_win_pct, price: row.entry,
  currency: row.currency, fxRateToEUR: row.fx_rate_to_eur, asOf: row.quote_fetched_at }));
pack.liquidityPreflight = {
  generatedAt: new Date().toISOString(),
  candidateCount: candidates.length,
  contractResolved: conidMap.size,
  quoteCount: quotes.length,
  ibkrSnapshotCount: ibkrSnapshots.length,
  ibkrSnapshotPricedCount: ibkrSnapshots.filter(snapshotHasPrice).length,
  ibkrSnapshotAttempts,
  ibkrHistoryAttempts,
  ibkrHistoryPricedCount: ibkrHistoryMap.size,
  accountLedgerLoaded,
  fxCurrenciesResolved: Array.from(fxRateToEUR.keys()).sort(),
  warnings: preflightWarnings,
  orderEndpointsCalled: false,
};

return [{ json: { ...input, opportunity_pack: pack, execution_audit: executionAudit } }];
