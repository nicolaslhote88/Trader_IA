/**
 * PF.02 - Normalize positions (1 item = 1 row)
 * - Parses portfolio row values robustly (FR number support)
 * - Excludes technical rows (__META__, CASH_EUR)
 * - Adds config from PF.00 node to each output item
 */

function safeJsonParse(v, fallback = {}) {
  try {
    if (!v) return fallback;
    if (typeof v === "object") return v;
    return JSON.parse(String(v));
  } catch {
    return fallback;
  }
}

function normSymbol(v) {
  return String(v ?? "").trim();
}

function stripSpaces(s) {
  return String(s ?? "").replace(/[\s\u00A0\u202F]/g, "");
}

function parseFrNumber(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;

  const s0 = stripSpaces(v)
    .replace(/EUR/gi, "")
    .replace(/€/g, "")
    .trim();

  if (!s0) return null;

  const s1 = s0.replace(/\./g, "").replace(",", ".");
  const n = Number(s1);
  return Number.isFinite(n) ? n : null;
}

function getRowNum(r) {
  const n = Number(r?.row_number);
  return Number.isFinite(n) ? n : null;
}

function loadConfig() {
  const candidates = ["PF.00 - Config", "PF.00", "Config"];
  for (const name of candidates) {
    try {
      const it = $items(name);
      if (it?.length) return it[0].json ?? {};
    } catch {}
  }
  return {};
}

const cfg = loadConfig();
const rows = $input.all().map(i => i.json || {});

const metaRow = rows.find(r => normSymbol(r.Symbol) === "__META__") || null;
const cashRow = rows.find(r => normSymbol(r.Symbol) === "CASH_EUR") || null;

const metaRowNumSrc = getRowNum(metaRow);
const cashRowNumSrc = getRowNum(cashRow);

const headerOffset = (metaRowNumSrc === 1) ? 1 : 0;
const metaRowNumber = (metaRowNumSrc ?? 1) + headerOffset;
const cashRowNumber = (cashRowNumSrc ?? 2) + headerOffset;

const initialCapitalEUR = metaRow ? parseFrNumber(metaRow.MarketValue) : 50000;
const cashMarketValueRaw = cashRow?.MarketValue ?? cashRow?.LastPrice ?? "";
const cashMarketValueEUR = parseFrNumber(cashMarketValueRaw);

const pf_ctx = {
  headerOffset,
  meta: {
    row_number: metaRowNumber,
    row_number_src: metaRowNumSrc,
    UpdatedAt: metaRow?.UpdatedAt ?? null,
    initialCapitalEUR,
  },
  cash: {
    row_number: cashRowNumber,
    row_number_src: cashRowNumSrc,
    UpdatedAt: cashRow?.UpdatedAt ?? null,
    MarketValueRaw: cashMarketValueRaw,
    MarketValueEUR: cashMarketValueEUR,
  },
};

const positions = rows.filter(r => {
  const sym = normSymbol(r.Symbol);
  if (!sym) return false;
  if (sym === "__META__") return false;
  if (sym === "CASH_EUR") return false;

  const q = r.Quantity ?? r.qty;
  return q !== null && q !== undefined && String(q).trim() !== "";
});

const out = positions.map(r => {
  const rowNumSrc = getRowNum(r);
  const rowNumSheet = rowNumSrc !== null ? (rowNumSrc + headerOffset) : null;

  const symbol = normSymbol(r.Symbol);
  const qty = parseFrNumber(r.Quantity ?? r.qty);
  const avgPrice = parseFrNumber(r.AvgPrice ?? r.avgPrice);

  return {
    ...cfg,
    ...r,
    pf_ctx,
    row_number_src: rowNumSrc,
    row_number: rowNumSheet ?? rowNumSrc,
    symbol,
    qty,
    avgPrice,
    Name: r.Name,
    AssetClass: r.AssetClass,
    Sector: r.Sector,
    Industry: r.Industry,
    ISIN: r.ISIN,
  };
});

return out.map(j => ({ json: j }));
