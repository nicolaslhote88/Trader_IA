// PF.08 - Build DuckDB payloads
// Input: items from PF.07 (1 item = 1 position with MTM)
// Output: per-portfolio rows for PF.08B (DuckDB writer), plus placeholders when a portfolio has 0 positions

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function safeNumOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function round2(v) {
  const n = safeNum(v);
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

function makeFallbackRunId() {
  const d = new Date();
  const pad = (x) => String(x).padStart(2, "0");
  return `PFMTM_${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
}

function safeJsonParse(v, fallback = null) {
  try {
    if (v == null || v === "") return fallback;
    if (typeof v === "object") return v;
    return JSON.parse(String(v));
  } catch {
    try {
      const s = String(v ?? "");
      if (s.includes(";")) {
        return JSON.parse(s.replace(/;/g, ","));
      }
    } catch {}
    return fallback;
  }
}

function normPath(v) {
  const s = String(v ?? "").trim();
  return s || null;
}

function normalizeDbPath(v) {
  const s = String(v ?? "").trim().replace(/\\/g, "/");
  return s.replace("/local-files/", "/files/");
}

function isLegacyAg1DbPath(v) {
  const s = String(v ?? "").trim().toLowerCase().replace(/\\/g, "/");
  return s.endsWith("/ag1_v2.duckdb");
}

function normSymbol(v) {
  return String(v ?? "").trim().toUpperCase();
}

function uniqPaths(arr) {
  const out = [];
  const seen = new Set();
  for (const x of arr || []) {
    const p = normPath(x);
    if (!p) continue;
    const k = p.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(p);
  }
  return out;
}

function getRowNum(r) {
  const n = Number(r?.row_number);
  return Number.isFinite(n) ? n : null;
}

function stripSpaces(s) {
  return String(s ?? "").replace(/[\s\u00A0\u202F]/g, "");
}

function parseFrNumber(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s0 = stripSpaces(v).replace(/EUR/gi, "").replace(/€/g, "").trim();
  if (!s0) return null;
  const s1 = s0.replace(/\./g, "").replace(",", ".");
  const n = Number(s1);
  return Number.isFinite(n) ? n : null;
}

function safeNodeItems(nodeName) {
  try {
    return $items(nodeName).map((i) => i.json || {});
  } catch {
    return [];
  }
}

function loadConfig() {
  const candidates = ["PF.00 - Config", "PF.00", "Config"];
  for (const name of candidates) {
    const arr = safeNodeItems(name);
    if (arr.length) return arr[0] || {};
  }
  return {};
}

function resolvePortfolioDbPaths(cfg, inItems, readRows, strictDbRouting) {
  const arr1 = Array.isArray(cfg?.portfolio_db_paths) ? cfg.portfolio_db_paths : [];
  const parsedJson = safeJsonParse(cfg?.portfolio_db_paths_json, null);
  const arr2 = Array.isArray(parsedJson) ? parsedJson : [];
  const csv = String(cfg?.portfolio_db_paths_csv ?? "").trim();
  const arr3 = csv ? csv.split(/[;,]/).map((x) => x.trim()) : [];
  const singles = [
    cfg?.portfolio_db_path,
    ...inItems.map((j) => j?.portfolio_db_path || j?.db_path),
    ...readRows.map((r) => r?.portfolio_db_path || r?.db_path),
  ];
  const out = uniqPaths([...arr1, ...arr2, ...arr3, ...singles]).filter((p) => !isLegacyAg1DbPath(p));
  if (strictDbRouting && out.length === 0) {
    throw new Error("Erreur FATALE (PF.08 JS) : Aucun chemin de base de données résolu en mode strict.");
  }
  return out;
}

function buildPfCtxFromRows(rows, dbPath) {
  const metaRow = rows.find((r) => normSymbol(r.Symbol) === "__META__") || null;
  const cashRow = rows.find((r) => normSymbol(r.Symbol) === "CASH_EUR") || null;

  const metaRowNumSrc = getRowNum(metaRow);
  const cashRowNumSrc = getRowNum(cashRow);
  const headerOffset = (metaRowNumSrc === 1) ? 1 : 0;

  const initialCapitalEUR = metaRow ? parseFrNumber(metaRow.MarketValue) : null;
  const cashMarketValueRaw = cashRow?.MarketValue ?? cashRow?.LastPrice ?? "";
  const cashMarketValueEUR = parseFrNumber(cashMarketValueRaw);

  return {
    portfolio_db_path: dbPath || "",
    headerOffset,
    meta: {
      row_number: (metaRowNumSrc ?? 1) + headerOffset,
      row_number_src: metaRowNumSrc,
      UpdatedAt: metaRow?.UpdatedAt ?? null,
      initialCapitalEUR,
    },
    cash: {
      row_number: (cashRowNumSrc ?? 2) + headerOffset,
      row_number_src: cashRowNumSrc,
      UpdatedAt: cashRow?.UpdatedAt ?? null,
      MarketValueRaw: cashMarketValueRaw,
      MarketValueEUR: cashMarketValueEUR,
    },
  };
}

const nowIso = new Date().toISOString();
const inItems = $input.all().map((i) => i.json || {});
const cfg = loadConfig();
const readRows = safeNodeItems("Read Portfolio");
const fallbackRunId = makeFallbackRunId();
const mtmRunId = String(cfg.workflow_run_id || cfg.run_id || fallbackRunId);
const strictDbRouting = cfg.strict_db_routing !== false;
const purgeLatestBeforeWrite = cfg.purge_latest_before_write !== false;
const writeOnlyIfAnyPriceFound = cfg.write_only_if_any_price_found === true;

const runInfoByDb = new Map();
for (const r of readRows) {
  const rawPath = r.portfolio_db_path || r.db_path;
  const normKey = normalizeDbPath(rawPath);
  if (!normKey) continue;
  if (!runInfoByDb.has(normKey)) {
    runInfoByDb.set(normKey, { ag1_source_run_id: null, ag1_source_snapshot_ts: null });
  }
  const info = runInfoByDb.get(normKey);
  if (!info.ag1_source_run_id) {
    info.ag1_source_run_id = normPath(r.ag1_source_run_id || r.run_id || null);
  }
  if (!info.ag1_source_snapshot_ts) {
    info.ag1_source_snapshot_ts = normPath(r.ag1_source_snapshot_ts || r.UpdatedAt || null);
  }
}

const readRowsByDb = new Map();
for (const r of readRows) {
  const rawPath = r.portfolio_db_path || r.db_path || cfg.portfolio_db_path;
  const normKey = normalizeDbPath(rawPath);
  if (!normKey) continue;
  if (!readRowsByDb.has(normKey)) readRowsByDb.set(normKey, []);
  readRowsByDb.get(normKey).push(r);
}

const ctxByDb = new Map();
for (const [normKey, rows] of readRowsByDb.entries()) {
  ctxByDb.set(normKey, buildPfCtxFromRows(rows, normKey));
}

for (const j of inItems) {
  const rawPath = j.portfolio_db_path || j.db_path || cfg.portfolio_db_path;
  const normKey = normalizeDbPath(rawPath);
  if (!normKey) continue;
  if (j?.pf_ctx && !ctxByDb.has(normKey)) {
    ctxByDb.set(normKey, j.pf_ctx);
  }
}

const targetDbPaths = resolvePortfolioDbPaths(cfg, inItems, readRows, strictDbRouting);
const targetByNorm = new Map();
for (const p of targetDbPaths) {
  targetByNorm.set(normalizeDbPath(p), p);
}
const seenPositionDbs = new Set();

const positionUpdates = inItems
  .filter((j) => j?.gs_update?.row_number != null)
  .map((j) => {
    const u = j.gs_update;
    const primaryDbPathRaw = normPath(j.portfolio_db_path || j.db_path || cfg.portfolio_db_path);
    if (!primaryDbPathRaw) {
      throw new Error("Erreur FATALE (PF.08 JS) : portfolio_db_path manquant pour une position.");
    }
    if (isLegacyAg1DbPath(primaryDbPathRaw)) {
      throw new Error(`Erreur FATALE (PF.08 JS) : Chemin legacy détecté (${primaryDbPathRaw}).`);
    }

    const currentNormPath = normalizeDbPath(primaryDbPathRaw);
    const routedDbPath = targetByNorm.get(currentNormPath) || primaryDbPathRaw;
    seenPositionDbs.add(currentNormPath);

    const pfCtx = j.pf_ctx || ctxByDb.get(currentNormPath) || {};
    const pfCash = pfCtx.cash || {};
    const pfMeta = pfCtx.meta || {};
    const ag1Info = runInfoByDb.get(currentNormPath) || {};
    const ag1SourceRunId = normPath(j.ag1_source_run_id || j.run_id || ag1Info.ag1_source_run_id || null);
    const ag1SourceSnapshotTs = normPath(j.ag1_source_snapshot_ts || j.UpdatedAt || ag1Info.ag1_source_snapshot_ts || null);

    return {
      row_number: u.row_number,
      LastPrice: round2(u.LastPrice),
      MarketValue: round2(u.MarketValue),
      UnrealizedPnL: round2(u.UnrealizedPnL),
      UpdatedAt: u.UpdatedAt || nowIso,

      Name: j.Name ?? "",
      AssetClass: j.AssetClass ?? "",
      Sector: j.Sector ?? "",
      Industry: j.Industry ?? "",

      // Extra fields for DuckDB persistence node
      Symbol: j.symbol || j.Symbol || "",
      ISIN: j.ISIN || "",
      Quantity: j.qty ?? j.Quantity ?? "",
      AvgPrice: j.avgPrice ?? j.AvgPrice ?? "",
      run_id: mtmRunId,
      workflow_run_id: mtmRunId,
      ag1_source_run_id: ag1SourceRunId,
      ag1_source_snapshot_ts: ag1SourceSnapshotTs,
      portfolio_db_path: routedDbPath,
      workflow_name: j.workflow_name || cfg.workflow_name || "PF Portfolio MTM Updater (DuckDB-only, AG1-V4)",
      strict_db_routing: strictDbRouting,
      purge_latest_before_write: purgeLatestBeforeWrite,
      write_only_if_any_price_found: writeOnlyIfAnyPriceFound,

      mtm_ok: j.mtm_ok,
      mtm_status: j.mtm_status,
      mtm_reason: j.mtm_reason,
      mtm_price_picked: j.mtm_price_picked,
      mtm_price_asof: j.mtm_price_asof,
      mtm_price_source: j.mtm_price_source,
      mtm_price_stale: j.mtm_price_stale,

      // Portfolio context persisted by PF.08B in DuckDB technical rows (per portfolio).
      pf_cash_market_value: safeNum(pfCash.MarketValueEUR),
      pf_cash_updated_at: pfCash.UpdatedAt || nowIso,
      pf_initial_capital: safeNumOrNull(pfMeta.initialCapitalEUR),
      pf_meta_updated_at: pfMeta.UpdatedAt || nowIso,
    };
  });

// Ensure PF.08B can still persist CASH_EUR/__META__ for portfolios with zero equity rows.
for (const dbPath of targetDbPaths) {
  const currentNormPath = normalizeDbPath(dbPath);
  if (seenPositionDbs.has(currentNormPath)) continue;
  const pfCtx = ctxByDb.get(currentNormPath) || {};
  const pfCash = pfCtx.cash || {};
  const pfMeta = pfCtx.meta || {};
  const ag1Info = runInfoByDb.get(currentNormPath) || {};
  positionUpdates.push({
    row_number: null,
    LastPrice: 0,
    MarketValue: 0,
    UnrealizedPnL: 0,
    UpdatedAt: nowIso,
    Name: "",
    AssetClass: "",
    Sector: "",
    Industry: "",
    Symbol: "",
    ISIN: "",
    Quantity: "",
    AvgPrice: "",
    run_id: mtmRunId,
    workflow_run_id: mtmRunId,
    ag1_source_run_id: ag1Info.ag1_source_run_id || null,
    ag1_source_snapshot_ts: ag1Info.ag1_source_snapshot_ts || null,
    portfolio_db_path: dbPath,
    workflow_name: cfg.workflow_name || "PF Portfolio MTM Updater (DuckDB-only, AG1-V4)",
    strict_db_routing: strictDbRouting,
    purge_latest_before_write: purgeLatestBeforeWrite,
    write_only_if_any_price_found: writeOnlyIfAnyPriceFound,
    mtm_ok: true,
    mtm_status: "NO_POSITION_ROWS",
    mtm_reason: "",
    mtm_price_picked: null,
    mtm_price_asof: null,
    mtm_price_source: null,
    mtm_price_stale: null,
    pf_cash_market_value: safeNum(pfCash.MarketValueEUR),
    pf_cash_updated_at: pfCash.UpdatedAt || nowIso,
    pf_initial_capital: safeNumOrNull(pfMeta.initialCapitalEUR),
    pf_meta_updated_at: pfMeta.UpdatedAt || nowIso,
  });
}

if (positionUpdates.length === 0) {
  throw new Error("Erreur FATALE (PF.08 JS) : Aucun payload n'a été construit.");
}

return positionUpdates.map((o) => ({ json: o }));
