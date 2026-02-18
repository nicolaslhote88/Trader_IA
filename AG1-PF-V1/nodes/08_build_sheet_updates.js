// PF.08 - Build Sheet Updates + payload for DuckDB writer
// Input: items from PF.07
// Output: only position update rows (no META row updates)

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
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

const nowIso = new Date().toISOString();
const inItems = $input.all().map((i) => i.json || {});
const fallbackRunId = makeFallbackRunId();

const positionUpdates = inItems
  .filter((j) => j?.gs_update?.row_number != null)
  .map((j) => {
    const u = j.gs_update;
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
      run_id: j.run_id || fallbackRunId,
      portfolio_db_path: j.portfolio_db_path || j.db_path || "/files/duckdb/ag1_v2.duckdb",
      workflow_name: j.workflow_name || "PF Portfolio MTM Updater",

      mtm_ok: j.mtm_ok,
      mtm_status: j.mtm_status,
      mtm_reason: j.mtm_reason,
      mtm_price_picked: j.mtm_price_picked,
      mtm_price_asof: j.mtm_price_asof,
    };
  });

return positionUpdates.map((o) => ({ json: o }));
