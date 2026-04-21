// PF.07 - Compute MTM (skip-safe)
// Input: enriched portfolio item (row_number, qty/avgPrice, yf_1d, optional yf_1h)
// Output: same fields + LastPrice / MarketValue / UnrealizedPnL + diagnostics + gs_update

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseTime(t) {
  const d = new Date(t);
  const ms = d.getTime();
  return Number.isFinite(ms) ? ms : null;
}

function extractLast(yf) {
  if (!yf || typeof yf !== "object") return null;
  if (yf.ok === false) return { ok: false, reason: yf.error || "YF_OK_FALSE" };

  if (yf.last && typeof yf.last === "object") {
    const price = safeNum(yf.last.c);
    const tMs = parseTime(yf.last.t);
    if (price != null) {
      return {
        ok: true,
        price,
        t: yf.last.t || null,
        tMs,
        interval: yf.interval || null,
        source: yf.source || null,
        stale: !!yf.stale,
      };
    }
  }

  if (Array.isArray(yf.bars) && yf.bars.length) {
    const b = yf.bars[yf.bars.length - 1];
    const price = safeNum(b?.c);
    const tMs = parseTime(b?.t);
    if (price != null) {
      return {
        ok: true,
        price,
        t: b.t || null,
        tMs,
        interval: yf.interval || null,
        source: yf.source || null,
        stale: !!yf.stale,
      };
    }
  }

  return { ok: false, reason: "NO_LAST_PRICE_IN_PAYLOAD" };
}

return $input.all().map((it) => {
  const j = it.json ?? {};

  const qty = safeNum(j.qty ?? j.Quantity);
  const avgPrice = safeNum(j.avgPrice ?? j.AvgPrice);

  const last1d = extractLast(j.yf_1d);
  const last1h = extractLast(j.yf_1h);

  let chosen = null;
  if (last1h?.ok && last1d?.ok) {
    if (last1h.tMs != null && last1d.tMs != null) {
      chosen = (last1h.tMs >= last1d.tMs) ? { ...last1h, picked: "1h" } : { ...last1d, picked: "1d" };
    } else {
      chosen = { ...last1h, picked: "1h" };
    }
  } else if (last1h?.ok) {
    chosen = { ...last1h, picked: "1h" };
  } else if (last1d?.ok) {
    chosen = { ...last1d, picked: "1d" };
  }

  const fallbackLastPrice = safeNum(j.LastPrice);
  if (!chosen && fallbackLastPrice != null && fallbackLastPrice > 0) {
    chosen = {
      ok: true,
      price: fallbackLastPrice,
      t: j.asof || j.UpdatedAt || null,
      tMs: parseTime(j.asof || j.UpdatedAt),
      interval: "fallback",
      source: "portfolio_input",
      stale: true,
      picked: "fallback",
    };
  }

  const updatedAt = new Date().toISOString();

  let mtm_ok = true;
  let mtm_status = "OK";
  let mtm_reason = null;

  const price = chosen?.ok ? chosen.price : null;
  const usedFallbackInput = chosen?.picked === "fallback";

  if (qty == null || avgPrice == null) {
    mtm_ok = false;
    mtm_status = "INVALID_INPUT";
    mtm_reason = "Missing qty or avgPrice";
  } else if (price == null) {
    mtm_ok = false;
    mtm_status = "NO_PRICE";
    mtm_reason = `No usable price from yf_1h/yf_1d (1h=${last1h?.reason || "n/a"}, 1d=${last1d?.reason || "n/a"})`;
  } else if (usedFallbackInput) {
    mtm_status = "FALLBACK_INPUT";
    mtm_reason = `No usable price from yf_1h/yf_1d; reused portfolio input (1h=${last1h?.reason || "n/a"}, 1d=${last1d?.reason || "n/a"})`;
  }

  const MarketValue = (mtm_ok ? qty * price : safeNum(j.MarketValue) ?? 0);
  const UnrealizedPnL = (mtm_ok ? qty * (price - avgPrice) : safeNum(j.UnrealizedPnL) ?? 0);

  const out = {
    ...j,

    LastPrice: mtm_ok ? price : (safeNum(j.LastPrice) ?? 0),
    MarketValue,
    UnrealizedPnL,
    UpdatedAt: updatedAt,

    mtm_ok,
    mtm_status,
    mtm_reason,
    mtm_price: price,
    mtm_price_picked: chosen?.picked || null,
    mtm_price_asof: chosen?.t || null,
    mtm_price_interval: chosen?.interval || null,
    mtm_price_source: chosen?.source || null,
    mtm_price_stale: chosen?.stale ?? null,

    gs_update: {
      row_number: j.row_number,
      LastPrice: mtm_ok ? price : (safeNum(j.LastPrice) ?? 0),
      MarketValue,
      UnrealizedPnL,
      UpdatedAt: updatedAt,
    },
  };

  if (out.yf_1d?.bars) delete out.yf_1d.bars;
  if (out.yf_1h?.bars) delete out.yf_1h.bars;

  return { json: out };
});
