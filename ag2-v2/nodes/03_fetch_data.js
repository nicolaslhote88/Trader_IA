// AG2-V2 — Fetch H1 + D1 from yfinance-api in a single node
const item = $input.item.json;
const base = item.yfinance_api_base || 'http://yfinance-api:8080';
const symbol = item.symbol;

async function fetchHistory(interval, lookback, maxBars, minBars) {
  try {
    const resp = await $http.request({
      method: 'GET',
      url: `${base}/history`,
      qs: {
        symbol,
        interval,
        lookback_days: lookback,
        max_bars: maxBars,
        min_bars: minBars,
        allow_stale: 'true',
      },
      timeout: 60000,
      json: true,
    });
    return resp;
  } catch (e) {
    return { ok: false, error: e.message, bars: [], count: 0, source: 'error' };
  }
}

const [h1, d1] = await Promise.all([
  fetchHistory(item.intraday.interval, item.intraday.lookback_days,
               item.intraday.max_bars, item.intraday.min_bars),
  fetchHistory(item.daily.interval, item.daily.lookback_days,
               item.daily.max_bars, item.daily.min_bars),
]);

return [{
  json: {
    ...item,
    h1_response: h1,
    d1_response: d1,
  }
}];
