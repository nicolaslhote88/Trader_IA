// PF.05B - Wrap 1H response
return $input.all().map((it) => {
  const r = it.json ?? {};
  return { json: { symbol: r.symbol, yf_1h: r } };
});
