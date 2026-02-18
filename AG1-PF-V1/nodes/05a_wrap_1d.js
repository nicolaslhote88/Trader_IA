// PF.05A - Wrap 1D response
return $input.all().map((it) => {
  const r = it.json ?? {};
  return { json: { symbol: r.symbol, yf_1d: r } };
});
