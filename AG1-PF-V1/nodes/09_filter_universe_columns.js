// Keep only identity + sector classification fields from Universe lookup
return $input.all().map((item) => {
  const row = item.json || {};
  return {
    json: {
      Symbol: row.Symbol,
      Name: row.Name,
      AssetClass: row.AssetClass,
      Sector: row.Sector,
      Industry: row.Industry,
      ISIN: row.ISIN,
    },
  };
});
