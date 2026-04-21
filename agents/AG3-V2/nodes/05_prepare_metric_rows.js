const rows = Array.isArray($json.metricRows) ? $json.metricRows : [];
if (!rows.length) return [];
return [{ json: { metricRows: rows } }];
