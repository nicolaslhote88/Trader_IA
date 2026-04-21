const row = $json.triageRow || null;
if (!row) return [];
return [{ json: row }];
