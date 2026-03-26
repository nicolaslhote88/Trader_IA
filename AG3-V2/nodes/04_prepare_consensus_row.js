const row = $json.consensusRow || null;
if (!row) return [];
return [{ json: row }];
