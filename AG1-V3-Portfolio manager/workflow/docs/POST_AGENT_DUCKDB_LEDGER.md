# AG1 Post-Agent DuckDB Ledger Notes

This file documents the post-agent branch:

1. `7 - Validate & Enforce Safety`
2. `8 - Build DuckDB Bundle`
3. `9 - Upsert Run Bundle (DuckDB)`
4. `10 - Post-Run Health (DuckDB)`

`9 - Upsert Run Bundle (DuckDB)` loads an external `duckdb_writer.py` and requires:

- `AG1_DUCKDB_PATH`
- `AG1_DUCKDB_WRITER_PATH`
- `AG1_LEDGER_SCHEMA_PATH` (optional override)

See `docs/architecture/etat_des_lieux.md` in the repo root for the canonical
description of this branch.
