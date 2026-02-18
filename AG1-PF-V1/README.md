# AG1-PF-V1

DuckDB extension for the portfolio MTM workflow.

Files:
- `docs/INTEGRATION.md`: n8n wiring and migration notes.
- `nodes/01_write_positions_mtm_duckdb.py`: write latest/history/run-log into DuckDB.
- `nodes/02_sync_positions_mtm_to_rows.py`: optional recovery sync from DuckDB to Google Sheets rows.
- `sql/schema.sql`: schema reference.
