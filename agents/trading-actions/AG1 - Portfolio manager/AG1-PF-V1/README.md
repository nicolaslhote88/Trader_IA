# AG1-PF-V1

DuckDB extension for the AG1 V4 portfolio MTM workflow.

Files:
- `AG1-PF-V1-workflow.json`: AG1 V4 n8n workflow ready to import.
- `build_workflow.py`: workflow generator (rebuild JSON from node source files).
- `docs/INTEGRATION.md`: n8n wiring and migration notes.
- `nodes/01_write_positions_mtm_duckdb.py`: transactionally write latest/history/run-log into DuckDB and fail on partial writes.
- `nodes/02_sync_positions_mtm_to_rows.py`: optional recovery sync from DuckDB to Google Sheets rows.
- `nodes/03_normalize_positions.js`: normalize portfolio rows before pricing calls.
- `nodes/05a_wrap_1d.js`: wrap 1D price payload.
- `nodes/05b_wrap_1h.js`: wrap 1H price payload.
- `nodes/07_compute_mtm.js`: compute MTM values and diagnostics.
- `nodes/08_build_sheet_updates.js`: build sheet updates + DuckDB payload.
- `nodes/09_filter_universe_columns.js`: keep only required Universe fields.
- `sql/schema.sql`: schema reference.
