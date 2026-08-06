from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from global_context_tab import _query, _redact, load_global_context_data


def test_missing_databases_are_reported_without_crash(tmp_path):
    data = load_global_context_data(
        str(tmp_path / "missing-global.duckdb"),
        str(tmp_path / "missing-world.duckdb"),
        str(tmp_path / "missing-macro.duckdb"),
    )
    assert data["snapshot"].empty
    assert data["ag9_events"].empty
    assert data["ag5"].empty
    assert data["errors"]["snapshot"] == "BASE_ABSENTE"


def test_worldmonitor_secret_is_never_rendered():
    raw = "HTTP failure for wm_live_abcdefghijklmnopqrstuvwxyz"
    rendered = _redact(raw)
    assert "wm_live_" not in rendered
    assert "[REDACTED]" in rendered


def test_empty_database_and_partial_schema_are_diagnostic_not_exceptions(tmp_path):
    path = tmp_path / "partial.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE present(id INTEGER)")
    frame, error = _query(str(path), "SELECT * FROM present")
    assert frame.empty and error is None
    frame, error = _query(str(path), "SELECT * FROM missing")
    assert frame.empty and "missing" in error.lower()


def test_locked_database_exhausts_bounded_retries_without_crash(tmp_path):
    path = tmp_path / "locked.duckdb"
    writer = duckdb.connect(str(path))
    writer.execute("CREATE TABLE t(id INTEGER)")
    try:
        frame, error = _query(str(path), "SELECT * FROM t", retries=2)
        assert isinstance(frame, pd.DataFrame)
        # DuckDB/OS combinations either allow the read or return a visible lock error.
        assert error is None or "lock" in error.lower() or "configuration" in error.lower()
    finally:
        writer.close()


def test_large_and_long_history_queries_remain_bounded(tmp_path):
    path = tmp_path / "volume.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE history AS SELECT i AS id, TIMESTAMP '2026-01-01' + i * INTERVAL 1 MINUTE AS ts FROM range(10000) t(i)")
    frame, error = _query(str(path), "SELECT * FROM history ORDER BY ts DESC LIMIT 200")
    assert error is None
    assert len(frame) == 200
    assert frame.iloc[0]["id"] == 9999
