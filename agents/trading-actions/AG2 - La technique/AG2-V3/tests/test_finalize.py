import sys
import textwrap
import types
import unittest
from pathlib import Path


class FakeConnection:
    def __init__(self):
        self.batch_state = {}
        self._fetchone = None

    def execute(self, sql, params=None):
        params = params or []
        normalized = " ".join(str(sql).split()).upper()
        if "INSERT OR REPLACE INTO BATCH_STATE" in normalized:
            self.batch_state[str(params[0])] = int(params[1])
        elif "SELECT VALUE FROM BATCH_STATE" in normalized:
            value = self.batch_state.get(str(params[0]))
            self._fetchone = (value,) if value is not None else None
        return self

    def fetchone(self):
        return self._fetchone

    def close(self):
        return None


def run_finalize(items):
    fake_duckdb = types.ModuleType("duckdb")
    fake_duckdb.connect = lambda *_args, **_kwargs: FakeConnection()
    sys.modules["duckdb"] = fake_duckdb
    source_path = Path(__file__).resolve().parents[1] / "nodes" / "10_finalize.py"
    source = source_path.read_text(encoding="utf-8")
    wrapped = "def node_main(_items):\n" + textwrap.indent(source, "    ")
    namespace = {}
    exec(compile(wrapped, str(source_path), "exec"), namespace)
    return namespace["node_main"](items)


class FinalizeTests(unittest.TestCase):
    def test_complete_failure_is_visible_to_n8n(self):
        items = [{"json": {
            "run_id": "test_run",
            "batch_info": {"size": 1},
            "_status": "error",
            "symbol": "AIR.PA",
            "error": "boom",
        }}]
        with self.assertRaisesRegex(RuntimeError, "AG2_RUN_FAILED"):
            run_finalize(items)

    def test_success_still_returns_summary(self):
        items = [{"json": {
            "run_id": "test_run",
            "batch_info": {"size": 1, "state_key": "test", "next_index": 1},
            "_status": "ok",
            "symbol": "AIR.PA",
        }}]
        result = run_finalize(items)
        self.assertEqual("SUCCESS", result[0]["json"]["status"])
        self.assertEqual(1, result[0]["json"]["symbols_ok"])
        self.assertTrue(result[0]["json"]["cursor_advanced"])
        self.assertEqual(1, result[0]["json"]["batch_next_index"])

    def test_missing_batch_context_is_a_visible_failure(self):
        items = [{"json": {
            "run_id": "test_run",
            "_status": "ok",
            "symbol": "AIR.PA",
        }}]
        with self.assertRaisesRegex(RuntimeError, "AG2_CURSOR_GUARD_FAILED: BATCH_INFO_MISSING"):
            run_finalize(items)

    def test_processed_count_must_match_batch_size(self):
        items = [{"json": {
            "run_id": "test_run",
            "batch_info": {"size": 2, "state_key": "test", "next_index": 2},
            "_status": "ok",
            "symbol": "AIR.PA",
        }}]
        with self.assertRaisesRegex(RuntimeError, "BATCH_PROCESSED_MISMATCH:1!=2"):
            run_finalize(items)

    def test_partial_batch_advances_after_all_items_are_accounted_for(self):
        batch_info = {"size": 2, "state_key": "test", "next_index": 2}
        items = [
            {"json": {
                "run_id": "test_run",
                "batch_info": batch_info,
                "_status": "ok",
                "symbol": "AIR.PA",
            }},
            {"json": {
                "run_id": "test_run",
                "batch_info": batch_info,
                "_status": "error",
                "symbol": "BROKEN.PA",
                "error": "injected",
            }},
        ]
        result = run_finalize(items)
        self.assertEqual("PARTIAL", result[0]["json"]["status"])
        self.assertTrue(result[0]["json"]["cursor_advanced"])
        self.assertEqual(2, result[0]["json"]["batch_next_index"])


if __name__ == "__main__":
    unittest.main()
