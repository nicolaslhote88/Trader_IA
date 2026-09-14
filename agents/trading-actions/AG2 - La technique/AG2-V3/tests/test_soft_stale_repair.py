import importlib.util
import tempfile
import unittest
from pathlib import Path

import duckdb


def load_repair_module():
    path = Path(__file__).resolve().parents[5] / "outils" / "scripts" / "repair_ag2_soft_stale_status.py"
    spec = importlib.util.spec_from_file_location("repair_ag2_soft_stale_status", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


REPAIR = load_repair_module()


class SoftStaleRepairTests(unittest.TestCase):
    def test_apply_and_rollback_only_touch_soft_stale_latest_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "ag2_test.duckdb")
            con = duckdb.connect(db_path)
            con.execute(
                """
                CREATE TABLE technical_signals (
                  id VARCHAR PRIMARY KEY,
                  symbol VARCHAR,
                  workflow_date TIMESTAMP,
                  updated_at TIMESTAMP,
                  h1_date TIMESTAMP,
                  d1_date TIMESTAMP,
                  data_age_h1_hours DOUBLE,
                  data_age_d1_hours DOUBLE,
                  h1_closed_only BOOLEAN,
                  d1_closed_only BOOLEAN,
                  h1_status VARCHAR,
                  d1_status VARCHAR,
                  h1_warnings VARCHAR,
                  data_quality_flags VARCHAR,
                  filter_reason VARCHAR
                )
                """
            )
            con.execute(
                """
                INSERT INTO technical_signals VALUES
                  ('soft|A', 'A', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                   CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_TIMESTAMP - INTERVAL '30 hours',
                   4, 30, TRUE, TRUE, 'STALE', 'OK',
                   '["H1 data is 4.0h old - STALE"]', '["STALE_H1"]', 'NO_H1_DATA'),
                  ('hard|B', 'B', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                   CURRENT_TIMESTAMP - INTERVAL '100 hours', CURRENT_TIMESTAMP - INTERVAL '30 hours',
                   100, 30, TRUE, TRUE, 'STALE', 'OK',
                   '["H1 data is 100.0h old - STALE"]', '["STALE_H1"]', 'NO_H1_DATA'),
                  ('ok|C', 'C', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                   CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP - INTERVAL '30 hours',
                   2, 30, TRUE, TRUE, 'OK', 'OK', '[]', '[]', 'OK')
                """
            )

            targets = REPAIR._rows_as_dicts(con, REPAIR.TARGET_SQL)
            self.assertEqual(["A"], [row["symbol"] for row in targets])
            self.assertEqual(1, REPAIR.apply_repair(con, targets))

            repaired = con.execute(
                "SELECT h1_status, h1_warnings, data_quality_flags, filter_reason FROM technical_signals WHERE symbol='A'"
            ).fetchone()
            self.assertEqual("OK", repaired[0])
            self.assertEqual("[]", repaired[1])
            self.assertIn("H1_OUTSIDE_AI_FRESHNESS_WINDOW", repaired[2])
            self.assertEqual("H1_OR_D1_OUTSIDE_AI_FRESHNESS_WINDOW", repaired[3])
            self.assertEqual(0, len(REPAIR._rows_as_dicts(con, REPAIR.TARGET_SQL)))

            self.assertEqual(1, REPAIR.rollback(con))
            restored = con.execute(
                "SELECT h1_status, h1_warnings, data_quality_flags, filter_reason FROM technical_signals WHERE symbol='A'"
            ).fetchone()
            self.assertEqual("STALE", restored[0])
            self.assertIn("old - STALE", restored[1])
            self.assertIn("STALE_H1", restored[2])
            self.assertEqual("NO_H1_DATA", restored[3])
            con.close()


if __name__ == "__main__":
    unittest.main()
