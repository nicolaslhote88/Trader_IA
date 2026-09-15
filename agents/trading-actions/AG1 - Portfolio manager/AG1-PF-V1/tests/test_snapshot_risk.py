import unittest
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from test_ibkr_symbol_identity import load_node_namespace


class SnapshotRiskTests(unittest.TestCase):
    def setUp(self):
        self.risk = load_node_namespace()["measured_snapshot_risk"]
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA core")
        self.con.execute("CREATE TABLE core.portfolio_snapshot(ts TIMESTAMPTZ, total_value_eur DOUBLE)")
        self.con.execute("CREATE TABLE core.cash_ledger(ts TIMESTAMPTZ, type VARCHAR, currency VARCHAR, amount DOUBLE)")
        self.con.execute("CREATE TABLE core.instruments(symbol VARCHAR, sector VARCHAR)")
        self.con.execute("INSERT INTO core.instruments VALUES ('EXM', 'Technology')")

    def tearDown(self):
        self.con.close()

    def test_previous_zoned_snapshot_and_booked_flows(self):
        # DuckDB's Python TIMESTAMPTZ decoder requires optional pytz. These
        # fixtures also run in the actual n8n runner image where pytz is absent.
        self.con.execute("INSERT INTO core.portfolio_snapshot VALUES ('2026-09-11 23:15:00+02', 1000), ('2026-09-14 08:00:00+00', 1040)")
        self.con.execute("INSERT INTO core.cash_ledger VALUES ('2026-09-14 07:00:00+00','DEPOSIT','EUR',100), ('2026-09-14 08:00:00+00','WITHDRAWAL','EUR',-20), ('2026-09-14 08:00:00+00','DIVIDEND','EUR',10), ('2026-09-14 08:00:00+00','AI_COST','EUR',-2), ('2026-09-15 00:00:00+00','DEPOSIT','EUR',500)")
        row = self.risk(self.con, "2026-09-14T10:15:00+00:00", 990, 90, [[None, None, "EXM", 1, 1, 1, 900]])
        self.assertAlmostEqual(row["drawdown"], 990 / 1040 - 1)
        self.assertAlmostEqual(row["meta"]["daily_return_pct"], -9)
        ref = datetime.fromisoformat(row["meta"]["daily_reference_at"])
        self.assertEqual(ref, datetime(2026, 9, 11, 21, 15, tzinfo=timezone.utc))
        self.assertAlmostEqual(row["top_sector"], 900 / 990)
        self.assertEqual(row["booked_ai"], 2)

    def test_missing_previous_day_is_explicitly_unknown(self):
        self.con.execute("INSERT INTO core.portfolio_snapshot VALUES ('2026-09-14 08:00:00+00',1000)")
        row = self.risk(self.con, "2026-09-14T10:15:00+00:00", 990, 990, [])
        self.assertIsNone(row["meta"]["daily_return_pct"])
        self.assertIsNone(row["meta"]["daily_reference_at"])
        self.assertEqual(row["status"], "DEFENSIVE")

    def test_zero_previous_nav_is_not_divided(self):
        self.con.execute("INSERT INTO core.portfolio_snapshot VALUES ('2026-09-11 21:15:00+00',0)")
        row = self.risk(self.con, "2026-09-14T10:15:00+00:00", 1000, 1000, [])
        self.assertIsNone(row["meta"]["daily_return_pct"])
        self.assertEqual(row["drawdown"], 0)

    def test_zoned_snapshot_without_optional_pytz(self):
        code = """import builtins, sys, unittest
original_import = builtins.__import__
def no_pytz(name, *args, **kwargs):
    if name == 'pytz' or name.startswith('pytz.'):
        raise ModuleNotFoundError("No module named 'pytz'")
    return original_import(name, *args, **kwargs)
builtins.__import__ = no_pytz
sys.path.insert(0, sys.argv[1])
from test_snapshot_risk import SnapshotRiskTests
result = unittest.TextTestRunner().run(unittest.TestSuite([
    SnapshotRiskTests('test_previous_zoned_snapshot_and_booked_flows')]))
sys.exit(0 if result.wasSuccessful() else 1)
"""
        result = subprocess.run([sys.executable, "-c", code, str(Path(__file__).parent)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
