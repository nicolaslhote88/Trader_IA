import ast
import json
import tempfile
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "nodes/post_agent/10_post_run_health.code.py"
TREE = ast.parse(SOURCE_PATH.read_text(encoding="utf-8-sig"))
FUNCTION = next(
    node for node in TREE.body if isinstance(node, ast.FunctionDef) and node.name == "_insert_chronic_alerts"
)
MODULE = ast.fix_missing_locations(ast.Module(body=[FUNCTION], type_ignores=[]))
NAMESPACE = {"json": json}
exec(compile(MODULE, str(SOURCE_PATH), "exec"), NAMESPACE)
INSERT_CHRONIC_ALERTS = NAMESPACE["_insert_chronic_alerts"]


class ChronicAlertDetectionTests(unittest.TestCase):
    def test_legacy_agent_warning_is_normalized_and_detected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "alerts.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA core")
                con.execute(
                    """
                    CREATE TABLE core.alerts (
                      alert_id VARCHAR PRIMARY KEY,
                      run_id VARCHAR,
                      ts TIMESTAMP,
                      severity VARCHAR,
                      category VARCHAR,
                      symbol VARCHAR,
                      message VARCHAR,
                      code VARCHAR,
                      payload_json JSON
                    )
                    """
                )
                for index, run_id in enumerate(("RUN_1", "RUN_2", "RUN_3"), start=1):
                    con.execute(
                        """
                        INSERT INTO core.alerts
                        VALUES (?, ?, CURRENT_TIMESTAMP - (? * INTERVAL '1 day'), 'WARN', 'AGENT', 'ORDER',
                                'ORDER_REJECT:MIN_ORDER_VALUE_EUR:CRM:714<1000', 'AGENT_WARNING', '{}')
                        """,
                        [f"ALT_{index}", run_id, 3 - index],
                    )

                inserted = INSERT_CHRONIC_ALERTS(con, "RUN_3")
                self.assertEqual(1, inserted)
                row = con.execute(
                    """
                    SELECT code, symbol, payload_json
                    FROM core.alerts
                    WHERE code = 'CHRONIC_AGENT_WARNING'
                    """
                ).fetchone()
                self.assertEqual("CHRONIC_AGENT_WARNING", row[0])
                self.assertEqual("CRM", row[1])
                payload = json.loads(row[2]) if isinstance(row[2], str) else row[2]
                self.assertEqual("MIN_ORDER_VALUE_EUR", payload["reason_code"])
                self.assertEqual(3, payload["distinct_runs"])
                self.assertEqual(8, payload["window_days"])
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
