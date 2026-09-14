import ast
import json
import tempfile
import unittest
from pathlib import Path

import duckdb


NODE_PATH = Path(__file__).resolve().parents[1] / "nodes" / "00c_reconcile_ibkr_ledger.py"


def load_node_namespace():
    source = NODE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(NODE_PATH))
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if "items" in names:
                break
        selected.append(node)
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(NODE_PATH), "exec"), namespace)
    return namespace


class IbkrSymbolIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ns = load_node_namespace()

    def test_exchange_mapping_never_infers_paris_from_eur_only(self):
        self.ns["IBKR_SYMBOL_BY_CONID"] = {}
        normalize = self.ns["ibkr_internal_symbol"]
        self.assertEqual(normalize({"symbol": "PRX", "currency": "EUR", "exchange": ""}), "PRX")
        self.assertEqual(normalize({"symbol": "PRX", "currency": "EUR", "exchange": "AEB"}), "PRX.AS")
        self.assertEqual(normalize({"symbol": "AI", "currency": "EUR", "exchange": "SBF"}), "AI.PA")

    def test_persisted_fill_conid_restores_canonical_order_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.duckdb")
            con = duckdb.connect(db_path)
            con.execute("CREATE SCHEMA core")
            con.execute(
                """
                CREATE TABLE core.orders (
                    order_id VARCHAR, run_id VARCHAR, ts_created TIMESTAMP, symbol VARCHAR,
                    side VARCHAR, qty DOUBLE, limit_price DOUBLE, status VARCHAR,
                    broker_order_id VARCHAR, rationale_json VARCHAR
                )
                """
            )
            con.execute("CREATE TABLE core.fills (order_id VARCHAR, raw_fill_json JSON)")
            con.execute(
                "INSERT INTO core.orders VALUES ('o1','r1',CURRENT_TIMESTAMP,'PRX.AS','BUY',26,39.355,'FILLED','1184124543','{}')"
            )
            con.execute(
                "INSERT INTO core.fills VALUES ('o1', ?)",
                [json.dumps({"source": "ibkr_pf_reconcile", "ibkrFill": {"conid": 382625193, "listing_exchange": "AEB"}})],
            )
            resolved, conflicts = self.ns["build_conid_symbol_map"](con, [])
            con.close()

        self.assertEqual(conflicts, {})
        self.assertEqual(resolved[382625193], "PRX.AS")
        self.ns["IBKR_SYMBOL_BY_CONID"] = resolved
        self.assertEqual(
            self.ns["ibkr_internal_symbol"](
                {"symbol": "PRX", "currency": "EUR", "exchange": "", "conid": 382625193}
            ),
            "PRX.AS",
        )


if __name__ == "__main__":
    unittest.main()
