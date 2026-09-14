import ast
import unittest
from pathlib import Path
from typing import Any


BROKER_DIR = Path(__file__).resolve().parents[1]
SOURCE = (BROKER_DIR / "app.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
SELECTED = [
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name == "_contract_exchanges"
]
NAMESPACE = {"Any": Any}
exec(
    compile(ast.Module(body=SELECTED, type_ignores=[]), str(BROKER_DIR / "app.py"), "exec"),
    NAMESPACE,
)
contract_exchanges = NAMESPACE["_contract_exchanges"]


class ContractExchangeIdentityTests(unittest.TestCase):
    def test_derivative_sections_do_not_impersonate_stock_listing(self):
        contract = {
            "conid": "382625193",
            "companyHeader": "PROSUS NV - AEB",
            "symbol": "PRX",
            "description": "AEB",
            "sections": [
                {"secType": "STK"},
                {"secType": "WAR", "exchange": "FWB;GETTEX;SBF;SWB"},
                {"secType": "CFD", "exchange": "SMART"},
            ],
        }
        self.assertEqual(contract_exchanges(contract), {"AEB"})

    def test_stock_section_exchange_is_kept(self):
        contract = {
            "companyHeader": "EXAMPLE SA",
            "symbol": "EXM",
            "sections": [{"secType": "STK", "exchange": "SBF;ENEXT"}],
        }
        self.assertEqual(contract_exchanges(contract), {"SBF", "ENEXT"})

    def test_known_suffixes_are_forced_off_smart_fallback(self):
        self.assertIn("strict_suffix_exchange", SOURCE)
        self.assertIn('if strict_suffix_exchange and wanted_exchange == "SMART"', SOURCE)
        self.assertIn("if not strict_suffix_exchange:", SOURCE)


if __name__ == "__main__":
    unittest.main()
