import ast
import unittest
from pathlib import Path


BROKER_DIR = Path(__file__).resolve().parents[1]
SOURCE = (BROKER_DIR / "app.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
SELECTED = [
    node
    for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name in {"_price_increment_for", "normalize_limit_price"}
]
NAMESPACE = {}
MODULE = ast.fix_missing_locations(
    ast.Module(
        body=[
            ast.ImportFrom(
                module="decimal",
                names=[
                    ast.alias(name="Decimal"),
                    ast.alias(name="InvalidOperation"),
                    ast.alias(name="ROUND_CEILING"),
                    ast.alias(name="ROUND_FLOOR"),
                ],
                level=0,
            ),
            *SELECTED,
        ],
        type_ignores=[],
    )
)
exec(
    compile(
        MODULE,
        str(BROKER_DIR / "app.py"),
        "exec",
    ),
    NAMESPACE,
)
normalize_limit_price = NAMESPACE["normalize_limit_price"]


class PriceNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.rules = {
            "increment": 0.01,
            "incrementRules": [
                {"lowerEdge": 0, "increment": 0.01},
                {"lowerEdge": 10, "increment": 0.02},
                {"lowerEdge": 20, "increment": 0.05},
            ],
        }

    def test_buy_is_rounded_down_to_active_tick(self):
        normalized, metadata = normalize_limit_price(16.99, self.rules, "BUY")
        self.assertEqual(16.98, normalized)
        self.assertEqual(0.02, metadata["increment"])
        self.assertEqual("FLOOR", metadata["side_rounding"])

    def test_sell_is_rounded_up_without_worsening_limit(self):
        normalized, metadata = normalize_limit_price(17.15, self.rules, "SELL")
        self.assertEqual(17.16, normalized)
        self.assertEqual("CEILING", metadata["side_rounding"])

    def test_tier_changes_at_lower_edge(self):
        normalized, metadata = normalize_limit_price(20.03, self.rules, "BUY")
        self.assertEqual(20.0, normalized)
        self.assertEqual(0.05, metadata["increment"])


if __name__ == "__main__":
    unittest.main()
