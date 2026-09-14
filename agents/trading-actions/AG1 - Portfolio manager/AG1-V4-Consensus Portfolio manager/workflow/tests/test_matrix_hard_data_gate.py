import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "nodes/pre_agent/calcul_matrice_briefing.code.py").read_text(encoding="utf-8-sig")
WRAPPED = "def _run_node(_items):\n" + "".join("    " + line for line in SOURCE.splitlines(keepends=True))
NAMESPACE = {}
exec(compile(WRAPPED, "calcul_matrice_briefing.code.py", "exec"), NAMESPACE)
RUN_NODE = NAMESPACE["_run_node"]


def strong_row(symbol: str, flags: str = "") -> dict:
    return {
        "Symbol": symbol,
        "Symbol_Yahoo": symbol,
        "Name": symbol,
        "AssetClass": "EQUITY",
        "Currency": "EUR",
        "Sector": "Technology",
        "Tech_Action": "BUY",
        "Tech_Confidence": 90,
        "Last_Close": 100,
        "D1_ATR_Pct": 2,
        "D1_Resistance": 110,
        "D1_Support": 95,
        "D1_Dist_Res_Pct": 10,
        "D1_Dist_Sup_Pct": 5,
        "AI_Stop_Loss": 95,
        "AI_Decision": "APPROVE",
        "AI_Quality": 9,
        "Funda_Score": 85,
        "Funda_Risk": 15,
        "Funda_Upside": 20,
        "Funda_Usable": True,
        "Regular_Market_Price": 100,
        "Bid": 99.9,
        "Ask": 100.1,
        "SpreadPct": 0.2,
        "SlippageProxyPct": 0.05,
        "Volume": 1_000_000,
        "IV_ATM": 0.2,
        "Options_Ok": True,
        "Days_To_Earnings": 30,
        "Data_Age_H1_Hours": 120 if "STALE_H1" in flags else 1,
        "Data_Age_D1_Hours": 2,
        "Data_Quality_Flags": flags,
        "Data_OK_For_Trading": not bool(flags),
    }


class MatrixHardDataGateTests(unittest.TestCase):
    def matrix_row(self, row: dict) -> dict:
        output = RUN_NODE([{"json": row}])[0]["json"]
        return output["opportunity_pack"]["rows"][0]

    def test_fresh_strong_setup_can_enter(self):
        row = self.matrix_row(strong_row("FRESH"))
        self.assertEqual("Entrer / Renforcer", row["decision"])
        self.assertTrue(row["data_ok_for_trading"])

    def test_stale_h1_forces_watch_even_with_strong_scores(self):
        row = self.matrix_row(strong_row("STALE", "STALE_H1"))
        self.assertEqual("Surveiller", row["decision"])
        self.assertIn("STALE_H1", row["gates"])
        self.assertIn("HARD_DATA_GATE", row["action_reason"])
        self.assertFalse(row["data_ok_for_trading"])


if __name__ == "__main__":
    unittest.main()
