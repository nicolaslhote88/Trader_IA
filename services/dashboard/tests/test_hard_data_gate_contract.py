import unittest
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


class HardDataGateContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = APP_PATH.read_text(encoding="utf-8")

    def test_effective_bar_age_uses_real_and_stored_age(self):
        self.assertIn("h1_age_hours_effective", self.source)
        self.assertIn("d1_age_hours_effective", self.source)
        self.assertIn('hard_data_flags.append("STALE_H1")', self.source)
        self.assertIn('hard_data_flags.append("STALE_D1")', self.source)

    def test_hard_data_gate_blocks_enter_action(self):
        self.assertIn("or hard_data_gate_block", self.source)
        self.assertIn('reasons.append("HARD_DATA_GATE")', self.source)

    def test_closed_bar_contract_is_loaded(self):
        for column in ("h1_status", "d1_status", "h1_closed_only", "d1_closed_only"):
            self.assertIn(f'"{column}"', self.source)


if __name__ == "__main__":
    unittest.main()
