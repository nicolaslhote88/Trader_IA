import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app_modules.core import calculate_sector_portfolio_tilts


class SectorPortfolioTiltsTests(unittest.TestCase):
    def test_net_conflicting_evidence_before_ranking(self):
        sec_df = pd.DataFrame(
            [
                {"sector": "Energy", "direction": "Bullish", "score": 120},
                {"sector": "Energy", "direction": "Bearish", "score": 80},
                {"sector": "Technology", "direction": "Bullish", "score": 30},
                {"sector": "Technology", "direction": "Bearish", "score": 70},
                {"sector": "Financial Services", "direction": "Bullish", "score": 60},
            ]
        )

        tilts = calculate_sector_portfolio_tilts(sec_df)

        self.assertEqual(
            tilts,
            {
                "overweight": ["Financial Services", "Energy"],
                "underweight": ["Technology"],
            },
        )
        self.assertTrue(set(tilts["overweight"]).isdisjoint(tilts["underweight"]))

    def test_omit_exactly_balanced_or_invalid_rows(self):
        sec_df = pd.DataFrame(
            [
                {"sector": "Energy", "direction": "Bullish", "score": 50},
                {"sector": "Energy", "direction": "Bearish", "score": 50},
                {"sector": "Industrials", "direction": "Neutral", "score": 999},
                {"sector": "", "direction": "Bullish", "score": 100},
            ]
        )

        self.assertEqual(
            calculate_sector_portfolio_tilts(sec_df),
            {"overweight": [], "underweight": []},
        )


if __name__ == "__main__":
    unittest.main()
