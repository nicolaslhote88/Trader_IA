import sys
import unittest
from pathlib import Path

import pandas as pd


DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from app_modules.ag2_funnel import build_ag2_operational_scope


NOW = pd.Timestamp("2026-08-13T22:00:00Z")


def _signal(
    symbol: str,
    *,
    age_hours: float = 12.0,
    decision: str = "SKIP",
    call_ai: bool = False,
    d1_action: str = "NEUTRAL",
    h1_status: str = "OK",
    d1_status: str = "OK",
    closed: bool = True,
) -> dict:
    return {
        "symbol": symbol,
        "workflow_date": NOW - pd.Timedelta(hours=age_hours),
        "data_age_h1_hours": 4.0,
        "data_age_d1_hours": 20.0,
        "h1_status": h1_status,
        "d1_status": d1_status,
        "h1_closed_only": closed,
        "d1_closed_only": closed,
        "d1_action": d1_action,
        "ai_decision": decision,
        "call_ai": call_ai,
        "ai_quality": 6 if decision == "APPROVE" else 3,
    }


class Ag2OperationalFunnelTests(unittest.TestCase):
    def setUp(self):
        self.universe = pd.DataFrame(
            [
                {"symbol": "AAA", "asset_class": "EQUITY", "segments": "CORE_AUTO", "quarantine_active": False},
                {"symbol": "BBB", "asset_class": "EQUITY", "segments": "WATCHLIST", "quarantine_active": False},
                {"symbol": "CCC", "asset_class": "EQUITY", "segments": "WATCHLIST", "quarantine_active": False},
                {"symbol": "DDD", "asset_class": "EQUITY", "segments": "WATCHLIST", "quarantine_active": False},
                {"symbol": "QUA", "asset_class": "EQUITY", "segments": "", "quarantine_active": True},
                {"symbol": "FX:EURUSD", "asset_class": "FX", "segments": "", "quarantine_active": False},
            ]
        )
        self.signals = pd.DataFrame(
            [
                _signal("AAA", decision="APPROVE", call_ai=True, d1_action="BUY"),
                _signal("BBB", decision="REJECT", call_ai=True, d1_action="SELL"),
                _signal("CCC", decision="SKIP", call_ai=False, d1_action="NEUTRAL"),
                _signal("DDD", age_hours=120.0, decision="APPROVE", call_ai=True, d1_action="BUY"),
                _signal("QUA", age_hours=500.0, decision="APPROVE", call_ai=True, d1_action="BUY"),
                _signal("FX:EURUSD", decision="APPROVE", call_ai=True, d1_action="BUY"),
            ]
        )

    def test_stages_are_monotonic_and_use_the_ag1_contract(self):
        _, metrics = build_ag2_operational_scope(self.signals, self.universe, now_utc=NOW)

        self.assertEqual(6, metrics["universe_total"])
        self.assertEqual(4, metrics["rotation_active"])
        self.assertEqual(3, metrics["tech_ready"])
        self.assertEqual(2, metrics["ai_not_rejected"])
        self.assertGreaterEqual(metrics["universe_total"], metrics["rotation_active"])
        self.assertGreaterEqual(metrics["rotation_active"], metrics["tech_ready"])
        self.assertGreaterEqual(metrics["tech_ready"], metrics["ai_not_rejected"])

    def test_approve_is_not_required_and_reject_is_the_only_ai_block(self):
        _, metrics = build_ag2_operational_scope(self.signals, self.universe, now_utc=NOW)

        self.assertEqual(2, metrics["ai_not_rejected"])
        self.assertEqual(2, metrics["ai_calls_ready"])
        self.assertEqual(1, metrics["ai_approve_ready"])
        self.assertEqual(1, metrics["ai_reject_ready"])

    def test_effective_age_uses_the_worse_of_stored_and_real_age(self):
        signals = pd.DataFrame([_signal("AAA", age_hours=120.0, decision="SKIP")])
        universe = self.universe[self.universe["symbol"] == "AAA"]

        scoped, metrics = build_ag2_operational_scope(signals, universe, now_utc=NOW)

        self.assertEqual(0, metrics["tech_ready"])
        self.assertEqual(120.0, scoped.iloc[0]["ag2_h1_age_hours_effective"])
        self.assertEqual(120.0, scoped.iloc[0]["ag2_d1_age_hours_effective"])

    def test_old_approve_outside_rotation_never_enters_funnel_end(self):
        _, metrics = build_ag2_operational_scope(self.signals, self.universe, now_utc=NOW)

        self.assertEqual(1, metrics["ai_approve_ready"])
        self.assertNotEqual(4, metrics["ai_approve_ready"])

    def test_rotated_symbol_without_signal_is_visible_and_blocked(self):
        missing = pd.DataFrame(
            [{"symbol": "MISS", "asset_class": "EQUITY", "segments": "WATCHLIST", "quarantine_active": False}]
        )
        universe = pd.concat([self.universe, missing], ignore_index=True)

        _, metrics = build_ag2_operational_scope(self.signals, universe, now_utc=NOW)

        self.assertEqual(7, metrics["universe_total"])
        self.assertEqual(6, metrics["signals_latest"])
        self.assertEqual(5, metrics["rotation_active"])
        self.assertEqual(3, metrics["tech_ready"])
        self.assertEqual(2, metrics["tech_blocked"])


if __name__ == "__main__":
    unittest.main()
