import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
import pandas as pd
import main


class HistoryBackfillTests(unittest.TestCase):
    def test_growth_transfer_aliases_preserve_requested_identity(self):
        self.assertEqual(("PVL.PA", "ALPVL.PA"), main._symbol_request_context("PVL.PA"))
        self.assertEqual(
            ("LHYFE.PA", "ALHYF.PA"), main._symbol_request_context("LHYFE.PA")
        )
        self.assertEqual(("ABB", "ABB"), main._symbol_request_context("ABB"))
        self.assertEqual(("ROG.SW", "ROG.SW"), main._symbol_request_context("ROG.SW"))

    def frame(self, days):
        return pd.DataFrame(
            [
                dict(Datetime=t, Open=10, High=12, Low=9, Close=11, Volume=100)
                for t in pd.date_range("2026-09-01", periods=days, tz="UTC")
            ]
        )

    def invoke(self, cache, fetched, meta=None):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            main, "DATA_DIR", directory
        ), patch.object(main, "CACHE_DIR", directory), patch.object(
            main, "STATE_DIR", directory
        ), patch.object(
            main, "_utcnow", return_value=datetime(2026, 10, 6, 6, tzinfo=timezone.utc)
        ), patch.object(
            main, "_read_cache", return_value=(cache, meta or {})
        ), patch.object(
            main, "_is_cache_stale", return_value=False
        ), patch.object(
            main, "_load_symbol_state", return_value={}
        ), patch.object(
            main, "_global_rate_limit_sleep"
        ), patch.object(
            main, "_clear_cooldown"
        ), patch.object(
            main, "_download_incremental", return_value=fetched
        ) as download, patch.object(
            main, "_write_cache"
        ) as save, patch.object(
            main, "_atomic_write"
        ) as metadata:
            result = main.history(
                symbol="TEST",
                interval="1d",
                lookback_days=400,
                max_bars=400,
                min_bars=20,
                allow_stale=False,
                force_refresh=False,
                exchange="NYSE",
                asset_class="EQUITY",
                closed_only=True,
                validated_only=True,
            )
            return result, download.call_args, save.call_args, metadata.call_args

    def test_recent_short_cache_backfills_the_requested_history(self):
        result, download, saved, _ = self.invoke(self.frame(5), self.frame(30))
        self.assertTrue(result["ok"])
        self.assertEqual(30, result["count"])
        self.assertEqual(datetime(2025, 9, 1, 6, tzinfo=timezone.utc), download.args[2])
        self.assertGreater(saved.args[3]["lastBackfillTs"], 0)

    def test_insufficient_recently_backfilled_cache_fails_closed(self):
        result, download, _, _ = self.invoke(
            self.frame(5), self.frame(30), {"lastBackfillTs": main.time.time()}
        )
        self.assertFalse(result["ok"])
        self.assertIsNone(download)
        self.assertTrue(result["closedOnly"])
        self.assertIn("NOT_ENOUGH_BARS", result["error"])

    def test_empty_backfill_keeps_insufficient_cache_blocked_and_records_attempt(self):
        import json

        result, _, _, metadata = self.invoke(self.frame(5), pd.DataFrame())
        self.assertFalse(result["ok"])
        self.assertGreater(json.loads(metadata.args[1])["lastBackfillTs"], 0)

    def test_sufficient_cache_does_not_redownload(self):
        result, download, _, _ = self.invoke(self.frame(30), self.frame(30))
        self.assertTrue(result["ok"])
        self.assertIsNone(download)

    def test_short_upstream_keeps_quality_contract_and_daily_backfill_marker(self):
        result, _, saved, _ = self.invoke(self.frame(5), self.frame(10))
        self.assertFalse(result["ok"])
        self.assertTrue(result["closedOnly"])
        self.assertEqual(10, result["count"])
        self.assertGreater(saved.args[3]["lastBackfillTs"], 0)


if __name__ == "__main__":
    unittest.main()
