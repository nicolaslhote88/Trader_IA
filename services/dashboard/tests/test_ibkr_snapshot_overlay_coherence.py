import ast
from pathlib import Path
import unittest

import pandas as pd


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _load_functions(*names):
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"))
    wanted = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    assert {node.name for node in wanted} == set(names)
    namespace = {"pd": pd, "safe_float": _safe_float}
    exec(compile(ast.Module(body=wanted, type_ignores=[]), str(APP_PATH), "exec"), namespace)
    return namespace


def _position_row(*, qty, updated_at, market_value=325.88, unrealized=-39.64):
    return pd.DataFrame(
        [
            {
                "symbol": "AVGO",
                "quantity": qty,
                "avgprice": 365.52,
                "lastprice": market_value / qty,
                "marketvalue": market_value,
                "unrealizedpnl": unrealized,
                "updatedat": updated_at,
                "currency": "USD",
                "fx_rate": 0.8632,
            }
        ]
    )


def _ibkr_row(*, qty, updated_at, market_value=978.06, unrealized=-118.50):
    return pd.DataFrame(
        [
            {
                "symbol": "AVGO",
                "quantity": qty,
                "avg_cost_eur": 365.52,
                "last_price_eur": market_value / qty,
                "market_value_eur": market_value,
                "unrealized_pnl_eur": unrealized,
                "currency": "USD",
                "fx_rate": 0.8632,
                "updated_at": updated_at,
            }
        ]
    )


class IbkrSnapshotOverlayCoherenceTests(unittest.TestCase):
    def test_post_trade_snapshot_is_not_overwritten_by_pre_trade_ibkr_overlay(self):
        ns = _load_functions(
            "_ag1_ibkr_row_is_coherent_with_snapshot",
            "_ag1_apply_ibkr_live_overlay",
        )
        now = pd.Timestamp.now(tz="UTC")
        snapshot = _position_row(qty=1.0, updated_at=now, market_value=325.88)
        ibkr = _ibkr_row(qty=3.0, updated_at=now - pd.Timedelta(minutes=20), market_value=978.06)
        ns["_ag1_fetchdf"] = lambda _conn, query: ibkr.copy() if "portfolio_positions_ibkr_latest" in query else pd.DataFrame()

        result, diag = ns["_ag1_apply_ibkr_live_overlay"](object(), snapshot)

        self.assertAlmostEqual(result.iloc[0]["quantity"], 1.0)
        self.assertAlmostEqual(result.iloc[0]["marketvalue"], 325.88)
        self.assertEqual(diag["source_counts"], {"recon_snapshot_quantity_mismatch": 1})

    def test_newer_quantity_coherent_ibkr_overlay_is_applied(self):
        ns = _load_functions(
            "_ag1_ibkr_row_is_coherent_with_snapshot",
            "_ag1_apply_ibkr_live_overlay",
        )
        now = pd.Timestamp.now(tz="UTC")
        snapshot = _position_row(qty=3.0, updated_at=now - pd.Timedelta(minutes=2), market_value=960.0)
        ibkr = _ibkr_row(qty=3.0, updated_at=now - pd.Timedelta(minutes=1), market_value=978.06)
        ns["_ag1_fetchdf"] = lambda _conn, query: ibkr.copy() if "portfolio_positions_ibkr_latest" in query else pd.DataFrame()

        result, diag = ns["_ag1_apply_ibkr_live_overlay"](object(), snapshot)

        self.assertAlmostEqual(result.iloc[0]["marketvalue"], 978.06)
        self.assertAlmostEqual(result.iloc[0]["unrealizedpnl"], -118.50)
        self.assertEqual(diag["source_counts"], {"ibkr_live": 1})

    def test_fx_breakdown_keeps_newer_snapshot_monetary_values(self):
        ns = _load_functions(
            "_ag1_ibkr_row_is_coherent_with_snapshot",
            "_ag1_attach_fx_breakdown",
        )
        now = pd.Timestamp.now(tz="UTC")
        snapshot = _position_row(qty=1.0, updated_at=now, market_value=325.88, unrealized=-39.64)

        class Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class Connection:
            def execute(self, query):
                if "FROM portfolio_positions_ibkr_latest" in query:
                    return Cursor(
                        [("AVGO", "USD", 0.8632, 365.52, 326.02, 978.06, -118.50, 3.0, now - pd.Timedelta(minutes=20))]
                    )
                return Cursor([])

        result = ns["_ag1_attach_fx_breakdown"](Connection(), snapshot)

        self.assertAlmostEqual(result.iloc[0]["mktval_eur"], 325.88)
        self.assertAlmostEqual(result.iloc[0]["pnl_prix_eur"], -39.64)


if __name__ == "__main__":
    unittest.main()
