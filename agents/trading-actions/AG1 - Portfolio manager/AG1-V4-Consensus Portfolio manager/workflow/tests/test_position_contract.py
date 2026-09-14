import ast
from pathlib import Path
from datetime import datetime, timezone
import math
import pytest

SOURCE = Path(__file__).resolve().parents[1] / "nodes/pre_agent/4C_enrich_portfolio_with_market_prices.code.py"
tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and not n.decorator_list]
ns = {"datetime": datetime, "timezone": timezone, "math": math}
exec(compile(ast.Module(body=functions,type_ignores=[]), str(SOURCE), "exec"), ns)


def fill(side, qty, day, **extra):
    return {"symbol":"EXM", "side":side,"qty":qty,"ts_ms":int(datetime(2026,1,day,tzinfo=timezone.utc).timestamp()*1000),"price_native":100, "fill_id":str(day),"stop_loss":-5,**extra}


def test_fill_lifecycle_partial_exit_and_reopen():
    rows = [fill("BUY",10,1), fill("BUY",5,2), fill("SELL",3,3)]
    record = ns["lifecycle_from_fills"](rows)["EXM"]
    assert record["netQty"] == 12 and record["openedAt"].startswith("2026-01-01")
    assert record["openingStopPriceNative"] == 95
    record = ns["lifecycle_from_fills"](rows+[fill("SELL",12,4),fill("BUY",2,5)])["EXM"]
    assert record["netQty"] == 2 and record["openedAt"].startswith("2026-01-05")
    assert "LEFT JOIN core.fills" not in SOURCE.read_text(encoding="utf-8")


def test_native_current_fx_and_paid_eur_cost_are_separate():
    ref = {"currency":"USD", "fx":.8,"quantity":10,"updatedAt":"2026-01-05T10:00:00Z","avg_eur_at_current_fx":80,"lp_eur":88}
    life = {"costBasisQty":10,"paidCostEUR":910}
    row = ns["position_currency_contract"](ref,life,10,"2026-01-05T10:00:00Z",880,80,88)
    assert row["avgPriceNative"] == 100 and row["lastPriceNative"] == 110
    assert row["perfLocalPct"] == pytest.approx(10)
    assert row["perfEURPct"] == pytest.approx((880/910-1)*100)
    old = ns["position_currency_contract"](ref,life,11,"2026-01-05T10:00:00Z",880,80,88)
    assert old["avgPriceNative"] is None and old["paidCostEUR"] is None
