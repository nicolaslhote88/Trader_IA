import ast
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[5]
MATRIX = Path(__file__).resolve().parents[1] / "nodes/pre_agent/calcul_matrice_briefing.code.py"
DASHBOARD = ROOT.parent / "services/dashboard/app.py"


def formula(path, action, confidence):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    targets = {"tech_direction", "tech_strength", "tech_prob"}
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t,ast.Name) and t.id in targets for t in n.targets)]
    assert len(nodes)==3
    ns={"tech_action":action,"tech_conf":confidence}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),"exec"),ns)
    return ns["tech_prob"]


@pytest.mark.parametrize("confidence",[0,16.7,50,66.7,100])
def test_direction_confidence_and_dashboard_parity(confidence):
    dashboard = next(p / "services/dashboard/app.py" for p in MATRIX.parents if (p / "services/dashboard/app.py").exists())
    buy=formula(MATRIX,"BUY",confidence);sell=formula(MATRIX,"SELL",confidence)
    assert buy>=50 and sell<=50 and buy+sell==pytest.approx(100)
    assert formula(MATRIX,"NEUTRAL",confidence)==50
    assert formula(dashboard,"BUY",confidence)==buy
    assert formula(dashboard,"SELL",confidence)==sell
