"""Vérifie les trois vues économiques depuis leur nouvelle navigation."""
import json
import sys
from pathlib import Path
from streamlit.testing.v1 import AppTest

app=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(app.parent))
from app_modules.economic_monitor import REPORT, VIEWS

data=json.loads(REPORT.read_bytes())
ui=AppTest.from_file(str(app),default_timeout=150).run()
ui.button(key="nav_Coûts").click().run()
for view in VIEWS:
    if view==VIEWS[2]:
        ui.button(key="nav_Mon compte").click().run()
    else:
        ui.radio(key="cost_detail").set_value(view).run()
    proof=next(e for e in ui.expander if e.label=="Détails et preuves")
    assert not ui.exception and not ui.error and proof.dataframe,view
    if view==VIEWS[0]:
        assert str(data["orders"]["count"]) in [m.value for m in proof.metric]
    if view==VIEWS[2]:
        assert any(data["performance"]["end"] in h.value for h in proof.subheader)
    print(view,flush=True)
