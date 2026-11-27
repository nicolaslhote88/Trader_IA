import json
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

app=Path(sys.argv[1]).resolve()
sys.path[:0]=[str(app.parent),"/app"]
from app_modules.forward_campaign import MENU, REPORT

report=json.loads(REPORT.read_bytes())
ui=AppTest.from_file(str(app),default_timeout=120).run()
assert not ui.exception
ui.button(key="nav_Test 6 mois").click().run()
assert any(x.label=="Détails et preuves" for x in ui.expander)
assert not ui.exception and not ui.error
assert str(report["formal"]["sessions"]) in {m.value for m in ui.metric}
ui.button(key="nav_Pilote et retraits").click().run()
for v in ("Pilote","Composants"):
    ui.radio(key="pilot_detail").set_value(v).run()
    assert not ui.exception and not ui.error
    if v == "Composants":
        assert len(ui.dataframe[0].value)==len(report["retirement"])
    print(v,flush=True)
ui.button(key="nav_Chaîne").click().run()
assert not ui.exception and any("Lot 13" in x.value for x in ui.caption)
print("État du système",flush=True)
