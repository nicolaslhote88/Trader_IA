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
ui.radio(key="portfolio_read_view").set_value(MENU).run()
assert not ui.exception and not ui.error
assert str(report["formal"]["sessions"]) in {m.value for m in ui.metric}
for v in ui.radio(key="forward_lot").options:
    ui.radio(key="forward_lot").set_value(v).run()
    assert not ui.exception and not ui.error
    if v.startswith("15"):
        assert len(ui.dataframe[0].value)==len(report["retirement"])
    print(v,flush=True)
ui.selectbox(key="dashboard_nav_section").set_value("Pilotage global").run()
assert not ui.exception and any("Lot 13" in x.value for x in ui.caption)
print("État du système",flush=True)
