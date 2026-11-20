import json
from pathlib import Path
import sys
from streamlit.testing.v1 import AppTest
p=Path(sys.argv[1]).resolve();sys.path.insert(0,str(p.parent))
from app_modules.economic_monitor import VIEWS,REPORT
d=json.loads(REPORT.read_bytes())
t=AppTest.from_file(str(p),default_timeout=150).run()
assert not t.exception,[x.message for x in t.exception]
for v in VIEWS:
    t.radio(key="portfolio_read_view").set_value(v).run()
    assert not t.exception,[x.message for x in t.exception]
    assert t.dataframe and not t.error
    if v==VIEWS[0]:assert str(d["orders"]["count"]) in [m.value for m in t.metric]
    if v==VIEWS[1]:assert any("226 003" in str(x.value) for x in t.markdown)
    if v==VIEWS[2]:assert any(d["performance"]["end"] in x.value for x in t.subheader)
    print("Vérifié",v,len(t.dataframe),flush=True)
