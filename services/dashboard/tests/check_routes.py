"""Recette AppTest des routes, utilisable en shadow et sur le montage de production."""
import re
import sys
import time
from pathlib import Path
from streamlit.testing.v1 import AppTest

app=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(app.parent))
from app_modules.navigation import NAV_SECTIONS
from app_modules.pilotage_pages import PAGES


def verify(ui, name):
    assert not ui.exception, (name,[e.message for e in ui.exception])
    errors = [e.value for e in ui.error]
    if name == "Three Pillars Monitor":
        # Cette vue historique affiche les alertes de positionnement CFTC avec st.error.
        alerts = [v for v in errors if re.fullmatch(r"[A-Z]{3}: z=-?\d+\.\d{2} \| net=-?[\d,]+(?:\.\d+)?", v)]
        if alerts:
            print("Alertes historiques de positionnement Forex", len(alerts), flush=True)
        errors = [v for v in errors if v not in alerts]
    assert not errors, (name, errors)


if __name__ == "__main__":
    started=time.monotonic()
    ui=AppTest.from_file(str(app),default_timeout=200).run()
    verify(ui,"Cockpit")
    assert ui.title[0].value=="Cockpit"
    print(f"Cockpit prêt en {time.monotonic()-started:.2f} s", flush=True)
    for section in NAV_SECTIONS.values():
        for label,page in section["pages"]:
            ui.button(key="nav_"+page).click().run()
            verify(ui,page)
            if page in PAGES:
                assert any(x.label=="Détails et preuves" for x in ui.expander),page
            print("OK",label,flush=True)
