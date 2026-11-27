import streamlit as st
from . import cockpit, economic_monitor, forward_campaign, research_sim, strategy_trials, verified_account
from .pilotage_data import context

PAGES = {"Cockpit", "Mon compte", "Coûts", "Test 6 mois", "Enseignements", "Univers et simulateur", "Pilote et retraits", "Chaîne"}
from . import account_view, costs_view, chain_health, lab_learnings, lab_mandate, lab_pilot, lab_universe
READERS = {"Mon compte":account_view.render, "Coûts":costs_view.render,
           "Test 6 mois":lab_mandate.render, "Enseignements":lab_learnings.render,
           "Univers et simulateur":lab_universe.render, "Pilote et retraits":lab_pilot.render, "Chaîne":chain_health.render}


def details(page, d, url):
    def available(key, callback, *args, **kwargs):
        if d[key]:
            callback(*args, **kwargs)
        else:
            st.caption("Publication indisponible.")
    with st.expander("Détails et preuves", expanded=False):
        if page == "Mon compte":
            available("account", verified_account.render, url)
            available("economic", economic_monitor.render, "Rendements comparés")
        elif page == "Coûts":
            option = st.radio("Détail des coûts", economic_monitor.VIEWS[:2], key="cost_detail", horizontal=True)
            available("economic", economic_monitor.render, option)
        elif page == "Enseignements":
            available("experiments", strategy_trials.render)
        elif page == "Test 6 mois":
            option = st.radio("Preuve de simulation", ["Mandat formel", "Portefeuilles simulés"], key="mandate_detail", horizontal=True)
            if option == "Mandat formel":
                available("campaign", forward_campaign.render, "13 · Observation")
            else:
                available("research", research_sim.render, option)
        elif page == "Univers et simulateur":
            option = st.radio("Dossier de recherche", research_sim.VIEWS[:2], key="universe_detail", horizontal=True)
            available("research", research_sim.render, option)
        elif page == "Pilote et retraits":
            option = st.radio("Dossier", ["Pilote", "Composants"], key="pilot_detail", horizontal=True)
            available("campaign", forward_campaign.render, "14 · Pilote" if option == "Pilote" else "15 · Composants")
        else:
            available("account", verified_account.render, url, compact=True)
            available("economic", economic_monitor.health)
            available("research", research_sim.health)
            available("experiments", strategy_trials.health)
            available("campaign", forward_campaign.health)


@st.fragment(run_every="60s")
def render(page, url):
    if st.session_state.get("dashboard_nav_page", "Cockpit") != page:
        st.rerun(scope="app")
    d = context(url)
    if page == "Cockpit":
        cockpit.render(d)
        return
    st.title({"Test 6 mois":"Test de 6 mois", "Chaîne":"Chaîne de bout en bout"}.get(page, page))
    if page in READERS:
        READERS[page](d)
    details(page, d, url)
