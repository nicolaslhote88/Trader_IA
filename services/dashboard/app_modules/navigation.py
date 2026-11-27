"""Navigation métier vers les pages historiques et les publications du VPS."""
NAV_SECTIONS = {
    "Pilotage": {"pages": (("Cockpit", "Cockpit"), ("Mon compte", "Mon compte"), ("Coûts", "Coûts"), ("Décisions du jour", "Dashboard Trading"))},
    "Laboratoire": {"pages": (("Test de 6 mois", "Test 6 mois"), ("Enseignements", "Enseignements"), ("Univers et simulateur", "Univers et simulateur"), ("Pilote et retraits", "Pilote et retraits"))},
    "Santé du système": {"pages": (("Chaîne de bout en bout", "Chaîne"), ("Tradabilité", "System Health (Monitoring)"))},
    "Marchés & analyses": {"pages": (("Contexte de marché", "Contexte global"), ("Macro & actualités", "Macro & News (AG4)"), ("Analyse technique", "Analyse Technique V2"), ("Analyse fondamentale", "Analyse Fondamentale V2"), ("Décisions & opportunités", "Vue consolidee Multi-Agents"))},
    "Forex": {"pages": (("Portefeuille Forex", "Dashboard Forex"), ("Contexte & piliers", "Three Pillars Monitor"))},
}
DEFAULT_NAV_SECTION = "Pilotage"
DEFAULT_PAGE = "Cockpit"


def page_labels(section):
    return tuple(label for label, page in NAV_SECTIONS[section]["pages"])


def resolve_page(section, label):
    return dict(NAV_SECTIONS[section]["pages"])[label]


def select_page(page):
    import streamlit as st
    st.session_state["dashboard_nav_page"] = page


def render_sidebar_navigation(url):
    import streamlit as st
    from .pilotage_data import broker

    @st.fragment(run_every="30s")
    def connection():
        if broker(url)[1] != "Compte réel connecté":
            st.markdown('<a href="?vue=Cockpit" target="_self" style="display:block;padding:.35rem .65rem;border:1px solid rgba(248,113,113,.38);border-radius:.5rem;background:rgba(239,68,68,.14);color:#ff7b85;text-align:center;text-decoration:none">Compte réel non connecté</a>', unsafe_allow_html=True)

    current = st.session_state.get("dashboard_nav_page", DEFAULT_PAGE)
    with st.sidebar:
        st.markdown("""<style>
        [data-testid="stSidebar"] {background:#262730}
        [data-testid="stSidebar"] .stButton button {justify-content:flex-start;min-height:1.65rem;padding:.1rem .65rem;border:1px solid transparent;background:transparent}
        [data-testid="stSidebar"] button[kind="primary"] {background:rgba(239,68,68,.14);border-color:rgba(248,113,113,.38)}
        [data-testid="stSidebar"] [class*="st-key-nav_"] {margin-bottom:-.6rem}
        [data-testid="stSidebar"] .st-key-broker_alert button {color:#ff7b85;font-weight:700}
        </style><strong style="font-size:1.4rem">TradingSim <span style="color:#f5c76b">AI</span></strong>""", unsafe_allow_html=True)
        connection()
        section = DEFAULT_NAV_SECTION
        for group, config in NAV_SECTIONS.items():
            st.caption(group)
            for label, page in config["pages"]:
                if page == current:
                    section = group
                st.button(label, key="nav_" + page, width="stretch", type="primary" if page == current else "secondary",
                          on_click=select_page, args=(page,))
    return section, current
