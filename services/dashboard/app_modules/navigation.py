"""Navigation métier du dashboard Streamlit.

Les libellés visibles restent indépendants des identifiants historiques de
pages afin de pouvoir clarifier l'interface sans toucher au routage interne.
"""

from __future__ import annotations

from typing import Final


NAV_SECTIONS: Final[dict[str, dict[str, object]]] = {
    "Pilotage global": {
        "description": "Santé du système, décisions et contexte transverse.",
        "pages": (
            ("État du système", "System Health (Monitoring)"),
            ("Décisions & opportunités", "Vue consolidee Multi-Agents"),
            ("Contexte de marché", "Contexte global"),
            ("Macro & actualités", "Macro & News (AG4)"),
        ),
    },
    "Actions / ETF": {
        "description": "Portefeuille réel et analyses par valeur.",
        "pages": (
            ("Portefeuille actions", "Dashboard Trading"),
            ("Analyse technique", "Analyse Technique V2"),
            ("Analyse fondamentale", "Analyse Fondamentale V2"),
        ),
    },
    "Forex": {
        "description": "Suivi du marché Forex — exécution désactivée.",
        "pages": (
            ("Portefeuille Forex", "Dashboard Forex"),
            ("Contexte & piliers", "Three Pillars Monitor"),
        ),
    },
}

DEFAULT_NAV_SECTION: Final[str] = "Actions / ETF"


def page_labels(section: str) -> tuple[str, ...]:
    """Retourne les libellés visibles d'une section."""
    return tuple(label for label, _ in NAV_SECTIONS[section]["pages"])


def resolve_page(section: str, label: str) -> str:
    """Résout un libellé métier vers l'identifiant historique de page."""
    for visible_label, page_id in NAV_SECTIONS[section]["pages"]:
        if visible_label == label:
            return page_id
    raise KeyError(f"Page inconnue pour {section!r}: {label!r}")


def _section_key(section: str) -> str:
    return (
        section.lower()
        .replace(" / ", "_")
        .replace(" ", "_")
        .replace("é", "e")
    )


def _render_navigation_styles(st) -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] .sidebar-brand {
            padding: .25rem 0 .15rem 0;
            font-size: 1.45rem;
            font-weight: 750;
            letter-spacing: -.02em;
            color: #f8fafc;
        }
        [data-testid="stSidebar"] .sidebar-brand span {
            color: #f5c76b;
        }
        [data-testid="stSidebar"] .sidebar-subtitle {
            margin: 0 0 1.25rem 0;
            color: #94a3b8;
            font-size: .82rem;
        }
        [data-testid="stSidebar"] .sidebar-section-label {
            margin: 1.15rem 0 .35rem 0;
            color: #94a3b8;
            font-size: .72rem;
            font-weight: 700;
            letter-spacing: .09em;
            text-transform: uppercase;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] {
            gap: .3rem;
        }
        [data-testid="stSidebar"] label[data-baseweb="radio"] {
            width: 100%;
            min-height: 2.35rem;
            margin: 0;
            padding: .55rem .7rem;
            border: 1px solid transparent;
            border-radius: .55rem;
            transition: background-color .12s ease, border-color .12s ease;
        }
        [data-testid="stSidebar"] label[data-baseweb="radio"]:hover {
            background: rgba(148, 163, 184, .10);
            border-color: rgba(148, 163, 184, .16);
        }
        [data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked) {
            background: rgba(239, 68, 68, .14);
            border-color: rgba(248, 113, 113, .38);
        }
        [data-testid="stSidebar"] label[data-baseweb="radio"] > div:first-child {
            display: none;
        }
        [data-testid="stSidebar"] label[data-baseweb="radio"] p {
            font-size: .92rem;
            line-height: 1.2;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_navigation() -> tuple[str, str]:
    """Affiche la navigation et retourne ``(section, page_id)``."""
    import streamlit as st

    with st.sidebar:
        _render_navigation_styles(st)
        st.markdown(
            '<div class="sidebar-brand">TradingSim <span>AI</span></div>'
            '<div class="sidebar-subtitle">Centre de pilotage</div>',
            unsafe_allow_html=True,
        )

        sections = tuple(NAV_SECTIONS)
        section = st.selectbox(
            "Espace",
            sections,
            index=sections.index(DEFAULT_NAV_SECTION),
            key="dashboard_nav_section",
        )
        st.caption(str(NAV_SECTIONS[section]["description"]))
        st.markdown('<div class="sidebar-section-label">Vues</div>', unsafe_allow_html=True)

        labels = page_labels(section)
        selected_label = st.radio(
            "Vue",
            labels,
            key=f"dashboard_nav_page_{_section_key(section)}",
            label_visibility="collapsed",
        )

    return section, resolve_page(section, selected_label)
