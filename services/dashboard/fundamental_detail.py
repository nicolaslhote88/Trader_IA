"""Fundamental detail UI backed by the same evidence object sent to AG1."""
import json
import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from fundamental_context import load_fundamental_context, FUNDAMENTAL_LEGEND, FUNDAMENTAL_METHOD

LABELS = {"quality": "Qualité", "growth": "Croissance", "valuation": "Valorisation",
          "health": "Santé financière", "consensus": "Consensus"}
METRICS = {"revenue_growth_pct": "Croissance du chiffre d’affaires", "earnings_growth_pct": "Croissance du bénéfice",
           "operating_margin_pct": "Marge opérationnelle", "net_margin_pct": "Marge nette", "roe_pct": "Rentabilité des capitaux propres",
           "forward_pe": "P/E prévisionnel", "trailing_pe": "P/E historique", "debt_equity_pct": "Dette / capitaux propres",
           "current_ratio": "Ratio de liquidité générale", "fcf_yield_pct": "Rendement du flux de trésorerie libre"}


@st.cache_data(ttl=120)
def load_detail(path, signature, symbol):
    card = load_fundamental_context(path, [symbol]).get(symbol, {})
    with duckdb.connect(path, read_only=True) as con:
        history = con.execute("""SELECT updated_at, score, risk_score, quality_score, growth_score,
            valuation_score, health_score, consensus_score, target_price, current_price,
            analyst_count, strategy_version FROM fundamentals_triage_history
            WHERE symbol=? AND updated_at >= CURRENT_TIMESTAMP - INTERVAL '120 days'
            ORDER BY updated_at DESC LIMIT 1000""", [symbol]).df().sort_values("updated_at")
    return card, history


def render_fundamental_detail(symbol, row, path, signature, fetch_history):
    card, history = load_detail(path, signature, symbol)
    if not card:
        st.info("Fiche fondamentale indisponible.")
        return
    st.subheader(f"{symbol} — {row.get('name', '')}")
    cols = st.columns(4)
    cols[0].metric("Triage fondamental", f"{row.get('score', 0):.0f}/100")
    cols[1].metric("Risque fondamental", f"{row.get('risk_score', 0):.0f}/100")
    cols[2].metric("Métriques disponibles", f"{card['metric_coverage_pct']:.0f} %")
    cols[3].metric("Âge du relevé", f"{card['age_h']:.0f} h" if card['age_h'] is not None else "Inconnu")
    st.caption("Le risque fondamental décrit le bilan et la rentabilité ; il ne mesure pas la probabilité de baisse du cours. Les scores sont des indicateurs de triage, sans calibration prédictive.")
    st.caption(f"Source : Yahoo Finance via AG3 · Relevé : {card['observed_at']} · Devise : {card['currency'] or 'inconnue'}")
    st.info("Les périodes comptables et dates de publication ne sont pas fournies par ce relevé. Une collecte récente ne signifie pas que les comptes viennent d’être publiés.")
    serious = [f for f in card['flags'] if f in ['SOURCE_ERROR', 'STALE_OR_INVALID_DATE', 'SNAPSHOT_UNAVAILABLE', 'LOW_METRIC_COVERAGE', 'LEGACY_SCORE_METHOD']]
    if serious:
        st.warning("Qualité à vérifier : " + ", ".join({"SOURCE_ERROR": "erreur de source", "STALE_OR_INVALID_DATE": "relevé ancien ou date invalide", "SNAPSHOT_UNAVAILABLE": "détail source indisponible", "LOW_METRIC_COVERAGE": "couverture faible", "LEGACY_SCORE_METHOD": "ancienne méthode de calcul"}[flag] for flag in serious))

    st.markdown("#### Les cinq dimensions")
    for col, (key, label) in zip(st.columns(5), LABELS.items()):
        value = card['scores'].get(key)
        col.metric(label, f"{value:.0f}/100" if value is not None else "Indisponible")
    left, right = st.columns(2)
    left.write("**Dimensions favorables selon les seuils de triage :** " + (", ".join(LABELS[k] for k in card['strong_dimensions']) or "Aucune dimension ≥70/100"))
    right.write("**Dimensions fragiles selon les seuils de triage :** " + (", ".join(LABELS[k] for k in card['weak_dimensions']) or "Aucune dimension <40/100"))
    st.caption("Une dimension à 50 peut résulter d’un manque de données. Lire les métriques et la couverture avant d’interpréter un score.")
    peer = card['sector_reference']
    if peer['triage_percentile'] is not None:
        st.caption(f"Référence du secteur : triage supérieur ou égal à {peer['triage_percentile']:.0f} % des {peer['n']} autres titres comparables par méthode, fraîcheur et couverture. Comparaison de scores, pas estimation de juste valeur.")
    else:
        st.caption(f"Comparaison sectorielle non concluante : {peer['n']} autres titres admissibles ; minimum 8.")

    st.markdown("#### Les chiffres derrière les scores")
    metric_rows = []
    for key, label in METRICS.items():
        value = card['metrics'].get(key)
        unit = '%' if key.endswith('_pct') else 'x'
        metric_rows.append({"Indicateur": label, "Valeur": f"{value:,.2f} {unit}" if value is not None else "Indisponible", "Période comptable": "Non fournie"})
    st.dataframe(pd.DataFrame(metric_rows), hide_index=True, width='stretch')
    if card['missing']:
        st.caption("Données manquantes : " + ", ".join(METRICS.get(k, k) for k in card['missing']))

    st.markdown("#### Ce qui a changé")
    changes = []
    for window, delta in card['changes'].items():
        target = delta.get('target_revision_pct') if delta else None
        score = delta.get('score_delta') if delta else None
        changes.append({"Comparaison": window, "Relevé de référence": delta['baseline_at'] if delta else "Indisponible",
                        "Révision de l’objectif moyen": f"{target:+.2f} %" if target is not None else "Indisponible",
                        "Variation du triage": f"{score:+.0f} points" if score is not None else ("Méthodes différentes" if delta and not delta['same_method'] else "Indisponible")})
    st.dataframe(pd.DataFrame(changes), hide_index=True, width='stretch')
    days = st.selectbox("Fenêtre d’observation", [30, 90], key="funda_detail_days")
    h = history.copy()
    if not h.empty:
        h = h[h.updated_at >= pd.Timestamp.now(tz='UTC').tz_localize(None) - pd.Timedelta(days=days)]
    if not h.empty:
        fig = go.Figure()
        for version, group in h.groupby('strategy_version', dropna=False):
            corrected = version == FUNDAMENTAL_METHOD
            for key, label in LABELS.items():
                fig.add_trace(go.Scatter(x=group.updated_at, y=group[key + '_score'], mode='lines+markers',
                    name=label + (" · corrigé" if corrected else " · historique"),
                    line=dict(dash='solid' if corrected else 'dot'), connectgaps=False))
        fig.update_layout(height=350, yaxis=dict(title="Score /100", range=[0, 100]), margin=dict(t=20, b=20))
        st.plotly_chart(fig, width='stretch')
        st.caption("Chaque point est un relevé, pas une publication de résultats. Les fondamentaux évoluent lentement ; arrondis et seuils peuvent laisser un score constant. Les méthodes différentes ne sont pas reliées.")
        ranges = [{"Dimension": label, "Minimum": h[key+'_score'].min(), "Maximum": h[key+'_score'].max()} for key,label in LABELS.items()]
        with st.expander("Amplitude observée et relevés"):
            st.dataframe(pd.DataFrame(ranges), hide_index=True, width='stretch')
            st.dataframe(h.sort_values('updated_at', ascending=False), hide_index=True, width='stretch')

    st.markdown("#### Consensus et objectifs analystes")
    targets = card['analyst_targets']
    cols = st.columns(4)
    cols[0].metric("Analystes", f"{card['analysts']:.0f}" if card['analysts'] is not None else "Indisponible")
    for col, key, label in zip(cols[1:], ['low','mean','high'], ['Objectif bas','Objectif moyen','Objectif haut']):
        value = targets.get(key)
        col.metric(label, f"{value:.2f} {card['currency'] or ''}" if value is not None else "Indisponible")
    st.caption("Objectifs individuels agrégés, horizon indicatif 12 mois. Le plus bas n’est pas une borne de perte ; aucun pourcentage de réalisation n’a été validé.")
    if 'THIN_OR_MISSING_CONSENSUS' in card['flags']:
        st.warning("Consensus absent ou peu fourni : moins de trois analystes connus.")
    prices = fetch_history(symbol, interval='1d', lookback_days=365)
    if prices is not None and not prices.empty and 'close' in prices.columns:
        prices = prices.sort_values('time')
        fig = go.Figure(go.Scatter(x=prices.time, y=prices.close, name="Cours observé", line=dict(color='#a0aabb')))
        for key, label, color in [('low','Objectif bas','#dc3545'),('mean','Objectif moyen','#ffc107'),('high','Objectif haut','#28a745')]:
            if targets.get(key) is not None:
                fig.add_hline(y=targets[key], line_dash='dot', line_color=color, annotation_text=f"{label} : {targets[key]:.2f}")
        fig.update_layout(height=350, yaxis_title=card['currency'] or "Cours", margin=dict(t=20,b=20))
        st.plotly_chart(fig, width='stretch')
        st.caption("Cours historique et niveaux des objectifs actuels. Les lignes d’objectifs ne constituent pas des trajectoires prédites.")
    with st.expander("Fiche factuelle transmise à AG1"):
        st.json(card)
        st.caption("Cette même fiche accompagne les titres retenus dans le pack AG1. Les scores déjà intégrés à la matrice ne doivent pas être comptés deux fois.")
    st.caption("Modèle prédictif : non validé. Les probabilités artificielles ont été retirées ; l’évaluation exige des données datées et des résultats futurs observables.")
