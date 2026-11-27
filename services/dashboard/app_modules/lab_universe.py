from collections import Counter
import plotly.graph_objects as go
import streamlit as st
from . import research_sim
from .ui_kit import chart, date_fr, fmt, metrics, verdict_card


def render(d):
    rep=d["research"]
    st.caption("Simulation · aucun argent réel")
    revisions=rep.get("census") or []
    if not revisions:
        verdict_card("La photographie de l’univers de recherche est indisponible.")
        return
    frame=revisions[-1]
    counts=frame["counts"]
    verdict_card(f"Sur **{fmt(counts.get('sources'),digits=0)}** entrées sources, **{fmt(counts.get('pre'),digits=0)}** sont présélectionnées et **{fmt(counts.get('paper'),digits=0)}** achetables en simulation à l’heure de la photographie.")
    replay=rep.get("replay",{})
    metrics([("Couverture du rejeu",f"{fmt(replay.get('complete_packs'),digits=0)} / {fmt(replay.get('ledger_runs'),digits=0)}"),("Lignes détenues",fmt(counts.get("held"),digits=0)),("Photographie",date_fr(frame.get("at")))])
    chart(go.Figure(go.Funnel(y=["Entrées sources","Dans le périmètre","Présélection","Achats simulables"],x=[counts.get(k) for k in ("sources","scoped","pre","paper")],marker_color=["#6c757d","#4ea1ff","#c084fc","#4ade80"])))
    try:
        rows=research_sim.read(frame["file"])
    except (OSError,ValueError,KeyError):
        st.caption("Répartition géographique indisponible.")
        return
    countries=Counter(r.get("country") or "Indéterminé" for r in rows if r.get("scoped"))
    ranked=sorted(countries.items(),key=lambda v:v[1])
    st.subheader("Pays des titres dans le périmètre")
    chart(go.Figure(go.Bar(x=[n for k,n in ranked],y=[k for k,n in ranked],orientation="h",marker_color="#4ea1ff")),max(260,20*len(ranked)))
    st.caption("Nombre de titres par pays dans la même photographie.")
