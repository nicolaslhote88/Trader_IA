import plotly.graph_objects as go
import streamlit as st
from .ui_kit import COLORS, GREEN, GREY, RED, chart, date_fr, fmt, metrics, verdict_card


def render(d):
    data=d["campaign"]
    p=data.get("pilot",{})
    if not p:
        verdict_card("Le registre du pilote est indisponible.")
        return
    blocks=p.get("blockers",[])
    criteria=[("Variante qualifiée",bool(p["eligible"]) if "eligible" in p else None),
              ("Mandat réel nominatif",False if any("mandat" in v.lower() for v in blocks) else True if p.get("real_capital") else None),
              ("Compte réel aligné",False if d["signals"]["Compte réel"].state==RED else d["live"][1]=="Compte réel connecté"),
              ("Capital compatible avec les commissions",None if p.get("real_capital") is None else p.get("commission_compatible"))]
    passed=sum(v is True for _,v in criteria)
    verdict_card(f"**{passed} sur 4** conditions d’entrée sont confirmées. Le registre du pilote indique « {p.get('state','indisponible')} ».")
    metrics([("Variantes qualifiées",fmt(len(p['eligible']) if 'eligible' in p else None,digits=0)),("Capital réel attribué",fmt(p.get('real_capital')," €")),("Collecte",date_fr(data.get("observed")))])
    for col,(title,v) in zip(st.columns(4),criteria):
        col.markdown(f'<span style="color:{COLORS[GREEN if v is True else RED if v is False else GREY]}">●</span> **{title}**',unsafe_allow_html=True)
        col.caption("Confirmé" if v is True else "Condition à remplir" if v is False else "Non mesurable")
    rows=sorted((r for r in data.get("retirement",[]) if r.get("calls") is not None),key=lambda r:r["calls"])
    st.subheader("Appels IA observés par workflow")
    if rows:
        chart(go.Figure(go.Bar(x=[r["calls"] for r in rows],y=[r["name"] for r in rows],orientation="h",marker_color="#4ea1ff",
              customdata=[[r.get("state"),fmt(r.get("saving")," €")] for r in rows],
              hovertemplate="%{y} · %{x} appels<br>%{customdata[0]} · économie facturée %{customdata[1]}<extra></extra>")),max(300,24*len(rows)))
    st.caption("Seules les économies facturées publiées peuvent être chiffrées.")
