from statistics import mean
import plotly.graph_objects as go
import streamlit as st
from .ui_kit import chart, date_fr, fmt, pct, verdict_card
from .navigation import select_page


def render(d):
    r=d["experiments"]
    if not r:
        verdict_card("Les résultats des expériences sont indisponibles.")
        return
    ex,news,historic = r.get("exits",{}),r.get("news",{}),r.get("historical_votes",{})
    votes={(v["variant"],v["side"]):v for v in historic.get("rows",[])}
    buy=votes.get(("Consensus approuvé","BUY"),{}).get("signed_mean")
    single=votes.get(("Premier avis écarté par le consensus","BUY"),{}).get("signed_mean")
    sells=ex.get("mean_excess")
    deltas=[v for row in ex.get("rows",[]) if (v:=row.get("arms",{}).get("réduire",{}).get("delta")) is not None]
    reducer=mean(deltas) if deltas else None
    m=news.get("metrics",{})
    verdict_card(("Réduire les positions a coûté en moyenne **"+fmt(-reducer," €")+"** face à leur conservation." if reducer is not None and reducer<0 else "Les expériences comparent les ventes, la couverture des actualités et les votes.")+" L’avantage du consensus reste à établir.")
    st.caption("Simulation · aucun argent réel engagé dans les essais de portefeuille · observations du "+date_fr(r.get("at")))
    a,b,c=st.columns(3)
    with a:
        st.caption("Défavorable" if reducer is not None and reducer<0 else "À confirmer")
        st.subheader("Vendre ou conserver")
        chart(go.Figure(go.Bar(x=["Titres achetés","Titres vendus"],y=[None if v is None else 100*v for v in (buy,sells)],marker_color=["#4ea1ff","#fd7e14"])),210)
        st.write("Écart moyen à l’ETF, vingt séances après la décision. Réduire au lieu de conserver · "+fmt(reducer," €")+" par dossier chiffré.")
        st.caption(f"{ex.get('evaluated','—')} ventes évaluables · {len(deltas)} différences de politique chiffrées. Les périodes se recouvrent.")
    with b:
        st.caption("À confirmer")
        st.subheader("Actualités par événement")
        chart(go.Figure(go.Bar(x=["Articles","Événements"],y=[news.get("raw"),news.get("events")],marker_color=["#6c757d","#4ea1ff"])),210)
        cov=next((c for c in m.get("coverage",[]) if c.get("segment")=="DÉTENUS FLEX"),{})
        st.write(f"{fmt(news.get('raw'),digits=0)} articles regroupés en {fmt(news.get('events'),digits=0)} événements. Couverture des détenus · {cov.get('new','—')} sur {cov.get('n','—')}.")
        st.caption("Détenus non couverts · "+(", ".join(cov["missing"]) or "aucun" if "missing" in cov else "indisponible")+f". Événements matériels manqués · {m.get('missed','—')}. Faux positifs · {m.get('false_positive','—')}.")
    with c:
        st.caption("Non démontré")
        st.subheader("Valeur du vote à trois IA")
        gap=(buy-single)*100 if None not in (buy,single) else None
        fig=go.Figure(go.Scatter(x=[gap],y=["Achats"],mode="markers",marker=dict(size=12,color="#f5c76b")))
        fig.add_vline(x=0,line_color="#9aa0a6")
        chart(fig,210)
        st.write("Consensus moins vote isolé écarté · "+fmt(gap," points")+". L’intervalle d’incertitude est indisponible.")
        bills=r.get("billing",[])
        cost=sum(v["eur"] for v in bills) if bills and all(v.get("eur") is not None for v in bills) else None
        st.caption("Essais · "+fmt(cost," €")+" sur 5 € autorisés. Achats observés du "+date_fr(historic.get("start"))+" au "+date_fr(historic.get("end"))+".")
    st.markdown("**Ce que le test de six mois va trancher** · Une variante dépasse-t-elle ses références après coûts sur les deux fenêtres ?")
    st.button("Ouvrir le test de 6 mois",on_click=select_page,args=("Test 6 mois",))
