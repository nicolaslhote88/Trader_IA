from datetime import date, timedelta
from html import escape
import plotly.graph_objects as go
import streamlit as st
from .forward_campaign import NAMES
from .ui_kit import COLORS, GREEN, GREY, RED, PARIS, chart, date_fr, fmt, limit_signal, metrics, pct, verdict_card


def grid(results, mandate, today):
    labels = ["Excédent net F1","Excédent net F2","Baisse maximale","Baisse relative","Exposition","Secteur","Coûts"]
    lines = []
    for row in results:
        wins = row.get("windows", [])
        cells = []
        for i in range(2):
            w = wins[i] if len(wins)>i else {}
            mature = w.get("complete") and w.get("end_exclusive","9999") <= today
            refs = w.get("comparisons",{})
            vals = [v.get("excess") for v in refs.values()]
            excess = min(vals) if vals and None not in vals else None
            cells.append((GREY if not mature or excess is None else GREEN if excess>0 else RED, "Plus faible excédent net · "+pct(excess)))
        finished = [w for w in wins if w.get("complete") and w.get("end_exclusive","9999") <= today]
        w = finished[-1] if finished else {}
        drop = w.get("drawdown")
        peer = w.get("comparisons",{}).get("variant_matched",{}).get("drawdown")
        relative = drop-peer if None not in (drop,peer) else None
        for v,lim,unit in [(drop, mandate.get("max_drawdown"),"%"),(relative,mandate.get("relative_drawdown"),"%"),
                           (row.get("max_exposure"),mandate.get("exposure"),"%"),(row.get("max_sector"),mandate.get("sector"),"%"),
                           (row.get("total_cost"),mandate.get("max_cost"),"€")]:
            cells.append((limit_signal(v,lim,bool(finished)), (pct(v) if unit=="%" else fmt(v," €"))+" · plafond "+(pct(lim) if unit=="%" else fmt(lim," €"))))
        cells_html="".join(f'<td title="{escape(detail)}" style="text-align:center;color:{COLORS[color]}">●</td>' for color,detail in cells)
        lines.append("<tr><th>"+escape(NAMES.get(row["variant"],row["variant"]))+"</th>"+cells_html+"</tr>")
    st.markdown('<div style="overflow-x:auto"><table><thead><tr><th>Variante</th>'+''.join("<th>"+v+"</th>" for v in labels)+"</tr></thead><tbody>"+''.join(lines)+"</tbody></table></div>",unsafe_allow_html=True)
    st.caption("Vert · respecté. Orange · à moins de 20 % du plafond. Rouge · dépassé. Gris · période non mûre ou mesure indisponible. Les baisses portent sur la dernière fenêtre achevée et documentée.")


def render(d):
    data = d["campaign"]
    m, f = data.get("mandate",{}),data.get("formal",{})
    st.caption("Simulation · aucun argent réel")
    if not all(m.get(k) for k in ("start","split","end")):
        verdict_card("Le mandat daté est indisponible.")
        return
    start,split,end = [date.fromisoformat(m[k]) for k in ("start","split","end")]
    today=d["now"].astimezone(PARIS).date()
    days=(end-start).days
    elapsed=max(0,min(days,(today-start).days+1))
    rows=f.get("results",[])
    verdict_card(f"Jour **{elapsed} sur {days}**. "+("Première fenêtre en observation jusqu’au **"+date_fr(str(split-timedelta(days=1)))+"** inclus." if today<split else " · ".join(NAMES.get(r["variant"],r["variant"])+" — "+r["verdict"] for r in rows)))
    metrics([("Séances formelles",fmt(f.get("sessions"),digits=0)),("Capital fictif",fmt(m.get("capital")," €",0)),("Revue économique",date_fr(m["end"]))])
    st.caption("Collecte du "+date_fr(data.get("observed")))
    fig=go.Figure()
    for name,lo,hi,color in [("Fenêtre 1",start,split,"#4ea1ff"),("Fenêtre 2",split,end,"#c084fc")]:
        fig.add_bar(x=[(hi-lo).days*86400000],base=[lo.isoformat()],y=[name],orientation="h",marker_color=color,showlegend=False,hovertext=f"{lo} → {hi} exclu",hoverinfo="text")
    fig.add_scatter(x=[end.isoformat()],y=["Fenêtre 2"],mode="markers",marker=dict(symbol="diamond",size=13,color="#f5c76b"),name="Revue économique")
    fig.update_xaxes(type="date")
    fig.add_shape(type="line",x0=today.isoformat(),x1=today.isoformat(),y0=0,y1=1,yref="paper",line=dict(dash="dot",color="#fff"))
    st.subheader("Deux fenêtres de trois mois")
    chart(fig,205)
    if not f.get("sessions"):
        st.info("Courbes publiées après la première séance formelle, chaque soir après 17 h 26.")
    else:
        fig=go.Figure()
        for row in rows:
            pts=[(w["end_exclusive"],w["net"]) for w in row.get("windows",[]) if w.get("complete") and w.get("net") is not None and w["end_exclusive"]<=str(today)]
            if pts:
                fig.add_scatter(x=[i for i,v in pts],y=[100*v for i,v in pts],mode="markers",name=NAMES[row["variant"]])
        if fig.data:
            st.subheader("Rendements nets des fenêtres formelles achevées")
            chart(fig)
        st.info("Les séries quotidiennes de valeur et de références du mandat ne sont pas publiées. Dernière séance formelle · "+date_fr(f.get("last_day"))+".")
    st.subheader("Qualification des quatre variantes")
    grid(rows,m,str(today))
    st.caption("Références · ETF monde, ETF à exposition comparable, panier conservé, AG1 figé et ETF à l’exposition de chaque variante. Robustesse · coûts doublés, retard d’une séance, retrait du meilleur titre.")
    before = data.get("exploratory",[])
    days_seen=sorted({r["day"] for r in before})
    if days_seen:
        st.subheader("Avant le mandat")
        groups={}
        for key,name in NAMES.items():
            series={r["day"]:r["net"] for r in before if r["variant"]==key}
            groups.setdefault(tuple(series.get(day) for day in days_seen),[]).append(name)
        for series,names in groups.items():
            st.write(" et ".join(names)+" · "+pct(series[-1])+" au "+date_fr(days_seen[-1]))
        st.caption(f"{len(days_seen)} séances exploratoires. Les séries de rendements identiques sont regroupées.")
