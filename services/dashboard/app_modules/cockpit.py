import plotly.graph_objects as go
import streamlit as st
from .navigation import select_page
from .ui_kit import (AMBER, RED, chart, date_fr, fmt, freshness_chip, metrics, pct,
                     status_tile, timestamp, verdict_card)

SERIES = {"account": ("Compte", "#4ea1ff"), "world": ("ETF monde", "#cfd3d8"),
          "matched": ("ETF à même exposition", "#fd7e14"), "basket": ("Panier conservé", "#c084fc")}


def returns_chart(p, basket=False):
    rows = p.get("series") or []
    end = rows[-1] if rows else {}
    a, w, matched = [end.get(k) for k in ("account", "world", "matched")]
    if a is not None and w is not None:
        st.subheader(f"Le compte est {fmt(abs(a-w)*100, ' points', 1)} {'sous' if a < w else 'au-dessus de'} l’ETF monde")
    else:
        st.subheader("Comparaison des rendements indisponible")
    fig = go.Figure()
    for key, (label, color) in SERIES.items():
        if key == "basket" and not basket:
            continue
        pts = [(r["date"], None if r.get(key) is None else 100*r[key]) for r in rows
               if key != "basket" or (p.get("freeze") and r["date"] >= p["freeze"])]
        if not any(y is not None for _, y in pts):
            continue
        fig.add_scatter(x=[x for x,y in pts], y=[y for x,y in pts], name=label, mode="lines",
                        line=dict(color=color, dash="dot" if key == "matched" else "solid"),
                        hovertemplate="%{x} · %{y:.2f} %<extra>"+label+"</extra>")
    if fig.data and basket:
        endpoints = sorted(((trace.y[-1], trace) for trace in fig.data if trace.y[-1] is not None), key=lambda item:item[0])
        prior = None
        for value,trace in endpoints:
            label_y = value if prior is None else max(value,prior+.6)
            fig.add_annotation(x=trace.x[-1],y=label_y,text=trace.name,showarrow=False,xanchor="left",xshift=4,
                               font=dict(color=trace.line.color,size=11))
            prior=label_y
        fig.update_layout(showlegend=False)
        if endpoints:
            from datetime import timedelta
            start,end=timestamp(rows[0]["date"]),timestamp(rows[-1]["date"])
            fig.update_xaxes(range=[start,end+timedelta(days=max(3,(end-start).days//4))])
    if fig.data:
        fig.update_yaxes(ticksuffix=" %")
        chart(fig)
    metrics([("Compte", pct(a)), ("ETF monde", pct(w))])
    if None not in (a,w,matched):
        st.caption(f"À exposition comparable, l’ETF fait {pct(matched)}. Le cash explique {fmt((matched-w)*100, ' points')} de l’écart. Les titres et les frais expliquent {fmt((a-matched)*100, ' points')}.")
    freshness_chip("Rendements à la clôture", p.get("end"), 3*1440)


def actions(d):
    signals, econ = d["signals"], d["economic"]
    out = []
    if signals["Compte réel"].state == RED:
        out.append((0,"Avant 17 h 10","Reconnecter la Gateway au compte réel."))
    if signals["Décision AG1 de 17 h 10"].state == RED and signals["Compte réel"].state != RED:
        out.append((0,"Avant 17 h 10","Examiner la dernière décision écrite au registre AG1."))
    if signals["Rapprochement du portefeuille"].state == RED:
        out.append((0,"À vérifier","Contrôler la prochaine écriture de rapprochement du portefeuille."))
    end = timestamp(d["account"].get("end"))
    if end and (d["now"].date()-end.date()).days > 3:
        out.append((1,"Cette semaine","Déposer le relevé officiel couvrant la période après le "+date_fr(d["account"]["end"])+"."))
    for issue in econ.get("performance",{}).get("market_status",{}).get("issues",[]):
        out.append((1,"Cette semaine","Vérifier la source de rendement · "+str(issue)))
    mismatch = sum(bool(c.get("requested") and c.get("registry") and c["requested"] != c["registry"]) for c in econ.get("usage",{}).get("calls",[]))
    if mismatch:
        out.append((2,"À corriger",f"Rapprocher le modèle demandé du registre pour {mismatch} appels IA."))
    for key in ("Sources","Analyses AG2 et AG3","Laboratoire"):
        if signals[key].state in (RED, AMBER):
            out.append((1,"Cette semaine",key+" · "+signals[key].title))
    return sorted(out,key=lambda v:v[0])


def render(d):
    st.title("Cockpit")
    todo = actions(d)
    verdict_card(todo[0][2] if todo else "Les contrôles disponibles ne signalent aucune action urgente.")
    signals = d["signals"]
    tiles = [("Compte réel",signals["Compte réel"],"Mon compte"),
             ("Décision AG1 de 17 h 10",signals["Décision AG1 de 17 h 10"],"Dashboard Trading"),
             ("Chaîne de données",d["chain"],"Chaîne"),("Test de 6 mois",d["test"],"Test 6 mois")]
    for col,(label,signal,page) in zip(st.columns(4),tiles):
        with col:
            status_tile(label, signal)
            st.button("Ouvrir · "+label, key="tile_"+page,on_click=select_page,args=(page,))
    stamp, value = d["writes"]["snapshot"]
    st.caption("Dernière valeur rapprochée du compte réel · "+fmt(value," €")+" · "+date_fr(stamp))
    curve, tasks = st.columns([1.65,1])
    p = d["economic"].get("performance",{})
    with curve:
        returns_chart(p)
    with tasks:
        st.subheader("À faire")
        for _,when,action in todo:
            st.markdown(f"**{when}** · {action}")
        if not todo:
            st.caption("Aucune action identifiée sur les données disponibles.")
        st.metric("Résultat après dépenses IA documentées",fmt(p.get("after_documented")," €"))
        st.caption(f"Résultat du compte {fmt(p.get('gross'),' €')} · dépenses IA documentées {fmt(p.get('api_documented'),' €')} · au {date_fr(p.get('flow_end'))}.")
    with st.expander("Détails et preuves"):
        st.write("Le compte utilise la sonde IBKR. La valeur rapprochée provient du registre de portefeuille. Les comparaisons utilisent les publications économiques datées.")
        for label,signal,page in tiles:
            st.caption(label+" · "+signal.detail+" · "+date_fr(signal.at))
