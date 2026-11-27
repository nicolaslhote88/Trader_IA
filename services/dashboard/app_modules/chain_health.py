from datetime import datetime, time, timedelta, timezone
import plotly.graph_objects as go
import streamlit as st
from .ui_kit import PARIS, RED, chart, date_fr, status_tile, timestamp, verdict_card


def slots(now, kind):
    lo=now-timedelta(hours=72)
    result=[]
    day=lo.astimezone(PARIS).date()
    while day<=now.astimezone(PARIS).date():
        weekday=day.weekday()<5
        if kind=="Recherche":
            times=[(h,m) for h in range(24) for m in (9,39)]
            if weekday:
                times.extend([(17,16),(17,19)])
        elif kind=="Expériences":
            times=[(12,32),(14,32),(20,32)]+([(17,28)] if weekday else [])
        else:
            times=[(h,15) for h in range(9,17)]+[(17,40),(23,15)] if weekday else []
        for h,m in times:
            at=datetime.combine(day,time(h,m),PARIS).astimezone(timezone.utc)
            if lo<=at<=now:
                result.append(at)
        day+=timedelta(days=1)
    return sorted(result)


def matched_slots(planned, stamps):
    ticks=sorted(t for v in stamps if (t:=timestamp(v)))
    used=set()
    for event in ticks:
        eligible=[t for t in planned if timedelta(0)<=event-t<timedelta(minutes=10)]
        if eligible:
            used.add(max(eligible))
    return len(used)


def render(d):
    signals=d["signals"]
    red=sum(s.state==RED for s in signals.values())
    detail=signals["Compte réel"].detail if signals["Compte réel"].state==RED else ""
    verdict_card(f"**{red} maillon(s) rouge(s)** sur sept. "+detail)
    st.subheader("Les sept maillons")
    for group in (list(signals.items())[:4],list(signals.items())[4:]):
        for col,(label,signal) in zip(st.columns(len(group)),group):
            with col:
                status_tile(label,signal)
    now=d["now"]
    census=[c.get("at") for c in d["research"].get("census",[])]
    complete=any(timestamp(v) and timestamp(v)<=now-timedelta(hours=72) for v in census)
    histories={"Recherche":census if complete else None, "Expériences":None,
               "Rapprochement":d["writes"]["pf_times"] if d["writes"]["snapshot"][0] else None}
    fig=go.Figure()
    for name,stamps in histories.items():
        planned=slots(now,name)
        if stamps is None:
            st.caption(name+f" · historique complet des écritures indisponible sur 72 h ({len(planned)} passages prévus).")
            continue
        n=matched_slots(planned,stamps)
        fig.add_bar(y=[name],x=[100*n/len(planned) if planned else None],orientation="h",name=name,
                    text=[f"{n} / {len(planned)}"],marker_color="#4ea1ff" if n==len(planned) else "#fd7e14")
    st.subheader("Passages ayant écrit leur donnée sur les dernières 72 heures")
    if fig.data:
        fig.update_xaxes(range=[0,105],ticksuffix=" %")
        chart(fig,225)
    st.caption("Une écriture datée est rattachée au dernier créneau prévu, dans les dix minutes suivant celui-ci. Chaque créneau compte une fois.")
    st.markdown("**Un succès n8n ne suffit pas.** Le rapprochement exige une valorisation inscrite au registre. Sa dernière écriture date du "+date_fr(d["writes"]["snapshot"][0])+".")
