from calendar import monthrange
from datetime import date, timedelta
from statistics import median

import plotly.graph_objects as go
import streamlit as st
from .ui_kit import chart, date_fr, fmt, freshness_chip, metrics, pct, verdict_card

COLORS = {"Commissions":"#4ea1ff", "OpenAI":"#10b981", "Anthropic":"#f5c76b", "DeepSeek":"#c084fc"}


def render(d):
    economic = d["economic"]
    p,u,o = [economic.get(k,{}) for k in ("performance","usage","orders")]
    months = d["account"].get("months", [])
    rows = p.get("series", [])
    nav = rows[0].get("nav") if rows else None
    dated = p.get("start") and p.get("end")
    begin = date.fromisoformat(p["start"]) if dated else None
    end = date.fromisoformat(p["end"]) if dated else None
    scoped = [r for r in months if dated and begin.strftime("%Y%m") <= r["key"] <= end.strftime("%Y%m")]
    fees = sum(float(r["fees"]) for r in scoped) if scoped and all(r.get("fees") is not None for r in scoped) else None
    api = p.get("api_documented")
    total = fees + api if None not in (fees,api) else None
    days = (end-begin).days if dated else None
    req = total/nav*365/days if total is not None and nav and days and days>0 else None
    verdict_card(f"Les coûts documentés atteignent **{fmt(total,' €')}** sur {len(scoped)} mois, soit {pct(total/nav)} du capital de départ. Les couvrir représente **{fmt(req*100 if req is not None else None,' points par an',1)}** de rendement supplémentaire." if total is not None and nav else "Le coût sur la période est indisponible.")
    metrics([("Commissions",fmt(fees," €")),("Dépenses IA documentées",fmt(api," €")),("Coût documenté",fmt(total," €")),("Rendement nécessaire par an",pct(req))])
    freshness_chip("Comptes arrêtés au",p.get("end"),4320)
    st.caption("Taxes sur transactions et VPS exclus. Les dépenses IA suivent les justificatifs disponibles.")
    selected = st.radio("Lecture des coûts",["Par mois","Barème et taille des ordres"],horizontal=True,key="cost_charts")
    if selected == "Par mois":
        buckets = {r["key"][:4]+"-"+r["key"][4:]:{"Commissions":float(r["fees"]) if r.get("fees") is not None else None} for r in scoped}
        for row in u.get("provider_costs",[]):
            if row["month"] in buckets:
                buckets[row["month"]][row["provider"]] = row.get("reporting_eur")
        if not buckets:
            st.caption("Ventilation mensuelle indisponible.")
            return
        keys = sorted(buckets)
        sums = [sum(v for v in buckets[k].values() if v is not None) for k in keys]
        st.subheader(f"{keys[sums.index(max(sums))]} est le mois au coût documenté le plus élevé")
        fig=go.Figure()
        for name,color in COLORS.items():
            fig.add_bar(name=name,x=keys,y=[buckets[k].get(name) for k in keys],marker_color=color)
        fig.add_scatter(x=keys,y=sums,text=[fmt(v," €") for v in sums],mode="text",textposition="top center",showlegend=False)
        fig.update_layout(barmode="stack")
        chart(fig)
        gaps=[]
        for k in keys:
            first=date.fromisoformat(k+"-01")
            first_day=max(begin, first)
            last_day=min(end, first.replace(day=monthrange(first.year,first.month)[1]))
            for name in list(COLORS)[1:]:
                ranges=u.get("provider_ranges",{}).get(name,[])
                if any(not any(a<=day.isoformat()<=b for a,b in ranges) for day in (first_day+timedelta(days=i) for i in range((last_day-first_day).days+1))):
                    gaps.append(name+" "+k)
        if gaps:
            st.warning("Justificatifs absents ou partiels · "+", ".join(gaps)+".")
        st.caption("Les barres utilisent les totaux mensuels fournisseurs. Le montant en tête est arrêté à la date du compte.")
    else:
        items=o.get("orders",[])
        ref=[r for r in items if r.get("date","") < "2026-11-20"]
        same=(bool(ref) and len(ref)==o.get("count") and all(r.get("old") is not None for r in ref)
              and o.get("expected") is not None and abs(sum(r["old"] for r in ref)-o["expected"])<.011)
        if same and all(r.get("fee") is not None and r.get("actual") is not None for r in ref):
            old=o["expected"]
            actual=sum(r["actual"] for r in ref)
            st.subheader(f"Les frais constatés valent {fmt(actual/old if old else None,' fois',1)} la prévision initiale")
            chart(go.Figure(go.Bar(y=["Ancien calcul","Nouveau barème","Frais constatés"],x=[old,sum(r["fee"] for r in ref),actual],orientation="h",marker_color=["#6c757d","#fd7e14","#4ea1ff"])),230)
            st.caption(f"{len(ref)} ordres de référence. Le nouveau barème est consultatif.")
        else:
            st.caption("Échantillon de référence comparable indisponible.")
        st.subheader("Les petits tickets supportent davantage le minimum de commission")
        b=list(range(100,3001,25))
        fig=go.Figure(go.Scatter(x=b,y=[100*max(3,v*.0005)/v for v in b],line_color="#4ea1ff"))
        sizes=[r["nominal"] for r in items if r.get("nominal") is not None and r["nominal"]>0]
        if sizes:
            fig.add_vline(x=median(sizes),line_dash="dot",annotation_text="Ticket médian")
        fig.update_xaxes(title="Montant de l’ordre (€)")
        fig.update_yaxes(title="Commission d’un sens (%)")
        chart(fig)
        st.caption("Paris et Amsterdam · 0,05 % avec un minimum de 3 €. "+(f"{sum(v<300 for v in sizes)} ordres sous 300 € sur {len(sizes)} chiffrés." if sizes else "Tickets réels indisponibles."))
