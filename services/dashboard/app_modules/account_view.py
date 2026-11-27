import plotly.graph_objects as go
import streamlit as st
from .cockpit import returns_chart
from .ui_kit import chart, date_fr, fmt, freshness_chip, metrics, pct, verdict_card


def waterfall(account, performance):
    rows = performance.get("series") or []
    if not rows or not performance.get("start") or not performance.get("end"):
        return None
    months = [r for r in account.get("months", []) if performance["start"][:7].replace("-", "") <= r["key"] <= performance["end"][:7].replace("-", "")]
    first, last = rows[0], rows[-1]
    nav = first.get("nav")
    a, w, m = [last.get(k) for k in ("account", "world", "matched")]
    api = performance.get("api_documented")
    if None in (nav,a,w,m,api) or not months or any(r.get("fees") is None for r in months):
        return None
    fees = -sum(float(r["fees"]) for r in months)
    parts = [w*nav, (m-w)*nav, fees, (a-m)*nav-fees, 0, -api, 0]
    published = performance.get("after_documented")
    residual = None if published is None else published-sum(parts)
    return parts, residual


def render(d):
    a, p = d["account"], d["economic"].get("performance", {})
    rows = p.get("series") or []
    last = rows[-1] if rows else {}
    close = a.get("close", {})
    wf = waterfall(a,p)
    names = ["ETF monde", "Cash non investi", "Commissions", "Sélection, taxes et change", "Compte", "Dépenses IA", "Résultat économique"]
    verdict = f"Depuis le {date_fr(p.get('start'))}, le compte fait **{pct(last.get('account'))}** et l’ETF monde **{pct(last.get('world'))}**."
    if wf:
        i = max(range(1,4), key=lambda i:abs(wf[0][i]))
        verdict += f" Le principal poste de l’écart est « {names[i]} » ({fmt(wf[0][i], ' €')})."
    verdict_card(verdict if rows else "Le rendement comparé du compte est indisponible.")
    freshness_chip("Relevé officiel IBKR", a.get("end"), 4320)
    pos = a.get("positions")
    complete = pos is not None and all(r.get("value") is not None and r.get("cost") is not None for r in pos)
    gains = [float(r["value"])-float(r["cost"]) for r in pos] if complete else None
    total, deposits = close.get("total"), a.get("external")
    metrics([("Valeur du compte",fmt(total," €")),("Résultat depuis l’apport",fmt(float(total)-float(deposits) if None not in (total,deposits) else None," €")),
             ("Après dépenses IA",fmt(p.get("after_documented")," €")),("Plus-values latentes",fmt(sum(gains) if gains is not None else None," €"))])
    st.caption("Trésorerie "+fmt(close.get("cash")," €")+(" · "+str(sum(v>0 for v in gains))+" lignes en gain et "+str(sum(v<0 for v in gains))+" en perte." if gains is not None else ""))
    view = st.radio("Lecture", ["Rendement et écart","Positions et ventes"], horizontal=True, key="account_charts")
    if view == "Rendement et écart":
        returns_chart(p, basket=True)
        st.subheader("Ce qui explique le résultat en euros")
        if not wf:
            st.caption("Décomposition indisponible sur la fenêtre publiée.")
            return
        values, residual = wf
        measures = ["absolute", "relative","relative","relative","total","relative","total"]
        if residual is None:
            st.warning("Résultat économique publié indisponible pour le contrôle.")
        elif abs(residual)>1:
            st.warning("Fenêtres différentes · écart avec le résultat publié de "+fmt(residual," €")+".")
            names += ["Écart au résultat publié"]
            measures += ["relative"]
            values += [residual]
        else:
            st.caption("Écart avec le résultat publié · "+fmt(residual," €")+".")
        fig = go.Figure(go.Waterfall(x=names,y=values,measure=measures,increasing_marker_color="#28a745",decreasing_marker_color="#dc3545",totals_marker_color="#4ea1ff"))
        fig.update_yaxes(ticksuffix=" €")
        chart(fig)
    else:
        st.subheader("Titres et trésorerie à la clôture")
        if total and pos is not None and all(r.get("value") is not None for r in pos):
            items = [(r["symbol"],float(r["value"]),float(r["value"])-float(r["cost"]) if r.get("cost") is not None else None) for r in pos]
            if close.get("cash") is not None:
                items.append(("Trésorerie",float(close["cash"]),None))
            items.sort(key=lambda i:i[1])
            fig = go.Figure(go.Bar(y=[r[0] for r in items],x=[r[1] for r in items],orientation="h",
                       marker_color=["#6c757d" if r[0]=="Trésorerie" else "#4ea1ff" for r in items],
                       text=[pct(r[1]/float(total))+" · "+fmt(r[2]," €") if r[2] is not None else pct(r[1]/float(total)) for r in items],
                       textfont_color=["#9aa0a6" if r[2] is None else "#4ade80" if r[2]>=0 else "#ff7b85" for r in items]))
            chart(fig,350)
            fx = sum(float(r["value"]) for r in pos if r.get("currency") and r["currency"]!="EUR")
            st.caption("Titres en devises étrangères · "+pct(fx/float(total))+" du compte. Les étiquettes indiquent le poids et le résultat latent.")
        else:
            st.caption("Composition indisponible.")
        months = a.get("months",[])
        st.subheader("Résultat réalisé des ventes par mois")
        if months:
            vals = [float(r["realized"]) if r.get("realized") is not None else None for r in months]
            chart(go.Figure(go.Bar(x=[r["key"][:4]+"-"+r["key"][4:] for r in months],y=vals,marker_color=["#28a745" if (v or 0)>=0 else "#dc3545" for v in vals])))
            st.caption("Total FIFO sur les ventes · "+fmt(sum(vals) if None not in vals else None," €")+".")
