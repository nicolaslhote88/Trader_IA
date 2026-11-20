"""Account reporting from dated Flex evidence and a separately authenticated live quote."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

REPORT = Path("/files/duckdb/lot03_reporting/compte.json")


def fmt(v, digits=2):
    if v is None:
        return "indisponible"
    return f"{Decimal(str(v)):,.{digits}f}".replace(",", " ").replace(".", ",")


def euros(v):
    return fmt(v) + " €" if v is not None else "indisponible"


def number(v):
    return None if v is None else float(v)


def load_report():
    try:
        d = json.loads(REPORT.read_bytes())
        if d["schema"] != 1:
            raise ValueError("version")
        return d
    except (OSError, ValueError, KeyError):
        st.error("La publication comptable du VPS est indisponible.")
        return None


def trusted(h, expected):
    a = h.get("account_alignment") or {}
    account = a.get("selected_account")
    return (h.get("authenticated") is True and h.get("dry_run") is False and a.get("aligned") is True
            and a.get("gateway_is_paper") is False and account and account == a.get("configured_account_id")
            and hashlib.sha256(account.upper().encode()).hexdigest() == expected)


@st.cache_data(ttl=20, show_spinner=False)
def probe(url, expected):
    at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    def read(path):
        response = requests.get(url.rstrip("/") + "/" + path, timeout=4)
        response.raise_for_status()
        return response.json()
    try:
        health = read("health")
        if not trusted(health, expected):
            return at, "Session IBKR déconnectée" if not health.get("authenticated") else "Compte réel non confirmé", None, None
        balances = read("account/ledger")
        ledger = balances["BASE"]
        eur_base = ledger.get("currency") == "EUR" or (ledger.get("currency") == "BASE" and float((balances.get("EUR") or {}).get("exchangerate", 0)) == 1)
        positions = read("positions")
        if not eur_base or ledger.get("netliquidationvalue") is None or not isinstance(positions, list) or not trusted(read("health"), expected):
            raise ValueError("compte")
        inventory = {str(r["conid"]):float(r["position"]) for r in positions if float(r["position"])}
        return at, "Compte réel connecté", float(ledger["netliquidationvalue"]), inventory
    except (requests.RequestException, KeyError, ValueError, TypeError):
        return at, "Lecture du compte réel indisponible", None, None


def live(url, expected, positions):
    at, status, nav, current = probe(url, expected)
    if nav is None:
        st.warning(f"{status}. Valorisation en direct indisponible. Contrôle à {at[11:19]} UTC.")
    else:
        st.metric("Valeur du compte en direct", euros(nav))
        match = current == {r["conid"]:float(r["qty"]) for r in positions if float(r["qty"])}
        st.caption(f"{status} · lecture du {at[:10]} à {at[11:19]} UTC · {len(current)} positions.")
        if not match:
            st.warning("Les quantités en direct diffèrent de la dernière clôture Flex.")


def status(d):
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(d["generated"])).total_seconds()
    text = f"Registre vérifié le {d['generated'][:10]} à {d['generated'][11:19]} UTC. Clôture Flex du {d['end']}. "
    missing = sum(r["nominal"] is None or r["fee"] is None for r in d["trades"])
    text += f"{d['paired']} exécutions appariées. {len(d['pending'])} exécution(s) du registre en attente de relevé."
    if age > 4500 or missing or d["mismatches"] or not d["qty_ok"] or d["pending"] or d["paired"] != len(d["trades"]):
        st.warning(text + f" {missing} exécution(s) Flex à chiffrer. " + ("Publication en retard." if age > 4500 else "Consulter le détail des contrôles."))
    else:
        st.caption(text)
    close_age = (datetime.now(timezone.utc).date() - datetime.fromisoformat(d["end"]).date()).days
    if close_age > 3:
        st.warning(f"Le dernier relevé Flex date de {close_age} jours calendaires. Les opérations suivantes attendent un relevé plus récent.")


def grid(rows, fields):
    df = pd.DataFrame([{label:(number(row.get(key)) if numeric else row.get(key)) for key,label,numeric in fields} for row in rows])
    if not df.empty:
        st.dataframe(df, hide_index=True, width="stretch")


@st.fragment(run_every="60s")
def render(url, compact=False):
    d = load_report()
    if d is None:
        return
    activity = d.get("activity", {})
    recent = " · ".join(label + " " + str(activity[k]) for k,label in [
        ("strategy_v4_consensus","Dernier consensus AG1"),("ag1_pf_v1_ibkr_reconcile","Dernier rapprochement PF"),
        ("latest_fill","Dernière exécution inscrite")] if activity.get(k))
    if compact:
        st.subheader("Rapprochement comptable IBKR")
        if recent:
            st.caption(recent)
        status(d)
        live(url, d["account"], d["positions"])
        return
    close = d["close"]
    series = d["series"]
    twr = next((r.get("twr") for r in reversed(series) if r["date"] == d["end"]), None)
    st.subheader(f"Compte IBKR à la clôture du {d['end']}")
    a,b,c = st.columns(3)
    a.metric("Valeur du compte", euros(close["total"]))
    b.metric("Résultat depuis le premier apport", euros(Decimal(close["total"]) - Decimal(d["external"])))
    c.metric("Rendement cumulé PortfolioAnalyst", fmt(Decimal(str(twr))*100,3)+" %" if twr is not None else "indisponible")
    st.caption(f"Titres {euros(close['stock'])} · trésorerie {euros(close['cash'])} · apports nets {euros(d['external'])}.")
    live(url, d["account"], d["positions"])
    status(d)
    if recent:
        st.caption(recent)
    st.subheader("Évolution du compte")
    if series:
        chart=pd.DataFrame([{"Date":r["date"], "Valeur du compte (€)":float(r["value"])} for r in series if r.get("value") is not None])
        if not chart.empty:
            st.line_chart(chart.set_index("Date"), height=220)
    st.subheader("Positions à la clôture")
    st.caption("Le prix de revient FIFO en euros inclut les commissions d’achat et le change daté de chaque exécution.")
    grid(d["positions"], [("symbol","Titre",0),("qty","Quantité FIFO",1),("flex_qty","Quantité IBKR",1),("pru","PRU (€)",1),
                          ("cost","Coût total (€)",1),("cost_delta","Correction du coût (€)",1),("currency","Devise",0),
                          ("mark","Cours de clôture natif",1),("value","Valeur (€)",1)])
    obs = d["observed"]
    st.caption(f"Quantités FIFO / Flex {'concordantes' if d['qty_ok'] else 'à rapprocher'}. Dernier inventaire indépendant du {obs[:10]} à {obs[11:19]} UTC · {'concordant' if d['observed_ok'] else 'écart de quantités'}.")
    st.subheader("Résultats des ventes et frais")
    realized = [r["realized"] for r in d["months"]]
    fees = [r["fees"] for r in d["months"]]
    a,b,c = st.columns(3)
    a.metric("Résultat FIFO des ventes", euros(sum(map(Decimal,realized)) if None not in realized else None))
    b.metric("Commissions sur toutes les exécutions", euros(sum(map(Decimal,fees)) if None not in fees else None))
    c.metric("Taxes sur transactions", euros(-Decimal(d["taxes"])))
    st.caption("FIFO au change de chaque exécution. Les commissions entrent déjà dans le coût des achats et le produit net des ventes. Les taxes sont présentées séparément.")
    state = d["state"]
    st.caption(f"Lot 2 · écart initial {euros(state['residu_historique_eur'])} · solde à ventiler {euros(state['residu_restant_eur'])}.")
    with st.expander("Comparer les anciens chiffres au FIFO corrigé"):
        reg = sum((Decimal(str(v)) for v in d["registered"].values() if v is not None), Decimal(0))
        st.write(f"Le registre historique contient {euros(reg)} de résultat réalisé. Le rejeu ci-dessous applique le même FIFO aux anciens montants puis aux montants justifiés par IBKR.")
        partial = [lot for lot in d.get("lot_comparison",[]) if number(lot["remaining"]) and lot["recorded"] is None and lot["matched_sales"]]
        if partial and all(lot["old_fifo"] is not None for lot in partial):
            amount = sum(Decimal(lot["old_fifo"]) for lot in partial)
            st.write(f"{len(partial)} lots encore ouverts ont déjà fait l’objet de ventes partielles. Le rejeu avec les anciens prix retrouve {euros(amount)} de résultat sur ces ventes. Le champ réalisé du registre est vide pour ces lots.")
        for label, key in [("Mois des ventes","months"),("Titre","symbols")]:
            st.markdown("**" + label + "**")
            grid(d[key], [("key",label,0),("count","Exécutions",1),("old_realized","Ancien rejoué (€)",1),
                          ("realized","FIFO corrigé (€)",1),("realized_delta","Écart sur ventes (€)",1),("turnover_delta","Écart de nominal brut échangé (€)",1)])
    with st.expander("Justificatifs des exécutions et lots FIFO"):
        special = st.checkbox("Afficher uniquement l’ancien taux de 0,8764", key="account_old_rate")
        rs=[r for r in d["trades"] if not special or number(r["old_rate"])==0.8764]
        grid(rs,[("date","Date",0),("symbol","Titre",0),("side","Sens",0),("qty","Quantité",1),("currency","Devise",0),("native","Nominal natif",1),
                 ("old_nominal","Ancien nominal (€)",1),("nominal","Nominal corrigé (€)",1),("rate","Taux vers EUR",1),("source","Source du taux",0),
                 ("fee_currency","Devise des frais",0),("commission_signed","Commission native signée",1),("fee","Frais (€)",1),("line","Ligne Flex",1)])
        st.download_button("Télécharger le détail des exécutions et des lots (JSON)", json.dumps({k:d.get(k) for k in ("trades","closed","open_lots","lot_comparison","periods")},ensure_ascii=False),
                           "executions_et_lots.json", "application/json")
    if d["pending"]:
        st.subheader("Exécutions du registre en attente de relevé")
        grid(d["pending"],[("date","Horodatage",0),("symbol","Titre",0),("side","Sens",0),("qty","Quantité",1),("currency","Devise",0),("currency_source","Source de la devise",0),("native","Nominal natif",1),("status","État",0)])
