import json
from datetime import datetime, timezone
from pathlib import Path
import streamlit as st

MENU = "Évaluation prospective et pilote"
REPORT = Path("/files/duckdb/research_reporting/pilotage/report.json")
NAMES = {"ag3":"Classement AG3", "neutral":"Sélection neutre", "ag3_veto":"AG3 avec veto AG2",
         "neutral_veto":"Neutre avec veto AG2"}
BENCH = {"world":"Fonds mondial", "matched":"Fonds mondial à exposition comparable", "basket":"Panier conservé",
         "account":"AG1 V4 figé","variant_matched":"Fonds mondial à l’exposition de la variante"}
STRESS = {"cout_double":"Coûts doublés","retard":"Retard d’une séance","sans_meilleur":"Sans le meilleur titre"}


def read():
    try:
        data=json.loads(REPORT.read_bytes())
    except (OSError,ValueError):
        st.warning("La publication des lots 13 à 15 est indisponible.")
        return None
    age=datetime.now(timezone.utc)-datetime.fromisoformat(data["observed"])
    if age.total_seconds()>4500:
        st.warning("La dernière collecte date de plus de 75 minutes.")
    return data


def pct(v):
    return "En attente" if v is None else f"{100*v:+.2f} %".replace(".",",")


def health():
    data=read()
    if data:
        g=data["pilot"]
        st.caption(f"Lot 13 · {data['formal']['sessions']} séances formelles · pilote réel {g['state']} · collecte {data['observed'][:16]} UTC")
        if g["paper"]:
            st.warning("IBKR a sélectionné le compte de simulation.")


def render():
    data=read()
    if not data:
        return
    mandate,formal,pilot=data["mandate"],data["formal"],data["pilot"]
    st.subheader(MENU)
    view=st.radio("Lot",["13 · Observation","14 · Pilote","15 · Composants"],horizontal=True,key="forward_lot")
    st.caption(f"Collecte du {data['observed'][:16]} UTC")
    if view.startswith("13"):
        st.markdown(f"**{mandate['objective']}** · {mandate['capital']:,.0f} € fictifs".replace(","," "))
        st.write(f"Du {mandate['start']} au {mandate['split']} exclu, puis jusqu’au {mandate['end']} exclu. "
                 f"Première revue le {mandate['end']}.")
        a,b,c=st.columns(3)
        a.metric("Séances formelles",formal["sessions"])
        b.metric("Revues manquantes",len(formal["gaps"]))
        c.metric("Plafond de coûts sur six mois",f"{mandate['max_cost']} €")
        st.caption("Exposition maximale à l’achat 90 %, par secteur 30 %. Baisse maximale 10 %, avec une tolérance de 2 points face au témoin à exposition comparable. Les coûts API documentés sont imputés une fois à chaque portefeuille alternatif.")
        choice=st.selectbox("Variante",list(NAMES),format_func=NAMES.get)
        row=next(x for x in formal["results"] if x["variant"]==choice)
        st.write("Décision actuelle · "+row["verdict"])
        if row["reason"]:
            st.warning(row["reason"])
        st.dataframe([{"Début":x["start"],"Fin exclue":x["end_exclusive"],"Séances attendues":x["sessions"],
                       "Idées à 20 séances":x["maturity"],"Rendement net":pct(x["net"]),"Baisse maximale":pct(x["drawdown"]),
                       "Mesures manquantes":", ".join(x["missing"])} for x in row["windows"]],hide_index=True)
        details=[]
        for x in row["windows"]:
            for key,v in x["comparisons"].items():
                details.append({"Fenêtre":x["start"],"Témoin":BENCH[key],"Rendement":pct(v["net"]),"Excédent":pct(v["excess"])})
        if details:
            st.dataframe(details,hide_index=True)
        for x in row["windows"]:
            if x["stress"]:
                with st.expander("Sensibilités et concentration · "+x["start"]):
                    st.dataframe([{"Test":STRESS.get(k,k),"Rendement":pct(v)} for k,v in x["stress"].items()],hide_index=True)
                    st.write("Meilleur contributeur",x.get("best") or "En attente")
                    st.write("Pays et secteurs",x.get("geography",{}),x.get("sectors",{}))
                    st.write("Cinq plus fortes variations quotidiennes en euros",x["weight_dates"])
        with st.expander("Séances exploratoires antérieures au mandat"):
            st.dataframe([{"Jour":r["day"],"Variante":NAMES[r["variant"]],"Rendement simulé cumulé":pct(r["net"])}
                          for r in data["exploratory"]],hide_index=True)
    elif view.startswith("14"):
        st.metric("Pilote réel",pilot["state"])
        if pilot.get("register"):
            st.write("Registre de passation",pilot["register"])
        for reason in pilot["blockers"]:
            st.write("• "+reason)
        st.caption(f"Dernière clôture Flex {pilot['account_day']}. Dernier rapprochement réel {pilot['last_reconciled']}.")
        if data["blocked"]:
            st.dataframe([{"Exécution AG1":r["execution"],"Date UTC":r["at"],"État":r["status"],
                           "Dossier":r["reason"]} for r in data["blocked"]],hide_index=True)
    else:
        st.write("Les retraits se décident après la revue du pilote. Chaque changement porte sur une version publiée et un seul producteur.")
        st.dataframe([{"Workflow":r["name"],"État":r["state"],"Version publiée":r["published"],
                       "Lecteurs identifiés":", ".join(r["consumers"]+r["direct"]),
                       "Appels IA observés sur la fenêtre de collecte":r["calls"],
                       "Économie facturée (€)":r["saving"]} for r in data["retirement"]],hide_index=True)
        st.caption("Une case vide indique une mesure indisponible. La liste des lecteurs directs est complétée par les usages du dashboard et des expériences.")
