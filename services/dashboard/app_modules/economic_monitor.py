from datetime import datetime,timezone
import json
from pathlib import Path

import pandas as pd
import streamlit as st

REPORT=Path("/files/duckdb/economic_reporting/report.json")
VIEWS=("Coût des ordres","Dépenses IA","Rendements comparés")

def fmt(v,unit="",digits=2):
    return "Indisponible" if v is None else f"{v:,.{digits}f}".replace(","," ").replace(".",",")+unit

def grid(rows,columns):
    if rows:st.dataframe(pd.DataFrame([{label:r.get(k) for k,label in columns} for r in rows]),hide_index=True,width="stretch")

def orders(o):
    st.subheader("Coût des ordres")
    a,b,c=st.columns(3)
    a.metric("Ordres de référence",o["count"]);b.metric("Commissions prévues à l’époque",fmt(o["expected"]," €"));c.metric("Commissions inscrites dans AG1",fmt(o["recorded"]," €"))
    st.caption("Les frais constatés ci-dessous proviennent des exécutions Flex converties par le lot 3. Le barème expérimental applique un minimum par ordre, y compris en cas d’exécutions partielles le même jour.")
    p=st.selectbox("Échantillon",["Référence","Réserve oct.–18 nov.","Prospectif depuis déploiement"])
    by=st.selectbox("Comparer par",["Ensemble","Taille du ticket","Place de cotation","Sens"])
    group={"Ensemble":"total","Taille du ticket":"size","Place de cotation":"place","Sens":"side"}[by]
    metrics=[dict(r,method="10 points de base" if r["method"]=="old" else "Barème par place") for r in o["metrics"] if r["group"]==group and r["period"]==p]
    if metrics:grid(metrics,[("key","Groupe"),("method","Estimation"),("n","Ordres comparables"),("bias","Biais moyen (€)"),("mae","Erreur absolue moyenne (€)"),("under","Sous-estimations > 0,005 €")])
    else:st.info("Aucun ordre exécuté et rapproché dans cet échantillon.")
    with st.expander("Estimation sur les candidats du dernier préflight AG1",expanded=True):
        st.caption("Quantité indicative de la matrice, prix et taux disponibles avant la décision des modèles. L’estimation suppose une sortie au même prix et une seule séance d’exécution. Un champ vide désigne une donnée manquante.")
        grid(o.get("previews",[]),[("symbol","Titre"),("qty","Quantité testée"),("place","Place"),("nominal","Nominal (€)"),("old_estimate","Ancienne estimation entrée (€)"),("entry_commission","Entrée (€)"),("tax","Taxe achat (€)"),("exit_commission","Sortie (€)"),("round_trip_spread","Spread aller-retour (€)"),("known_round_trip","Commissions + taxe + spread (€)"),("quote","Cotation UTC"),("availability","Disponibilité")])
    try:
        watch=json.loads(REPORT.with_name("quotes.json").read_bytes())
    except (OSError,ValueError):
        watch=None
    if watch:
        with st.expander("Dernière observation indépendante avant et pendant le consensus"):
            st.caption("Collecte du "+watch["at"]+". Quantités reprises du dernier préflight, change retenu de la veille. Une cotation différée écarte toute estimation du spread.")
            grid(watch["rows"],[("symbol","Titre"),("qty","Quantité indicative"),("old","10 pb à l’entrée (€)"),("commission","Commission par sens (€)"),("tax","Taxe d’achat (€)"),("spread","Spread aller-retour (€)"),("age","Âge (s)"),("availability","Disponibilité IBKR")])
    st.subheader("Entrée, sortie et coûts observés")
    st.caption(f"Barème {o['version']}. L’aller-retour conserve prix, quantité et taux de l’entrée. Le coût du change attend une conversion attribuable. Sur les {o['historical_missing']} exécutions historiques, le registre ne renseigne aucun glissement.")
    if o.get("future_measured",0):
        st.caption(f"Contrôle prospectif : {o['future_measured']} ordre(s) avec glissement mesuré sur {o['future_executed']} exécuté(s).")
    else:
        st.info(f"Validation du glissement sur un nouvel ordre en attente. {o.get('future_executed',0)} ordre(s) exécuté(s) depuis le déploiement.")
    small=st.checkbox("Limiter aux tickets de moins de 300 €")
    rows=[r for r in o["orders"] if not small or r["size"]=="< 300 €"]
    grid(rows,[("date","Date"),("symbol","Titre"),("side","Sens"),("place","Place"),("parts","Exécutions"),("planned_nominal","Nominal prévu (€)"),("old","Ancienne commission (€)"),("fee","Commission prévue (€)"),("actual","Commission Flex (€)"),("tax","Taxe estimée (€)"),("actual_tax","Taxe attribuée (€)"),("explicit_round_trip","Commissions + taxe aller-retour (€)"),("estimated_half_spread","Demi-spread estimé à la décision (€)"),("half_spread","Demi-spread à l’envoi (€)"),("slippage_beyond_touch","Écart au prix exécutable (€)"),("fx_cost","Coût du change (€)")])
    with st.expander("Change et exécutions partielles"):
        st.caption("Scénario AutoFX : 0,03 % du nominal si l’intégralité doit être convertie. Une position déjà financée en devise peut ne nécessiter aucune conversion. Le montant effectivement attribuable reste vide sans pièce de change.")
        grid(rows,[("date","Date"),("symbol","Titre"),("parts","Exécutions"),("fill_days","Jours d’exécution"),("fee_replayed_per_day","Commission rejouée par jour, partiels groupés (€)"),("currency","Devise"),("fx_scenario","Scénario AutoFX par sens (€)"),("fx_cost","Change constaté (€)")])
    with st.expander("Horodatage et qualité des cotations"):
        grid(rows,[("date","Date"),("symbol","Titre"),("decision_quote","Cotation à la décision"),("decision_received","Fin du préflight"),("decision_age","Âge fourni (min)"),("decision_availability","Disponibilité IBKR"),("sent","Envoi broker UTC"),("send_quote_epoch","Cotation à l’envoi (epoch UTC)"),("send_availability","Disponibilité à l’envoi")])
        st.write("Seule une fourchette acheteur-vendeur temps réel observée avant l’envoi et âgée de 60 secondes au plus permet la mesure. Le demi-spread et l’écart au prix exécutable s’additionnent une seule fois. Un champ vide indique une composante inconnue.")
        measured=[r for r in rows if r.get("slippage_bps") is not None]
        grid(measured,[("date","Date"),("symbol","Titre"),("slippage_bps","Écart au prix exécutable (pb)"),("slippage_beyond_touch","Montant au change Flex (€)")])
    st.download_button("Télécharger les ordres et comparaisons",json.dumps(o,ensure_ascii=False),"couts-ordres.json")

def expenses(u):
    st.subheader("Dépenses API du projet")
    totals=u["provider_costs"]
    for col,p in zip(st.columns(3),["OpenAI","Anthropic","DeepSeek"]):
        col.metric(p,fmt(sum(x["total_usd"] for x in totals if x["provider"]==p)," $"))
    st.caption("Dépenses en dollars sur les périodes exportées par chaque fournisseur. Les trois API sont entièrement affectées à Trader IA. Les montants VPS et données restent vides selon le périmètre retenu.")
    grid(totals,[("month","Mois"),("provider","Fournisseur"),("total_usd","Export projet ($)"),("AG1","AG1 justifié ($)"),("AG2","AG2 justifié ($)"),("AG4","AG4 justifié ($)"),("Commun projet","Commun au projet ($)"),("reporting_eur","Conversion indicative (€)")])
    with st.expander("Ventilation estimée des traces couvertes"):
        st.write("Pour DeepSeek, les tokens mesurés sont valorisés au prix moyen d’entrée et de sortie du même modèle et de la même journée dans l’export. Cette estimation constitue une seconde lecture de la même dépense.")
        grid(totals,[("month","Mois"),("provider","Fournisseur"),("estimate_AG1","AG1 estimé ($)"),("estimate_AG2","AG2 estimé ($)"),("estimate_AG4","AG4 estimé ($)"),("estimate_unallocated","Reste commun ($)")])
    with st.expander("Couverture des fichiers et des appels"):
        grid(u["proofs"],[("provider","Fournisseur"),("start","Du"),("end","Au"),("source","Pièce"),("sha","SHA-256")])
        grid(u["groups"],[("month","Mois"),("source","Producteur"),("model","Modèle demandé"),("calls","Appels observés"),("input","Tokens d’entrée observés"),("uncertain_output","Sorties incertaines"),("errors","Erreurs observées")])
        grid(u["execution_coverage"],[("workflow","Workflow"),("first","Première trace"),("last","Dernière trace"),("retained","Exécutions archivées")])
        st.caption("La conservation n8n borne la couverture historique. Les reprises internes à un SDK peuvent être invisibles.")
    st.subheader("Appels d’une décision AG1")
    calls=[r for r in u["calls"] if r["producer"]=="AG1"]
    choices=sorted({r["execution"] for r in calls},reverse=True)
    if choices:
        eid=st.selectbox("Exécution du consensus",choices,index=choices.index(28587) if 28587 in choices else 0)
        rs=[r for r in calls if r["execution"]==eid]
        st.write(f"{sum(r['input'] or 0 for r in rs):,} tokens d’entrée observés.".replace(","," "))
        grid(rs,[("provider","Fournisseur"),("requested","Modèle demandé"),("registry","Modèle du registre"),("returned","Modèle retourné"),("input","Entrée"),("output","Sortie déclarée"),("output_reliable","Sortie exploitable"),("cache_read","Cache lu"),("cache_write","Cache écrit"),("prompt","Version du prompt"),("ms","Durée (ms)"),("cost_export_usd","Coût directement rapproché ($)"),("allocated_estimate_usd","Quote-part estimée ($)")])
        traced=[r["cost_export_usd"] for r in rs if r.get("cost_export_usd") is not None]
        st.write("Issue du consensus · "+str(rs[0].get("decision_outcome") or "inconnue")+". Montant directement rapproché des appels · "+(fmt(sum(traced)," $",4) if traced else "indisponible")+f" ({len(traced)}/{len(rs)} appels).")
        st.caption("Le compteur de sortie OpenAI peut afficher zéro avec une proposition effectivement produite. Le coût complet de la décision reste ouvert si un de ses appels manque de justificatif.")
    st.download_button("Télécharger les appels et les ventilations",json.dumps(u,ensure_ascii=False),"consommations-ia.json")

def performance(p):
    st.subheader(f"Performance à la clôture du {p['end']}")
    last=p["series"][-1];a,b,c=st.columns(3)
    a.metric("Valeur officielle IBKR",fmt(last["nav"]," €"));b.metric("TWR officiel depuis l’ouverture",fmt(100*last["twr"]," %"));c.metric("Résultat du compte après API documentées",fmt(p["after_documented"]," €"))
    st.caption(f"Dépenses API documentées converties en euros jusqu’à cette clôture : {fmt(p['api_documented'],' €')}. "+("Les trois exports couvrent la période." if p["api_covered"] else "Des périodes fournisseur restent sans export. Le résultat économique complet reste ouvert.")+" Les frais et taxes prélevés dans le compte sont déjà inclus dans la NAV.")
    st.write(f"Comparaison cumulée depuis le {p['start']}. Le panier conservé commence le {p['freeze']}.")
    df=pd.DataFrame(p["series"]).set_index("date")[["account","world","matched","basket"]]*100
    df.columns=["Compte IBKR","Fonds mondial","Fonds à exposition de la veille","Panier conservé depuis le 30 octobre"]
    st.line_chart(df,y_label="Rendement (%)")
    if None not in (last["world"],last["matched"],last["account"]):
        st.caption("Sur cette fenêtre, l’écart entre l’exposition effective et la pleine exposition est de "
                   +fmt(100*(last["matched"]-last["world"])," points")+". Le compte s’écarte ensuite du témoin à exposition comparable de "
                   +fmt(100*(last["account"]-last["matched"])," points")+". Ce second écart regroupe sélection, pondérations, devises et frais.")
    st.subheader("Rendements mensuels et résultat après dépenses")
    rows=[{**r,**{k:r[k]*100 if r[k] is not None else None for k in ("account","world","matched","basket")}} for r in p["months"]]
    grid(rows,[("month","Mois"),("start","Base"),("end","Clôture"),("nav","NAV (€)"),("flow","Flux externes (€)"),("account","Compte (%)"),("world","Monde (%)"),("matched","Exposition veille (%)"),("basket","Conservation (%)"),("api_eur","API documentées (€)"),("api_covered","Exports API complets"),("net_eur","Résultat mensuel après API (€)")])
    grid([{"date":d,"amount":v} for d,v in p["flows"]],[("date","Date du flux externe"),("amount","Montant (€)")])
    with st.expander("Conventions et reproduction de l’audit"):
        st.write("Le fonds EUNL.DE capitalise les dividendes. Les témoins utilisent des cours ajustés, un cash à rendement nul et des transactions simulées sans frais. Le second applique à chaque séance Xetra l’exposition observée à la clôture précédente. Une fermeture de Xetra conserve la dernière valeur et sa date. Les titres du panier utilisent leur propre clôture et les cours de change en EUR. Des horaires de clôture différents limitent l’interprétation à la journée.")
        check=p["check"];ref=check["end"]
        st.write(f"Du {p['start']} au {ref['date']} : compte {fmt(ref['account']*100,' %')}, monde {fmt(ref['world']*100,' %')}, exposition comparable {fmt(ref['matched']*100,' %')}.")
        st.write(f"Du 30 octobre au 16 novembre : panier {fmt(check['basket']*100,' %')}, compte {fmt(check['held_account']*100,' %')}, monde {fmt(check['held_world']*100,' %')}.")
        st.caption("Titres du panier : "+", ".join(p["holding_symbols"])+". Dernière collecte PortfolioAnalyst : "+str(p["market_status"]["pa_at"]))
    if p["market_status"]["issues"]:st.warning("Collecte partielle · "+", ".join(p["market_status"]["issues"]))
    st.download_button("Télécharger les séries datées",json.dumps(p,ensure_ascii=False),"comparaisons-rendement.json")

def health():
    try:
        d=json.loads(REPORT.read_bytes())
    except (OSError,ValueError):
        st.warning("Les mesures de coûts et de rendement sont indisponibles.");return
    u=d["usage"];p=d["performance"]
    text=f"Coûts et rendement · {len(d['orders']['orders'])} ordres exécutés · {len(u['calls'])} appels IA observés · clôture {p['end']} · calcul {p['published'][:19]} UTC."
    if u["trace_errors"] or p["market_status"]["issues"] or (datetime.now(timezone.utc)-datetime.fromisoformat(p["published"])).total_seconds()>4500:
        st.warning(text+" Contrôle de fraîcheur ou de collecte ouvert.")
    else:st.caption(text)

@st.fragment(run_every="120s")
def render(view):
    try:
        data=json.loads(REPORT.read_bytes())
    except (OSError,ValueError):
        st.warning("Publication économique indisponible.");return
    section,fn=dict(zip(VIEWS,[("orders",orders),("usage",expenses),("performance",performance)]))[view]
    value=data[section];when=value["published"]
    if (datetime.now(timezone.utc)-datetime.fromisoformat(when)).total_seconds()>4500:
        st.warning("Publication en retard. Dernier calcul du "+when)
    fn(value);st.caption("Dernière publication du VPS · "+when)
