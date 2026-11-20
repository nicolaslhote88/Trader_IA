from datetime import datetime, timezone
import gzip, json
from pathlib import Path
import pandas as pd
import streamlit as st
from app_modules.economic_monitor import fmt
from app_modules.research_sim import date_fr

ROOT = Path("/files/duckdb/research_reporting/experiments")
MENU = "Expériences de stratégie"
VIEWS = ["Conserver ou vendre", "Nouvelles par événement", "Contribution des IA"]
NAMES = {
    "A": "Règle seule",
    "B": "Premier avis conditionnel",
    "C": "Consensus V4",
    "un_avis": "Premier avis systématique",
    "deux_avis": "Deuxième avis conditionnel",
    "A_veto": "Règle avec veto AG2",
    "original": "Pack initial",
    "original_repeat": "Répétition à l’identique",
    "news_v2": "Nouvelles resserrées",
    "no_context": "Sans contexte AG5–AG8",
}


def read(n):
    p = (ROOT / n).resolve()
    if not p.is_relative_to(ROOT):
        raise ValueError("Nom de publication")
    raw = p.read_bytes()
    return json.loads(gzip.decompress(raw) if p.suffix == ".gz" else raw)


def grid(rows):
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.caption("Aucune observation pour cette sélection.")


def health():
    try:
        r = read("report.json")
        st.caption(
            f"Expériences actions · collecte du {date_fr(r['at'])} · {len(r['cases'])} sessions appariées étudiées."
        )
        if (datetime.now(timezone.utc) - datetime.fromisoformat(r["at"])).total_seconds() > 86400:
            st.warning("Dernière publication des expériences vieille de plus de 24 heures.")
    except (ValueError, OSError):
        st.warning("Publication des expériences actions indisponible.")


def exits(r):
    h = r["exits"]
    a, b, c = st.columns(3)
    a.metric("Ventes approuvées", h["approved"])
    b.metric("Évaluables à 20 séances", h["evaluated"])
    c.metric("Titre vendu moins fonds mondial", fmt(100 * h["mean_excess"], " points"))
    st.caption(
        "Rendement du titre après la proposition, du premier cours de clôture suivant jusqu’à vingt séances plus tard. Une valeur positive signifie que le titre vendu a dépassé le fonds mondial."
    )
    summary = []
    for k in ["conserver", "réduire", "bande", "remplacer"]:
        vals = [x["arms"][k] for x in h["rows"] if k in x["arms"]]
        summary.append(
            {
                "Politique": k,
                "Cas chiffrés": len(vals),
                "Écart moyen face à la détention passive (€)": sum(x["delta"] for x in vals) / len(vals)
                if vals
                else None,
                "Coût moyen (€)": sum(x["fees"] for x in vals) / len(vals) if vals else None,
                "Amélioration du point bas moyen (€)": sum(x["drawdown_avoided"] for x in vals) / len(vals)
                if vals
                else None,
            }
        )
    grid(summary)
    st.caption(
        "Estimations aux clôtures ajustées avec commissions et change modélisés. Chaque proposition repart de sa position initiale. Plusieurs observations peuvent se recouvrir. La conservation et la bande respectent les sorties de risque et de liquidité."
    )
    why = st.selectbox(
        "Motif principal simulé", ["Tous", "risque", "liquidités", "fondamental", "remplacement"]
    )
    mode = st.selectbox("Politique à détailler", ["réduire", "conserver", "bande", "remplacer"])
    grid(
        [
            {
                "Date": x["day"],
                "Titre": x["symbol"],
                "Motif": x["reason"],
                "Quantité": x["qty"],
                "Source quantité": x["quantity_source"],
                "Remplaçant": x["replacement"],
                "Écart net (€)": x["arms"].get(mode, {}).get("delta"),
                "Frais (€)": x["arms"].get(mode, {}).get("fees"),
                "Cash réinvesti (€)": x["arms"].get(mode, {}).get("cash_used"),
                "Poids retiré (%)": 100 * x["arms"][mode]["weight_removed"] if mode in x["arms"] else None,
                "Donnée manquante": x["excluded"]
                or (x.get("replacement_gap") if mode == "remplacer" else None),
            }
            for x in h["rows"]
            if why == "Tous" or x["reason"] == why
        ]
    )
    z = h["basket"]
    st.write(
        f"Du 30 octobre au 16 novembre, le panier conservé aurait rapporté {fmt(z['hold'] * 100, ' %')}. Le compte a enregistré {fmt(z['account'] * 100, ' %')} sur cette période."
    )
    with st.expander("Prosus et rejouabilité avec des fourchettes datées"):
        grid(h["prosus"])
        grid(
            [
                {
                    "Date": x["day"],
                    "Titre": v["symbol"],
                    "Motif": v["reason"],
                    "Remplaçant": v["alternative"],
                    "Résultat": v["status"],
                }
                for x in r["cases"]
                for v in x["exit_review"]
            ]
        )


def comparison(r):
    return [
        {
            "Date": x["day"],
            "Comparaison": NAMES[y["arm"]],
            "Paire valide": y["valid_pair"],
            "Décisions modifiées": y["changed_decisions"],
            "Instructions modifiées": y["changed_orders"],
            "Exécutions modifiées": y["changed_fills"],
            "Achats, écart après exécution (€)": y["sides"]["BUY"],
            "Ventes, écart après exécution (€)": y["sides"]["SELL"],
            "Écart après IA (€)": y["net_delta"],
        }
        for x in r["cases"]
        for y in x["paid"]["differences"]
    ]


def news(r):
    n = r["news"]
    m = n["metrics"]
    a, b, c = st.columns(3)
    a.metric("Articles sur 30 jours", n["raw"])
    b.metric("Événements retenus sur 14 jours", n["events"])
    c.metric(
        "Détenus couverts",
        next((f"{v['new']} / {v['n']}" for v in m["coverage"] if v["segment"] == "DÉTENUS FLEX"), "—"),
    )
    grid(
        [
            {
                "Segment": g["segment"],
                "Titres": g["n"],
                "Digest initial": g["old"],
                "Nouveau pack": g["new"],
                "Détenus sans événement": ", ".join(g["missing"]),
            }
            for g in m["coverage"]
        ]
    )
    pos = sum(x["label"] == 1 for x in m["labels"])
    neg = sum(x["label"] == 0 for x in m["labels"])
    st.write(
        f"Échantillon annoté avant le test · {pos} lignes matérielles, {neg} autres lignes classées. {m['missed']} matérielle(s) manquée(s), {m['false_positive']} faux positif(s)."
    )
    st.caption(
        f"Doublons dans les listes transmises par titre · collecte initiale {m['duplicates_old']}, sélection événementielle {m['duplicates_new']}. Les lignes non tranchées de l’échantillon sont consultables ci-dessous."
    )
    c = m["cost"]
    st.caption(
        f"Coûts amont · {c['priced']} appels AG4 chiffrables sur {c['n_calls']}. Estimation partielle rapportée à un événement retenu {fmt(c['covered_usd_per_event'], ' USD', 4)}. Le coût complet par événement reste ouvert. La sélection locale ajoute 0 USD de facturation API."
    )
    grid([v for v in comparison(r) if v["Comparaison"] == NAMES["news_v2"]])
    with st.expander("Nouvelles, agrégats R8 et sources"):
        choices = {
            "Dernière collecte": n["latest"],
            **{x["day"] + " · " + x["run"]: x["news_file"] for x in r["cases"]},
        }
        key = st.selectbox("Pack à examiner", list(choices))
        data = read(choices[key])
        v = data.get("rebuilt", data)
        sym = st.selectbox("Titre du pack", sorted(set(v["r8"]) | set(v["pack"])))
        grid(
            [
                {"Version": k, **a.get(sym, {})}
                for k, a in [("Collecte initiale", v["r8"]), ("Série par événement", v["v2"])]
            ]
        )
        if "old" in data and "rebuilt" in data:
            st.write("Digest transmis aux modèles à cette date", data["old"].get(sym, []))
        ids = v["pack"].get(sym, [])
        grid(
            [
                {
                    "Événement": e["title"],
                    "Type": e["kind"],
                    "Disponible depuis": e["first"],
                    "Sources": ", ".join(sorted({a["source"] for a in e["sources"]})),
                    "Officiel": bool(e["official"]),
                    "Rectifié": bool(e["corrections"]),
                }
                for e in v["events"]
                if e["id"] in ids
            ]
        )
        for e in v["events"]:
            if e["id"] in ids:
                grid(e["sources"])
    with st.expander("Annotations et erreurs du sélecteur"):
        grid(m["labels"])


def opinions(r):
    bills = r["billing"]
    st.caption(
        "A applique la règle seule. B consulte le premier avis sur les dossiers déclenchés. C reprend les décisions approuvées du consensus. Chaque comparaison repart des mêmes avoirs. Le veto AG2 et le second avis disposent de variantes distinctes."
    )
    a, b, c = st.columns(3)
    a.metric("Sessions détaillées", len(r["cases"]))
    b.metric("Nouveaux appels reçus", sum(x["received"] for x in bills))
    c.metric("Coût estimé des essais", fmt(sum(x["eur"] for x in bills if x["eur"] is not None), " €"))
    st.caption(
        f"Plafond autorisé de 5 €. Conversion au change du {r['fx_asof']}. Les coûts des essais reposent sur les compteurs du fournisseur. Une cellule vide indique que le coût ou le résultat reste inconnu."
    )
    grid(
        [
            {
                "Workflow manuel": x["workflow"],
                "Modèle": x["requested"],
                "Format JSON imposé": x["structured"],
                "Réponses": x["count"],
                "Réponses valides": x["valid"],
                "Coût (USD)": x["usd"],
            }
            for x in bills
        ]
    )
    grid(comparison(r))
    with st.expander("Historique à vingt séances avant le lancement"):
        h = r["historical_votes"]
        st.caption(
            f"Du {h['start']} au {h['end']}. Écart signé face au fonds mondial, positif si le sens de l’idée était favorable. Les signaux répétés sur un titre le même jour sont regroupés. Les tailles, le cash et les frais ne sont pas intégrés à cette mesure. Les modèles ont évolué sur la période."
        )
        grid(
            [
                {
                    "Sélection": v["variant"],
                    "Sens": v["side"],
                    "Idées titre/jour": v["ideas"],
                    "Jours": v["days"],
                    "Écart moyen (points)": 100 * v["signed_mean"],
                    "Écart médian (points)": 100 * v["signed_median"],
                }
                for v in h["rows"]
            ]
        )
    if not r["cases"]:
        return
    sel = st.selectbox(
        "Session archivée", r["cases"][::-1], format_func=lambda v: v["day"] + " · " + v["run"]
    )
    st.write(
        f"Cash initial {fmt(sel['start_cash'], ' €')}. Données de décision identiques vérifiées pour les trois avis · {'oui' if sel['meta']['paired'] else 'non'}."
    )
    grid(
        [
            {
                "Variante": NAMES[k],
                "Propositions avec gardes communes": len(v["proposed"]),
                "Instructions": v["attempts"],
                "Exécutions simulées": v["fills"],
                "Achats après exécution (€)": v["outcome"]["sides"]["BUY"]["net_eur"],
                "Ventes après exécution (€)": v["outcome"]["sides"]["SELL"]["net_eur"],
                "Résultat après IA, bas (€)": v["outcome"]["after_ai_low"],
                "Résultat après IA, haut (€)": v["outcome"]["after_ai_high"],
            }
            for k, v in sel["arms"].items()
        ]
    )
    st.caption(
        "Les instructions peuvent être rejetées à l’arrivée. Un résultat de trading nul avec zéro exécution décrit le simulateur. Les coûts de fonctionnement partagés restent suivis au niveau du projet."
    )
    with st.expander("Votes, abstentions, dossiers et motifs"):
        grid(sel["meta"]["models"])
        st.write("Dossiers déclenchés", sel["meta"]["triggers"])
        st.write("Désaccords pouvant déclencher un second avis", sel["meta"]["disputes"])
        grid(sel["meta"]["ballot"])
        grid(sel["differences"])
        grid(sel["after_costs"])
        arm = st.selectbox("Variante", list(sel["arms"]), format_func=lambda k: NAMES[k])
        grid(
            [
                {k: x.get(k) for k in ("symbol", "side", "reason", "requested", "filled", "reject")}
                for x in sel["arms"][arm]["decisions"]
            ]
        )
        st.write("Ordres réellement transmis par V4", sel["actual_dispatch"])


def render():
    st.subheader(MENU)
    health()
    try:
        r = read("report.json")
    except (OSError, ValueError):
        st.warning("Rapport des expériences indisponible.")
        return
    tab = st.radio("Expérience", VIEWS, horizontal=True, key="experiment_view")
    {VIEWS[0]: exits, VIEWS[1]: news, VIEWS[2]: opinions}[tab](r)
