from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import gzip
import json
import pandas as pd
import streamlit as st
from app_modules.economic_monitor import fmt

ROOT = Path("/files/duckdb/research_reporting")
VIEWS = ("Univers de recherche", "Rejeu des décisions", "Portefeuilles simulés")
NAMES = {
    "ag3": "Classement AG3",
    "neutral": "Sélection neutre",
    "ag3_veto": "AG3 avec veto AG2",
    "neutral_veto": "Sélection neutre avec veto AG2",
}


def date_fr(s):
    return datetime.fromisoformat(s).astimezone(ZoneInfo("Europe/Paris")).strftime("%d/%m/%Y à %H:%M:%S")


def read(name):
    path = (ROOT / name).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError("Chemin de publication invalide")
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == ".gz" else raw)


def table(rows):
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.caption("Aucune ligne pour cette sélection.")


def inspect_health(report):
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(report["observation"])).total_seconds()
    ok = all(report["network"].values()) and report["checks"]["total"] == report["checks"]["passed"]
    msg = f"Recherche en simulation · dernière collecte le {date_fr(report['observation'])} · {report['checks']['passed']}/{report['checks']['total']} contrôles validés."
    if age > 70 * 60 or not ok:
        st.warning(msg + " Vérifier la collecte et le service de simulation.")
    else:
        st.caption(msg)


def health():
    try:
        inspect_health(read("report.json"))
    except (ValueError, OSError):
        st.warning("La publication de recherche est indisponible.")


def universe(rep):
    revisions = list(reversed(rep["census"]))
    rev = st.selectbox("Photographie datée", revisions, format_func=lambda r: date_fr(r["at"]))
    rows = read(rev["file"])
    history = rev == revisions[-1]
    cap = st.number_input(
        "Capital fictif en euros", min_value=0.0, value=float(rep["scenario"]["capital_eur"]), step=500.0
    )
    cash = cap * (1 - rep["scenario"]["cash_floor"])
    for r in rows:
        if history or r.get("block") or r.get("minimum_cost") is None:
            continue
        r["state"] = (
            "achat simulable" if r["minimum_cost"] <= cash else "suivi détenu" if r["held"] else "étude"
        )
        if r["minimum_cost"] > cash:
            r["block"] = ["ticket minimal supérieur au cash disponible"]
    counts = rev["counts"]
    cols = st.columns(4)
    for col, (label, num) in zip(
        cols,
        [
            ("Entrées sources", counts["sources"]),
            ("Dans le périmètre", counts["scoped"]),
            ("Présélection du soir", counts["pre"]),
            ("Lignes détenues", counts["held"]),
        ],
    ):
        col.metric(label, num)
    if history:
        st.caption(
            "Archive du 18 novembre. La fourchette et les permissions à l’instant d’envoi manquent pour établir l’achat possible à cette heure."
        )
    else:
        st.caption(
            f"{sum(r['state'] == 'achat simulable' for r in rows)} achat(s) simulable(s) au ticket minimal, avec {fmt(cash, ' €')} disponibles après réserve. La règle de portefeuille applique ensuite ses limites par ligne et par secteur."
        )
    st.caption(
        "L’accès historique correspond à un achat Flex antérieur sur le même contrat. La permission actuelle du compte reste à vérifier auprès d’IBKR. EUNL.DE figure en plus des entrées sources."
    )
    f = st.selectbox(
        "Population",
        [
            "Tous les titres",
            "Détenus",
            "Quarantaine",
            "Présélection du soir",
            "Achats simulables",
            "Fonds et produits à levier",
        ],
    )
    search = st.text_input("Rechercher un titre ou un contrat").strip().casefold()

    def ok(r):
        return (
            not search
            or any(search in str(r.get(k) or "").casefold() for k in ("symbol", "contract", "name"))
        ) and {
            "Tous les titres": True,
            "Détenus": r.get("held"),
            "Quarantaine": r.get("quarantine"),
            "Présélection du soir": r.get("pre"),
            "Achats simulables": r["state"] == "achat simulable",
            "Fonds et produits à levier": r["product"]
            in ("fonds mondial", "fonds sectoriel", "levier quotidien"),
        }[f]

    table(
        [
            {
                "Titre": r["symbol"],
                "Nom": r.get("name"),
                "État": r["state"],
                "Détenu": r["held"],
                "Segments": ", ".join(r.get("segments") or []),
                "Produit": r["product"],
                "Motifs": ", ".join(r.get("block") or []),
                "Quarantaine": r.get("reason"),
                "Contrat": r.get("contract"),
                "Place": r.get("place"),
                "Devise": r.get("currency"),
                "Lot": r.get("lot"),
                "Minimum": r.get("minimum"),
                "Nominal minimal (€)": r.get("minimum_nominal"),
                "Ticket avec frais (€)": r.get("minimum_cost"),
                "Séance ouverte": r.get("session_open"),
                "Accès historique": r.get("permission_day"),
                "Bid": r.get("bid"),
                "Ask": r.get("ask"),
                "Spread observé (%)": (
                    200 * (r["ask"] - r["bid"]) / (r["ask"] + r["bid"])
                    if r.get("ask") and r.get("bid") and r["ask"] >= r["bid"] > 0
                    else None
                ),
                "Fourchette observée (UTC)": r.get("quote_time"),
                "Volume": r.get("volume"),
                "Change": r.get("fx"),
                "Date du change": r.get("fx_day"),
            }
            for r in rows
            if ok(r)
        ]
    )
    with st.expander("Horaires et provenance d’un titre"):
        symbol = st.selectbox(
            "Instrument",
            sorted(r["symbol"] for r in rows),
            index=next((i for i, s in enumerate(sorted(r["symbol"] for r in rows)) if s == "SAP"), 0),
        )
        line = next(r for r in rows if r["symbol"] == symbol)
        hours = line.get("session_utc") or []
        st.write("Calendrier", line.get("calendar") or "inconnu")
        if hours:
            st.write(f"Séance de {date_fr(hours[0])} à {date_fr(hours[-1])} (Paris).")
        if len(hours) > 2 and hours[1]:
            st.write(f"Interruption de {date_fr(hours[1])} à {date_fr(hours[2])} (Paris).")
        st.write("Source du lot", str(line.get("lot_source") or "inconnue"))
        st.write("Dernier état des positions", str(line.get("held_asof") or rev["at"][:10]))
    with st.expander("Entrées, sorties et changements de segment"):
        st.caption(
            "Première photographie disponible le 18 novembre 2026. L’appartenance antérieure, les radiations historiques et les mouvements entre deux observations restent inconnus."
        )
        table(
            [
                {
                    "Titre": x["symbol"],
                    "Avant": json.dumps(x["before"], ensure_ascii=False),
                    "Après": json.dumps(x["after"], ensure_ascii=False),
                }
                for x in rev["changes"]
            ]
        )


def replay(rep):
    r = rep["replay"]
    den = r["ledger_runs"]
    n = r["complete_packs"]
    st.metric(
        "Sessions avec pack de décision détaillé",
        f"{n} / {den}",
        help="Dénominateur des sessions du journal contenant des interventions modèle.",
    )
    st.caption(
        f"Couverture {fmt(100 * n / den if den else None, ' %')}. Le moteur relit les décisions et les réponses archivées. Les modèles et le workflow n8n ne sont pas réexécutés."
    )
    for e in sorted(r["runs"], key=lambda x: x["run"], reverse=True):
        with st.expander(f"{e['run']} · {len(e['orders']) // 2} ordre(s)", expanded=True):
            if e["orders"]:
                table(
                    [
                        {
                            "Titre": o["symbol"],
                            "Sens": o.get("side"),
                            "Âge maximal (s)": o["age_limit"],
                            "Quantité Flex": o["actual_qty"],
                            "Prix Flex": o["actual_price"],
                            "Quantité simulée": o["filled"],
                            "Prix simulé": o.get("price"),
                            "Écart par titre": o.get("price_delta"),
                            "Résultat": o.get("reason") or o["status"],
                        }
                        for o in e["orders"]
                    ]
                )
            elif e["dispatch"]:
                st.write("Session clôturée sans ordre.")
            st.caption(
                f"{e['models']} interventions modèle · entrées {e['input']} · réponses {e['responses']}"
            )
    with st.expander("Recette et isolement du moteur"):
        table(
            [
                {"Contrôle": x["name"], "Réussi": x["ok"], "Preuve": x["detail"]}
                for x in rep["checks"]["cases"]
            ]
        )
        st.caption(
            "Les cas contrôlés utilisent un portefeuille indépendant. Les devises et les prix de transaction du rejeu sont ceux du contrat."
        )
        st.json(rep["network"])


def paper(rep):
    cfg = rep["scenario"]
    reviews = rep["reviews"]
    st.caption(
        f"Quatre portefeuilles de {fmt(cfg['capital_eur'], ' €', 0)} chacun. Revue à {cfg['review_paris']} heure de Paris du lundi au vendredi. Réserve de 10 %, cible de 15 % par ligne, six lignes au plus et 30 % par secteur. Horizon de 28 jours calendaires."
    )
    for k, day in rep.get("pauses", {}).items():
        st.info(f"{NAMES[k]} suspendu depuis le {day}. Les revues précédentes restent consultables.")
    if not reviews:
        st.info(
            "Première revue prospective attendue le 20 novembre 2026 à 17 h 18, heure de Paris. Les rendements seront publiés après observation."
        )
        table(
            [
                {"Portefeuille": NAMES[k], "Capital initial (€)": cfg["capital_eur"], "Séances observées": 0}
                for k in cfg["variants"]
            ]
        )
    else:
        sel = st.selectbox("Séance simulée", list(reversed(reviews)), format_func=lambda x: x["day"])
        table(
            [
                {
                    "Portefeuille": NAMES[x["variant"]],
                    "Liquidités (€)": x["cash"],
                    "À régler (€)": x["unsettled"],
                    "NAV à la revue (€)": x["nav"],
                    "Frais cumulés (€)": x["fees"],
                    "Rotation cumulée (%)": 100 * x["turnover"],
                    "Lignes": len(x["positions"]),
                    "Plus grand secteur (%)": 100 * max(x["sectors"].values(), default=0),
                    "Net à la clôture (%)": None if x.get("closing_net") is None else 100 * x["closing_net"],
                    **{
                        label: None if k not in x["excess"] else 100 * x["excess"][k]
                        for k, label in [
                            ("world", "Écart monde (points)"),
                            ("matched", "Écart monde à exposition égale (points)"),
                            ("basket", "Écart panier figé (points)"),
                            ("account", "Écart compte IBKR (points)"),
                        ]
                    },
                }
                for x in sel["results"]
            ]
        )
        st.caption(
            "La rotation additionne les montants achetés et vendus depuis le lancement, divisés par le capital initial. "
            "Les écarts requièrent des clôtures du même jour. Les comptes fictifs déduisent les frais et le change simulés. "
            "Les dividendes, les opérations sur titres et les frais de fonctionnement restent hors de ces rendements. "
            "Une cellule vide indique une donnée manquante."
        )
        results = read(sel["file"])["results"]
        if not results:
            st.info("Toutes les variantes étaient suspendues pour cette revue.")
            return
        v = st.selectbox("Décisions et alternatives", results, format_func=lambda x: NAMES[x["variant"]])
        table(
            [
                {
                    "Titre": a["symbol"],
                    "Rang": a["rank"],
                    "Secteur": a["sector"],
                    "Score": a["score"],
                    "Date du score": a["score_at"],
                    "Source": a["score_source"],
                    "Avis AG2": a["tech"],
                    "Quantité prévue": (a.get("decision") or {}).get("quantity"),
                    "Quantité exécutée": (a.get("order") or {}).get("filled"),
                    "Montant (€)": (a.get("order") or {}).get("nominal"),
                    "Coûts (€)": (a.get("order") or {}).get("cost"),
                    "Motif": ", ".join(a["reason"]),
                }
                for a in v["alternatives"]
            ]
        )
        if v["sales"]:
            table(v["sales"])
        st.download_button(
            "Télécharger les décisions de la séance",
            json.dumps(results, ensure_ascii=False),
            file_name="decisions-" + sel["day"] + ".json",
            mime="application/json",
        )
    if rep["gaps"]:
        table([{"Séance": g["day"], "Motif": g["reason"]} for g in rep["gaps"]])
    n, corr, spread, sample = rep["signal"]
    with st.expander("Origine du classement et version"):
        st.write(
            f"Audit sur {n} semaines et {sample} observations. Corrélation de rang moyenne de {fmt(corr, digits=3)}. L’écart hebdomadaire moyen entre les groupes haut et bas est de {fmt(spread * 100, ' point', 2)}. Ces chiffres décrivent le signal historique."
        )
        st.caption(
            "Les deux sélections neutres utilisent un ordre reproductible. Un verdict AG2 absent, périmé ou REJECT écarte un titre uniquement dans les deux variantes avec veto."
        )
        st.code(rep["version"])


def render(view):
    st.subheader(view)
    try:
        report = read("report.json")
        inspect_health(report)
        {VIEWS[0]: universe, VIEWS[1]: replay, VIEWS[2]: paper}[view](report)
    except (ValueError, OSError) as exc:
        st.warning("Lecture des résultats indisponible (" + type(exc).__name__ + ").")
