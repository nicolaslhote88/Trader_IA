"""Règles de lecture communes aux écrans de pilotage."""
from datetime import date, datetime, time, timedelta, timezone
from html import escape
from typing import NamedTuple
from zoneinfo import ZoneInfo

from dateutil.easter import easter

PARIS = ZoneInfo("Europe/Paris")
RED, AMBER, GREEN, GREY = "rouge", "orange", "vert", "gris"
COLORS = {RED: "#ff7b85", AMBER: "#fd7e14", GREEN: "#4ade80", GREY: "#9aa0a6"}
MISSING = "Indisponible"


class Signal(NamedTuple):
    state: str
    title: str
    detail: str = ""
    at: str | None = None


def timestamp(value):
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(str(value))
        return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def age_minutes(now, value):
    stamp = timestamp(value)
    if stamp is None or stamp > now + timedelta(minutes=1):
        return None
    return (now - stamp).total_seconds() / 60


def date_fr(value):
    d = timestamp(value)
    if d is None:
        return MISSING
    if len(str(value)) == 10:
        return d.strftime("%d/%m/%Y")
    return d.astimezone(PARIS).strftime("%d/%m/%Y à %H h %M")


def fmt(value, unit="", digits=2):
    if value is None:
        return MISSING
    try:
        return f"{float(value):,.{digits}f}".replace(",", " ").replace(".", ",") + unit
    except (TypeError, ValueError):
        return MISSING


def pct(value):
    return fmt(None if value is None else value * 100, " %")


def trading_day(day):
    spring = easter(day.year)
    return day.weekday() < 5 and (day.month, day.day) not in {(1, 1), (5, 1), (12, 25), (12, 26)} and day not in {spring - timedelta(days=2), spring + timedelta(days=1)}


def account_signal(probe, positions, pilot, observed, now):
    at, status, nav, inventory = probe
    if status != "Compte réel connecté" or nav is None:
        detail = status
        age = age_minutes(now, observed)
        if pilot.get("paper") is True and age is not None and age <= 75:
            detail += " · compte de simulation selon la collecte du " + date_fr(observed)
        return Signal(RED, "Compte réel non connecté", detail, at)
    if positions is None:
        return Signal(AMBER, "Compte réel connecté", "Inventaire officiel indisponible.", at)
    expected = {str(r["conid"]): float(r["qty"]) for r in positions if float(r["qty"])}
    if inventory != expected:
        return Signal(AMBER, "Quantités à rapprocher", "Le direct diffère de la clôture officielle.", at)
    return Signal(GREEN, status, "Quantités conformes à la clôture officielle.", at)


def decision_signal(now, run, broker, blocked=None):
    local = now.astimezone(PARIS)
    day = local.date()
    if not trading_day(day):
        return Signal(GREY, "Hors séance", "Prochain contrôle un jour de bourse.", run)
    before = local.time() < time(17, 10)
    if before and broker.state == RED:
        return Signal(RED, "Décision menacée à 17 h 10", "Reconnecter le compte réel.", run)
    expected = day - timedelta(days=1) if before else day
    while not trading_day(expected):
        expected -= timedelta(days=1)
    stamp = timestamp(run)
    if stamp is None:
        return Signal(GREY, "Décision indisponible", "Aucune écriture de consensus publiée.")
    if stamp.astimezone(PARIS).date() < expected:
        return Signal(RED, "Décision attendue absente", "Dernier consensus inscrit au registre.", run)
    if blocked and timestamp(blocked.get("at")) and timestamp(blocked["at"]) >= stamp:
        return Signal(AMBER, "Décision avec incident", str(blocked.get("reason") or "Contrôle avant ordre"), run)
    return Signal(GREEN, "Consensus enregistré", "Décision datée dans le registre AG1.", run)


def reconcile_signal(now, at):
    minutes = age_minutes(now, at)
    if minutes is None:
        return Signal(GREY, "Rapprochement indisponible", "Aucune valorisation datée accessible.")
    local = now.astimezone(PARIS)
    if trading_day(local.date()) and minutes > 26 * 60:
        return Signal(RED, "Rapprochement interrompu", "Dernière valorisation réellement écrite.", at)
    if trading_day(local.date()) and 9 <= local.hour < 18 and minutes > 120:
        return Signal(AMBER, "Rapprochement en retard", "Plus de deux heures en séance.", at)
    return Signal(GREEN, "Rapprochement à jour", "Dernière valorisation réellement écrite.", at)


def official_signal(now, account, economic):
    p = economic.get("performance", {})
    end = timestamp(account.get("end"))
    pubs = [age_minutes(now, v) for v in (account.get("generated"), p.get("published"), economic.get("usage", {}).get("published"))]
    if not end or None in pubs:
        return Signal(GREY, "Publication incomplète", "Clôture ou date de publication indisponible.", account.get("end"))
    delay = (now.astimezone(PARIS).date() - end.date()).days
    issues = list(p.get("market_status", {}).get("issues", []))
    if delay > 3:
        issues.append(f"Relevé vieux de {delay} jours")
    if max(pubs) > 75:
        issues.append("Publication en retard")
    if economic.get("usage", {}).get("trace_errors"):
        issues.append("Traces IA incomplètes")
    return Signal(AMBER if issues else GREEN, "Comptabilité à compléter" if issues else "Comptabilité à jour", " · ".join(issues), account.get("end"))


def lab_signal(now, research, experiments, formal):
    checks = research.get("checks", {})
    at = research.get("observation")
    if checks.get("total") and checks.get("passed", 0) < checks["total"] or any(not x.get("ok") for x in experiments.get("checks", [])):
        return Signal(RED, "Contrôle en échec", "Consulter les preuves du laboratoire.", at)
    ages = [age_minutes(now, at), age_minutes(now, experiments.get("at")), age_minutes(now, formal.get("observed"))]
    if None in ages or not checks.get("total") or not experiments.get("checks"):
        return Signal(GREY, "Laboratoire incomplet", "Observation ou contrôles indisponibles.", at)
    late = ages[0] > 70 or ages[1] > 1440 or ages[2] > 75 or research.get("gaps") or formal.get("formal", {}).get("gaps")
    return Signal(AMBER if late else GREEN, "Collecte à vérifier" if late else "Collectes à jour", "Dernière observation de recherche.", at)


def campaign_signal(campaign):
    f = campaign.get("formal", {})
    rows = f.get("results", [])
    if any(r.get("reason") or r.get("verdict") == "abandonner" for r in rows):
        return Signal(RED, "Variante arrêtée", "Consulter les motifs du mandat.", f.get("last_day"))
    if rows and any(r.get("verdict") == "retenir" for r in rows):
        return Signal(GREEN, "Qualification publiée", "Revue des fenêtres disponible.", f.get("last_day"))
    sessions = f.get("sessions")
    detail = MISSING if sessions is None else f"{sessions} séance(s) formelle(s) enregistrée(s)."
    return Signal(GREY, "Observation en cours", detail, f.get("last_day"))


def freshness_signal(now, stamps, maximum, label):
    ages = [age_minutes(now, v) for v in stamps]
    valid = [v for v in stamps if timestamp(v)]
    at = min(valid, key=timestamp) if valid else None
    if not ages or None in ages:
        return Signal(GREY, "Écriture indisponible", label, at)
    late = max(ages) > maximum
    return Signal(AMBER if late else GREEN, "Écriture en retard" if late else "Écritures à jour", label, at)


def limit_signal(value, limit, mature=True):
    if not mature or value is None or limit is None:
        return GREY
    if value > limit:
        return RED
    return AMBER if value >= .8 * limit else GREEN


def worst(signals):
    order = [RED, AMBER, GREY, GREEN]
    return min(signals, key=lambda signal: order.index(signal.state))


def verdict_card(text):
    import streamlit as st
    with st.container(border=True):
        st.markdown(text)


def status_tile(label, signal):
    import streamlit as st
    st.markdown(f"<div style='padding:.7rem;background:#1e1e2e;border:1px solid #333;border-radius:8px;min-height:8rem'><small>{escape(label)}</small><br><strong style='color:{COLORS[signal.state]}'>● {escape(signal.title)}</strong><br>{escape(signal.detail)}<br><small style='color:#9aa0a6'>{escape(date_fr(signal.at))}</small></div>", unsafe_allow_html=True)


def freshness_chip(label, at, threshold):
    import streamlit as st
    age = age_minutes(datetime.now(timezone.utc), at)
    color = COLORS[GREY if age is None else AMBER if age > threshold else GREEN]
    st.markdown(f"<small style='color:{color}'>{escape(label)} · {escape(date_fr(at))}</small>", unsafe_allow_html=True)


def plotly_theme(fig, height=300):
    fig.update_layout(template="plotly_dark", paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                      font_color="#cfd3d8", margin=dict(l=12, r=16, t=15, b=20), height=height,
                      legend=dict(orientation="h", y=-.18))
    return fig


def chart(fig, height=300):
    import streamlit as st
    st.plotly_chart(plotly_theme(fig, height), width="stretch", theme=None, config={"displayModeBar": False})


def metrics(pairs):
    import streamlit as st
    for col, (label, value) in zip(st.columns(len(pairs)), pairs):
        col.metric(label, value)
