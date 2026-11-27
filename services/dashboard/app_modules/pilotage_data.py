"""Lecture des publications atomiques et des écritures métier du VPS."""
from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
import duckdb
import streamlit as st

from . import verified_account
from .ui_kit import (account_signal, campaign_signal, decision_signal, freshness_signal,
                     lab_signal, official_signal, reconcile_signal, worst)

ROOT = Path("/files/duckdb")
FILES = {"account": "lot03_reporting/compte.json", "economic": "economic_reporting/report.json",
         "quotes": "economic_reporting/quotes.json", "research": "research_reporting/report.json",
         "experiments": "research_reporting/experiments/report.json", "campaign": "research_reporting/pilotage/report.json"}


@lru_cache(maxsize=18)
def _published(path, version):
    result = json.loads(path.read_bytes())
    return result if isinstance(result, dict) else {}


def published(name):
    path = ROOT / FILES[name]
    try:
        stat = path.stat()
        return _published(path, (stat.st_ino, stat.st_mtime_ns, stat.st_size))
    except (OSError, ValueError):
        return {}


@st.cache_data(ttl=60, show_spinner=False)
def evidence():
    def query(name, sql):
        try:
            with duckdb.connect(str(ROOT / (name + ".duckdb")), read_only=True) as db:
                return db.execute(sql).fetchall()
        except duckdb.Error:
            return None
    def last(name, table, field):
        rows = query(name, f"select cast(max({field}) as varchar) from {table}")
        return rows[0][0] if rows else None
    # Les runs n8n réussis et les dates de modification des fichiers ne prouvent aucune écriture métier.
    book = """from core.portfolio_snapshot p join core.runs r using(run_id)
              where r.strategy_version='ag1_pf_v1_ibkr_reconcile' and p.total_value_eur is not null"""
    close = query("ag1_v4_consensus", "select cast(p.ts as varchar),p.total_value_eur " + book + " order by p.ts desc limit 1")
    captures = query("ag1_v4_consensus", "select distinct cast(p.ts as varchar) " + book + " and p.ts >= current_timestamp - interval '73 hours'")
    return dict(snapshot=close[0] if close else (None, None), pf_times=[r[0] for r in captures] if captures is not None else None,
                news=last("ag4_v3", "news_history", "updated_at"),
                technical=last("ag2_v3", "technical_signals", "updated_at"),
                fundamental=last("ag3_v2", "fundamentals_snapshot", "updated_at"))


def broker(url, account=None):
    report = published("account") if account is None else account
    expected = report.get("account")
    if not expected:
        return (None, "Identité du compte indisponible", None, None)
    return verified_account.probe(url, expected)


def context(url):
    data = {key: published(key) for key in FILES}
    now = datetime.now(timezone.utc)
    a, e, r, x, f = [data[k] for k in ("account", "economic", "research", "experiments", "campaign")]
    writes = evidence()
    live = broker(url, a)
    account = account_signal(live, a.get("positions"), f.get("pilot", {}), f.get("observed"), now)
    snapshots = writes["snapshot"]
    if account.state == "rouge":
        account = account._replace(at=snapshots[0], detail=account.detail+" · contrôle du direct à "+str(live[0] or "date indisponible")[:16]+" UTC")
    run = a.get("activity", {}).get("strategy_v4_consensus")
    blocked = max(f.get("blocked", []), key=lambda v: v.get("at", ""), default=None)
    signals = {
        "Sources": freshness_signal(now, [writes["news"], data["quotes"].get("at")], 24 * 60, "Actualités générales et cotations publiées."),
        "Analyses AG2 et AG3": freshness_signal(now, [writes["technical"], writes["fundamental"]], 96 * 60, "Dernière écriture technique et fondamentale. Date la plus ancienne affichée."),
        "Décision AG1 de 17 h 10": decision_signal(now, run, account, blocked),
        "Compte réel": account, "Rapprochement du portefeuille": reconcile_signal(now, snapshots[0]),
        "Comptabilité officielle": official_signal(now, a, e), "Laboratoire": lab_signal(now, r, x, f)}
    data.update(now=now, signals=signals, writes=writes, live=live, test=campaign_signal(f),
                chain=worst([signals[k] for k in ("Sources", "Analyses AG2 et AG3", "Comptabilité officielle", "Laboratoire")]))
    return data
