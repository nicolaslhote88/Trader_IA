"""Cascade de valeur detaillee (onglet Rendement Financier).

Decompose le passage du capital initial a un resultat final *hypothetique*
(= liquidation totale + rapatriement en EUR, net de tous frais), en 12 postes.

Chaque poste est etiquete par sa nature :
  - "reel"    : mesure a partir de donnees enregistrees (lots, fill_costs, MTM).
  - "estime"  : calcule via un modele/barème (frais de change, frais IBKR)
                faute de donnee enregistree.
  - "simule"  : cout hypothetique non encore engage (frais de vente du latent).

IMPORTANT : les frais de change (poste 4), les frais simules sur le latent
(poste 7) et l'impact FX cash (poste 9) reposent sur des hypotheses ; ils sont
signales comme tels. Le "resultat final" n'est donc PAS la NAV actuelle mais
une estimation de ce qu'il resterait apres liquidation et rapatriement.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from app_modules.core import safe_float, safe_float_series

# ── Modele de frais IBKR (approximation, univers Actions) ───────────────────
# US (USD)      : 0,005 $/action, min 1 $, plafond 1 % du notionnel.
# Euronext (EUR): 0,05 % du notionnel, min 1,25 €.
IBKR_US_PER_SHARE_USD = 0.005
IBKR_US_MIN_USD = 1.0
IBKR_US_MAX_PCT = 0.01
IBKR_EU_PCT = 0.0005
IBKR_EU_MIN_EUR = 1.25

# Frais de change IBKR IDEALPRO : ~0,2 bps du montant converti, min 2 $.
FX_CONV_BPS = 0.00002
FX_CONV_MIN_USD = 2.0

# Base dédiée alimentée par le collecteur broker_costs (cf. outils/scripts/broker_costs_collector.py).
BROKER_COSTS_DB_CANDIDATES = (
    "/files/duckdb/broker_costs.duckdb",
    "/local-files/duckdb/broker_costs.duckdb",
)


def read_broker_costs(db_path: str | None = None) -> dict[str, object]:
    """Lit les coûts broker réels (cash par devise, conversions FX). Best-effort, read-only.

    Renvoie {usd_cash, usd_rate, fx_conv_fees_eur|None, has_real_fx}. Ne lève jamais.
    """
    out: dict[str, object] = {"usd_cash": None, "usd_rate": None, "fx_conv_fees_eur": None, "has_real_fx": False}
    paths = [db_path] if db_path else list(BROKER_COSTS_DB_CANDIDATES)
    for p in paths:
        if not p:
            continue
        try:
            import duckdb  # lazy : ne casse jamais le module si absent

            con = duckdb.connect(p, read_only=True)
        except Exception:
            continue
        try:
            row = con.execute(
                "SELECT cashbalance, exchangerate FROM cash_snapshots "
                "WHERE currency='USD' ORDER BY ts_day DESC LIMIT 1"
            ).fetchone()
            if row:
                out["usd_cash"] = safe_float(row[0])
                out["usd_rate"] = safe_float(row[1]) or None
            fx = con.execute(
                "SELECT COUNT(*), COALESCE(SUM(commission),0) FROM broker_trades WHERE sec_type='CASH'"
            ).fetchone()
            if fx and int(fx[0]) > 0:
                # commission FX facturée en devise cotée (USD) → conversion EUR au taux courant.
                rate = safe_float(out.get("usd_rate")) or 1.0
                out["fx_conv_fees_eur"] = abs(safe_float(fx[1])) * rate
                out["has_real_fx"] = True
            return out
        except Exception:
            return out
        finally:
            try:
                con.close()
            except Exception:
                pass
    return out


def ibkr_est_fee_eur(currency: str, qty: float, price_loc: float, usd_eur_rate: float = 1.0) -> float:
    """Frais de courtage IBKR estimes (une jambe), en EUR."""
    qty = abs(safe_float(qty))
    price = abs(safe_float(price_loc))
    if qty <= 0:
        return 0.0
    notional_loc = qty * price
    ccy = str(currency or "EUR").strip().upper()
    rate = safe_float(usd_eur_rate) or 1.0
    if ccy == "USD":
        fee_usd = max(IBKR_US_PER_SHARE_USD * qty, IBKR_US_MIN_USD)
        if notional_loc > 0:
            fee_usd = min(fee_usd, IBKR_US_MAX_PCT * notional_loc)
        return fee_usd * rate
    # Euronext / autres marches EUR
    if notional_loc <= 0:
        return 0.0
    return max(IBKR_EU_PCT * notional_loc, IBKR_EU_MIN_EUR)


def _is_usd(currency: str) -> bool:
    return str(currency or "").strip().upper() == "USD"


def _sum_col(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(safe_float_series(df[col]).sum())


def read_cash_balances(db_path: str | None = None) -> list[dict[str, object]]:
    """Cash IBKR par devise (hors agregat BASE), dernier snapshot quotidien du collecteur.

    Renvoie [{currency, balance, rate_eur, ts_day}] trie EUR d'abord puis par
    contre-valeur EUR decroissante. Best-effort : liste vide si base absente.
    """
    paths = [db_path] if db_path else list(BROKER_COSTS_DB_CANDIDATES)
    for p in paths:
        if not p:
            continue
        try:
            import duckdb

            con = duckdb.connect(p, read_only=True)
        except Exception:
            continue
        try:
            rows = con.execute(
                """
                SELECT currency, cashbalance, exchangerate, ts_day
                FROM cash_snapshots
                WHERE ts_day = (SELECT max(ts_day) FROM cash_snapshots)
                  AND UPPER(currency) <> 'BASE'
                """
            ).fetchall()
            out = [
                {"currency": str(r[0]).upper(), "balance": safe_float(r[1]),
                 "rate_eur": safe_float(r[2]) or 1.0, "ts_day": r[3]}
                for r in rows
            ]
            out.sort(key=lambda x: (x["currency"] != "EUR", -abs(x["balance"] * x["rate_eur"])))
            return out
        except Exception:
            return []
        finally:
            try:
                con.close()
            except Exception:
                pass
    return []


def compute_financial_waterfall(
    *,
    init_cap: float,
    realized_closed_gross: float,
    realized_partial_gross: float,
    df_positions: pd.DataFrame | None,
    df_fill_costs: pd.DataFrame | None = None,
    df_transactions: pd.DataFrame | None = None,
    df_open_lots_fx: pd.DataFrame | None = None,
    usd_cash: float | None = None,
    usd_eur_rate_now: float | None = None,
    usd_eur_rate_acq: float | None = None,
    total_val: float | None = None,
    dividends: float = 0.0,
    broker_costs_db: str | None = None,
    use_broker_costs: bool = True,
) -> dict[str, object]:
    """Calcule les postes de la cascade. Renvoie {bars: [...], meta: {...}}.

    bars : liste ordonnee de dict {label, value, measure, kind, note}.
      measure ∈ {absolute, relative, total}.
      kind    ∈ {base, reel, estime, simule, sous_total, total}.
    """
    init_cap = safe_float(init_cap)
    realized_closed_gross = safe_float(realized_closed_gross)
    realized_partial_gross = safe_float(realized_partial_gross)

    pos = df_positions.copy() if isinstance(df_positions, pd.DataFrame) and not df_positions.empty else pd.DataFrame()

    # Coûts broker réels (cash USD, conversions FX) depuis la base collecteur.
    bc = read_broker_costs(broker_costs_db) if use_broker_costs else {}
    if usd_cash is None and bc.get("usd_cash") is not None:
        usd_cash = safe_float(bc.get("usd_cash"))
    if usd_eur_rate_now is None and bc.get("usd_rate") is not None:
        usd_eur_rate_now = safe_float(bc.get("usd_rate"))
    fx_fees_real_eur = bc.get("fx_conv_fees_eur") if bc.get("has_real_fx") else None

    # ── Frais reels enregistres (fill_costs) ────────────────────────────────
    real_fees_total = _sum_col(df_fill_costs, "commission_eur")

    # Estimation des frais d'entree sur les positions encore ouvertes (barème),
    # pour isoler la part "realisee" des frais reels enregistres.
    est_open_entry_fees = 0.0
    est_latent_exit_fees = 0.0
    latent_price_eur = 0.0
    latent_fx_eur = 0.0
    foreign_pos_value_eur = 0.0  # valeur EUR des positions non-EUR (a reconvertir a la sortie)
    if not pos.empty:
        for r in pos.itertuples(index=False):
            ccy = str(getattr(r, "currency", "EUR") or "EUR")
            qty = safe_float(getattr(r, "quantity", 0))
            avg_loc = safe_float(getattr(r, "avg_loc", 0))
            last_loc = safe_float(getattr(r, "last_loc", 0))
            fx_now = safe_float(getattr(r, "fx_rate", 1.0)) or 1.0
            fx_entry = safe_float(getattr(r, "fx_rate_entry", fx_now)) or fx_now
            est_open_entry_fees += ibkr_est_fee_eur(ccy, qty, avg_loc, fx_entry)
            est_latent_exit_fees += ibkr_est_fee_eur(ccy, qty, last_loc, fx_now)
            if ccy.strip().upper() != "EUR":
                mv = safe_float(getattr(r, "mktval_eur", 0))
                if mv <= 0:
                    mv = abs(qty) * last_loc * fx_now
                foreign_pos_value_eur += mv
        latent_price_eur = _sum_col(pos, "pnl_prix_eur")
        latent_fx_eur = _sum_col(pos, "pnl_fx_eur")

    # Frais reels attribues au realise = total reel - part estimee des entrees ouvertes.
    fees_realized_real = max(0.0, real_fees_total - est_open_entry_fees)

    # ── Frais de change estimes (poste 4) ───────────────────────────────────
    # Barème FX IBKR applique aux montants convertis, deduits des trades sur
    # titres USD (achats = conversion EUR->USD, ventes = USD->EUR).
    fx_conv_fees_est = 0.0
    if isinstance(df_transactions, pd.DataFrame) and not df_transactions.empty:
        ccy_map: dict[str, str] = {}
        if not pos.empty and "symbol" in pos.columns and "currency" in pos.columns:
            for _, pr in pos.iterrows():
                s = str(pr.get("symbol") or "").strip().upper()
                if s:
                    ccy_map[s] = str(pr.get("currency") or "").strip().upper()
        tx = df_transactions.copy()
        sym_col = "symbol" if "symbol" in tx.columns else None
        notio_col = "notional" if "notional" in tx.columns else None
        if sym_col and notio_col:
            rate = safe_float(usd_eur_rate_now) or 0.876
            for r in tx.itertuples(index=False):
                s = str(getattr(r, sym_col, "") or "").strip().upper()
                if not s:
                    continue
                ccy = ccy_map.get(s)
                is_usd = (ccy == "USD") if ccy else ("." not in s)  # heuristique si inconnu
                if not is_usd:
                    continue
                notio = abs(safe_float(getattr(r, notio_col, 0)))
                if notio <= 0:
                    continue
                # notional suppose en EUR (valeur du trade cote portefeuille EUR).
                fee_usd = max(FX_CONV_BPS * (notio / (rate or 0.876)), FX_CONV_MIN_USD)
                fx_conv_fees_est += fee_usd * (rate or 0.876)

    # ── Impact change latent VRAI (par les fills, migration FX 22/07) ────────
    # Depuis la normalisation des fills (price EUR + price_native + fx_rate_eur),
    # le FX latent reel = somme sur les lots ouverts de
    #   remaining * price_native * (taux_courant - taux_du_fill).
    # df_open_lots_fx : DataFrame (symbol, remaining_qty, price_native, fx_rate_eur)
    # prepare par le loader. None/vide -> fallback latent_fx_eur du tableau positions.
    fx_latent_true = None
    if isinstance(df_open_lots_fx, pd.DataFrame) and not df_open_lots_fx.empty:
        try:
            r_now = safe_float(usd_eur_rate_now) or 0.876
            acc = 0.0
            for rr in df_open_lots_fx.itertuples(index=False):
                pn = safe_float(getattr(rr, "price_native", None))
                fxf = safe_float(getattr(rr, "fx_rate_eur", None))
                rq = safe_float(getattr(rr, "remaining_qty", None))
                if pn and fxf and rq and abs(fxf - 1.0) > 1e-9:  # lots en devise seulement
                    acc += rq * pn * (r_now - fxf)
            fx_latent_true = acc
        except Exception:
            fx_latent_true = None
    fx_latent_used = fx_latent_true if fx_latent_true is not None else latent_fx_eur

    # ── Bouclage NAV : change cash & conversions FX (non traces localement) ──
    # Identite : NAV = capital + realise_net (frais clos inclus) + latent_prix_IBKR
    #            (frais d'achat deja dans le PRU IBKR) + FX latent vrai + change_cash.
    # Le residu de bouclage EST le change cash + frais des conversions EUR<->USD
    # (cash_ledger ne les trace pas encore) + petits ecarts de taux.
    latent_total_eur = latent_price_eur + latent_fx_eur
    realized_net_total = realized_closed_gross + realized_partial_gross
    realized_net_ibkr = None
    cash_fx_residual = 0.0
    if total_val is not None:
        realized_net_ibkr = (safe_float(total_val) - init_cap) - latent_total_eur
        cash_fx_residual = (safe_float(total_val) - init_cap) - (
            realized_net_total + latent_price_eur + fx_latent_used
        )

    # ── Conversion de la liquidite devise -> EUR a la sortie (dernier poste) ──
    # Scenario "je vends tout et je rapatrie en EUR maintenant" : on convertit le
    # cash devise + le produit de la vente des positions non-EUR. Cout = frais de
    # change IBKR (~0,2 bps, min 2 $) ; l'effet de taux lui-meme est deja porte par
    # la valorisation EUR (cash au taux courant, positions via l'impact FX latent).
    rate_now = safe_float(usd_eur_rate_now) or 0.876
    uc = safe_float(usd_cash) if usd_cash is not None else 0.0
    usd_cash_eur = uc * rate_now
    total_foreign_liq_eur = foreign_pos_value_eur + usd_cash_eur
    exit_fx_conv_fee = 0.0
    if total_foreign_liq_eur > 0:
        exit_fx_conv_fee = max(FX_CONV_BPS * total_foreign_liq_eur, FX_CONV_MIN_USD * rate_now)

    # Effet de taux sur le cash devise lui-meme (0 si base d'acquisition inconnue).
    fx_cash_impact = 0.0
    if uc > 0 and usd_eur_rate_now and usd_eur_rate_acq and usd_eur_rate_acq > 0:
        fx_cash_impact = uc * (safe_float(usd_eur_rate_now) - safe_float(usd_eur_rate_acq))

    cash_bit = f"{uc:,.0f} $ de cash + " if uc > 0 else "cash devise non instrumente + "
    exit_note = (
        f"Rapatriement de {total_foreign_liq_eur:,.0f} EUR de liquidite en devise ({cash_bit}"
        f"{foreign_pos_value_eur:,.0f} EUR de positions non-EUR vendues), frais de change IBKR."
    )

    # ── Assemblage des postes ───────────────────────────────────────────────
    # Depuis la migration FX du 22/07, les fills sont normalises en EUR
    # (chaque jambe convertie au taux de son jour) et realized_pnl_eur est un
    # NET de frais economique. Plus de barre frais globale (les frais clos sont
    # dans le realise, les frais d'achat des ouvertes dans le PRU IBKR du
    # latent) ; plus de plug opaque : le bouclage NAV est porte par une barre
    # explicite "change cash & conversions FX".
    bars: list[dict[str, object]] = [
        {"label": "Capital initial", "value": init_cap, "measure": "absolute",
         "kind": "base", "note": "Depot initial du portefeuille."},
        {"label": "P&L realise net clos", "value": realized_closed_gross, "measure": "relative",
         "kind": "reel", "note": "Somme des P&L des lots clos, en EUR economique (chaque jambe au taux de "
                                 "change de son jour), NET des frais broker des trades clos."},
        {"label": "P&L realise net sur lot partiel", "value": realized_partial_gross, "measure": "relative",
         "kind": "reel", "note": "P&L realise sur la fraction vendue de lots encore ouverts, EUR economique, "
                                 "net des frais de la fraction vendue."},
        {"label": "Resultat realise (net)", "value": 0.0, "measure": "total",
         "kind": "sous_total", "note": "Capital + realise net clos + realise net partiel. Les frais des trades "
                                       "clos sont deja comptes ; ceux des positions ouvertes sont dans le PRU "
                                       "IBKR de la barre latente."},
        {"label": "P&L latent (prix, IBKR)", "value": latent_price_eur, "measure": "relative",
         "kind": "reel", "note": "Performance prix des positions ouvertes au taux de change courant, source "
                                 "IBKR (unrealized) ; le PRU IBKR inclut les frais d'achat."},
        {"label": "Resultat hors change", "value": 0.0, "measure": "total",
         "kind": "sous_total", "note": "Realise + performance prix latente, avant l'effet de change sur les positions."},
        {"label": "Impact change latent (reel)", "value": fx_latent_used, "measure": "relative",
         "kind": "reel", "note": "Effet de change VRAI des positions en devise : remaining x prix_natif x "
                                 "(taux courant - taux du jour d'achat), calcule depuis les fills normalises "
                                 "(migration 22/07). N'est plus l'artefact de devise d'avant l'audit."},
        {"label": "Change cash & conversions FX (non trace)", "value": cash_fx_residual, "measure": "relative",
         "kind": "estime", "note": "Bouclage sur la NAV IBKR : change realise/latent sur le cash en devise et "
                                   "frais des conversions EUR<->USD, pas encore traces dans cash_ledger "
                                   "(backlog P2 : import historique IBKR Flex). Contient aussi les petits "
                                   "ecarts taux BCE vs execution."},
        {"label": "Valeur nette actuelle (NAV IBKR)", "value": 0.0, "measure": "total",
         "kind": "sous_total", "note": "Point de controle : correspond EXACTEMENT a la NAV reelle du compte "
                                       "IBKR (bandeau principal), le bouclage etant porte par la barre "
                                       "precedente (explicite, plus de plug cache)."},
        {"label": "Frais simules de sortie (vente + change)", "value": -(est_latent_exit_fees + exit_fx_conv_fee - fx_cash_impact),
         "measure": "relative", "kind": "simule",
         "note": "Cout HYPOTHETIQUE total de sortie : vente des positions actuelles (barème IBKR) + frais de "
                 "change du rapatriement EUR de la liquidite en devise. Non engage. " + exit_note},
        {"label": "Resultat final (sortie totale EUR)", "value": 0.0, "measure": "total",
         "kind": "total", "note": "Ce qu'il resterait en EUR si tu vendais tout et rapatriais hors IBKR maintenant, "
                                  "net des frais hypothetiques de vente + change."},
    ]

    # Somme de controle des sous-totaux et du total (pour affichage/hover).
    running = init_cap
    subtotals: dict[str, float] = {}
    final_val = running
    for b in bars:
        if b["measure"] == "relative":
            running += safe_float(b["value"])
        elif b["measure"] == "total":
            subtotals[str(b["label"])] = running
            if b["kind"] == "total":
                final_val = running
    realized_subtotal = subtotals.get("Resultat realise (net)")
    intermediate = subtotals.get("Resultat hors change")
    nav_checkpoint = subtotals.get("Valeur nette actuelle (NAV IBKR)")

    meta = {
        "real_fees_total": real_fees_total,
        "fx_conv_fees_est": fx_conv_fees_est,
        "cash_fx_residual": cash_fx_residual,
        "fx_latent_true": fx_latent_true,
        "fx_latent_used": fx_latent_used,
        "realized_net_ibkr": realized_net_ibkr,
        "nav_checkpoint": nav_checkpoint,
        "nav_real": safe_float(total_val) if total_val is not None else None,
        "nav_reconciled_ok": (nav_checkpoint is not None and total_val is not None
                              and abs(safe_float(nav_checkpoint) - safe_float(total_val)) < 0.5),
        "usd_cash": safe_float(usd_cash) if usd_cash is not None else None,
        "usd_cash_source": ("reel" if bc.get("usd_cash") is not None else "absent"),
        "est_latent_exit_fees": est_latent_exit_fees,
        "latent_price_eur": latent_price_eur,
        "latent_fx_eur": latent_fx_eur,
        "fx_cash_impact": fx_cash_impact,
        "foreign_pos_value_eur": foreign_pos_value_eur,
        "exit_fx_conv_fee": exit_fx_conv_fee,
        "realized_subtotal": realized_subtotal if realized_subtotal is not None else final_val,
        "intermediate": intermediate if intermediate is not None else final_val,
        "final": final_val,
    }
    return {"bars": bars, "meta": meta}


# ── Rendu Plotly ────────────────────────────────────────────────────────────
_KIND_TAG = {
    "estime": " · est.",
    "simule": " · sim.",
}


def _cumulative_levels(bars: list[dict[str, object]]) -> list[float]:
    """Niveaux atteints par la courbe cumulee (haut/bas de chaque marche)."""
    run = 0.0
    levels: list[float] = []
    for b in bars:
        measure = str(b.get("measure", "relative"))
        val = safe_float(b.get("value"))
        if measure == "absolute":
            run = val
            levels.append(run)
        elif measure == "relative":
            prev = run
            run = run + val
            levels.extend([prev, run])
        else:  # total : la barre vaut le cumul courant
            levels.append(run)
    return levels or [0.0]


def build_financial_waterfall_figure(
    bars: list[dict[str, object]],
    *,
    title: str = "Cascade de valeur detaillee",
    zoom: bool = True,
) -> go.Figure:
    x_labels = []
    y_vals = []
    measures = []
    hover = []
    for b in bars:
        tag = _KIND_TAG.get(str(b.get("kind")), "")
        x_labels.append(f"{b['label']}{tag}")
        y_vals.append(safe_float(b.get("value")))
        measures.append(str(b.get("measure", "relative")))
        kind = str(b.get("kind"))
        nature = {"reel": "Reel", "estime": "Estime", "simule": "Simule",
                  "base": "Base", "sous_total": "Sous-total", "total": "Total"}.get(kind, "")
        hover.append(f"<b>{b['label']}</b><br>{nature}<br>{b.get('note', '')}")

    # Etiquettes : delta pour les marches, cumul pour les barres "total" (bleues).
    texts = []
    _run = 0.0
    for v, m in zip(y_vals, measures):
        if m == "absolute":
            _run = v
            texts.append(f"{_run:,.0f}")
        elif m == "relative":
            _run += v
            texts.append(f"{v:,.0f}")
        else:  # total : afficher le cumul courant
            texts.append(f"<b>{_run:,.0f}</b>")

    fig = go.Figure(
        go.Waterfall(
            orientation="v",
            measure=measures,
            x=x_labels,
            y=y_vals,
            text=texts,
            textposition="outside",
            increasing={"marker": {"color": "#28a745"}},
            decreasing={"marker": {"color": "#dc3545"}},
            totals={"marker": {"color": "#0d6efd"}},
            connector={"line": {"color": "#888"}},
            customdata=hover,
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    yaxis_cfg = dict(title="EUR", gridcolor="rgba(255,255,255,0.08)")
    annotations = []
    if zoom:
        levels = _cumulative_levels(bars)
        lo, hi = min(levels), max(levels)
        span = hi - lo
        if span <= 0:
            span = max(abs(hi) * 0.01, 1.0)
        # Marge : ~18 % de la variation en bas (place pour les barres/labels) et ~12 % en haut.
        y_floor = lo - span * 0.18
        y_ceiling = hi + span * 0.14
        yaxis_cfg.update(range=[y_floor, y_ceiling], autorange=False)
        annotations.append(
            dict(
                text="Echelle tronquee (zoom sur les variations)",
                x=0, y=1.06, xref="paper", yref="paper",
                showarrow=False, xanchor="left",
                font=dict(size=11, color="rgba(230,230,230,0.7)"),
            )
        )

    fig.update_layout(
        title=title,
        height=480,
        margin=dict(t=64, b=140, l=40, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickangle=-40, tickfont=dict(size=11)),
        yaxis=yaxis_cfg,
        annotations=annotations,
    )
    return fig
