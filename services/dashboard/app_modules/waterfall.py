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


def compute_financial_waterfall(
    *,
    init_cap: float,
    realized_closed_gross: float,
    realized_partial_gross: float,
    df_positions: pd.DataFrame | None,
    df_fill_costs: pd.DataFrame | None = None,
    df_transactions: pd.DataFrame | None = None,
    usd_cash: float | None = None,
    usd_eur_rate_now: float | None = None,
    usd_eur_rate_acq: float | None = None,
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
    bars: list[dict[str, object]] = [
        {"label": "Capital initial", "value": init_cap, "measure": "absolute",
         "kind": "base", "note": "Depot initial du portefeuille."},
        {"label": "P&L brut realise clos", "value": realized_closed_gross, "measure": "relative",
         "kind": "reel", "note": "Somme des P&L des lots clos, avant frais."},
        {"label": "Frais sur realise clos", "value": -fees_realized_real, "measure": "relative",
         "kind": "reel", "note": "Commissions reelles (fill_costs) attribuees au realise "
                                  "(= total enregistre - estimation des entrees ouvertes)."},
        {"label": "P&L brut realise partiel", "value": realized_partial_gross, "measure": "relative",
         "kind": "reel", "note": "P&L realise sur la fraction vendue de lots encore ouverts, avant frais."},
        {"label": "Frais de change (trades passes)", "value": -fx_conv_fees_est, "measure": "relative",
         "kind": "estime", "note": "Estimation barème FX IBKR (~0,2 bps, min 2 $) sur les conversions "
                                   "liees aux trades USD deja passes. Non enregistre en base."},
        {"label": "Frais sur realise partiel", "value": 0.0, "measure": "relative",
         "kind": "reel", "note": "Frais sur les ventes partielles (0 tant qu'aucun lot partiel)."},
        {"label": "P&L brut latent (prix)", "value": latent_price_eur, "measure": "relative",
         "kind": "reel", "note": "Performance prix des positions ouvertes (hors effet de change), au taux actuel."},
        {"label": "Frais simules sur vente latent", "value": -est_latent_exit_fees, "measure": "relative",
         "kind": "simule", "note": "Cout hypothetique pour vendre les positions actuelles (barème IBKR). Non engage."},
        {"label": "Resultat intermediaire", "value": 0.0, "measure": "total",
         "kind": "sous_total", "note": "Sous-total avant effets de change de rapatriement."},
        {"label": "Impact FX latent si liquide EUR", "value": latent_fx_eur, "measure": "relative",
         "kind": "reel", "note": "Effet de change deja porte par les positions non-EUR (colonne Impact FX), "
                                 "cristallise en cas de vente + rapatriement."},
        {"label": "Conversion devises -> EUR (sortie)", "value": -(exit_fx_conv_fee - fx_cash_impact), "measure": "relative",
         "kind": "estime", "note": exit_note},
        {"label": "Resultat final (sortie totale EUR)", "value": 0.0, "measure": "total",
         "kind": "total", "note": "Ce qu'il resterait en EUR si tu vendais tout et rapatriais hors IBKR maintenant, "
                                  "net de tous frais (vente + change)."},
    ]

    # Somme de controle du sous-total et du total (pour affichage/hover).
    running = init_cap
    intermediate = None
    for b in bars:
        if b["measure"] == "relative":
            running += safe_float(b["value"])
        elif b["measure"] == "total" and b["kind"] == "sous_total":
            intermediate = running
        elif b["measure"] == "total" and b["kind"] == "total":
            b_final = running

    meta = {
        "real_fees_total": real_fees_total,
        "est_open_entry_fees": est_open_entry_fees,
        "fees_realized_real": fees_realized_real,
        "fx_conv_fees_est": fx_conv_fees_est,
        "est_latent_exit_fees": est_latent_exit_fees,
        "latent_price_eur": latent_price_eur,
        "latent_fx_eur": latent_fx_eur,
        "fx_cash_impact": fx_cash_impact,
        "foreign_pos_value_eur": foreign_pos_value_eur,
        "exit_fx_conv_fee": exit_fx_conv_fee,
        "intermediate": intermediate if intermediate is not None else running,
        "final": running,
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
