"""
Three Pillars Monitor — Onglet Dashboard (version complète)

Suit efficacement le travail des agents intermédiaires :
  AG5-FX-Macro      → Pilier 1 : indicateurs macro + scores
  AG6-FX-Valuation  → Pilier 2 : carry + PPP par devise
  AG7-FX-Positioning → Pilier 3 : COT z-scores + historique
  AG8-FX-Rates      → Courbe des taux + signaux steepener

Structure de l'onglet :
  Section 0 : Santé pipeline (derniers runs, fraîcheur, erreurs)
  Section 1 : Vue synthèse (heatmap + opportunités alignées)
  Section 2 : AG5 — Détail macro/flows par pays
  Section 3 : AG6 — Détail valorisation (carry + PPP)
  Section 4 : AG7 — COT positionnement (z-scores + historique)
  Section 5 : AG8 — Courbe des taux (slopes + signaux)
  Section 6 : Historique des scores (7j, 30j, 90j, all)
"""

import os
from datetime import date, datetime, timedelta, timezone

import duckdb
import pandas as pd

from app_modules.tables import render_wrapped_dataframe


# ── Helpers ────────────────────────────────────────────────────────────────────

def _connect(db_path: str) -> duckdb.DuckDBPyConnection | None:
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        return duckdb.connect(db_path, read_only=True)
    except Exception:
        return None


def _safe_df(con, query: str, params=None) -> pd.DataFrame:
    try:
        if params:
            return con.execute(query, params).fetchdf()
        return con.execute(query).fetchdf()
    except Exception:
        return pd.DataFrame()


def _freshness_badge(as_of_str: str | None, warn_days: int = 2, error_days: int = 7) -> tuple[str, str]:
    """Retourne (emoji, message) selon l'ancienneté de la donnée."""
    if not as_of_str:
        return "⚪", "Jamais"
    try:
        as_of = date.fromisoformat(str(as_of_str)[:10])
        days = (date.today() - as_of).days
        if days == 0:
            return "🟢", "Aujourd'hui"
        elif days == 1:
            return "🟢", "Hier"
        elif days <= warn_days:
            return "🟡", f"Il y a {days}j"
        elif days <= error_days:
            return "🟠", f"Il y a {days}j — attention"
        else:
            return "🔴", f"Il y a {days}j — données périmées"
    except Exception:
        return "⚪", str(as_of_str)


def _score_color(score: float | None) -> str:
    if score is None:
        return "gray"
    if score >= 0.5:
        return "darkgreen"
    if score >= 0.2:
        return "green"
    if score >= -0.2:
        return "gray"
    if score >= -0.5:
        return "red"
    return "darkred"


PILLAR_UNIVERSE = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]


def _render_score_reading_guide(st):
    st.markdown(
        """
Les scores sont normalisés entre `-1` et `+1`. Une valeur positive soutient la devise, une valeur négative la pénalise, et la zone `-0.20 / +0.20` doit être lue comme du bruit ou une absence de signal fort. Le composite n'est pas une prévision de prix: c'est une mesure de **pression macro relative** qui sert à cadrer les décisions Forex avec un horizon plus long que les news intraday.
"""
    )


def _render_data_universe_note(st):
    with st.expander("Périmètre devises et données manquantes", expanded=False):
        st.markdown(
            """
Le monitor 3 piliers couvre aujourd'hui le panier liquide pour lequel les trois familles de données sont comparables: `USD, EUR, JPY, GBP, CHF, CAD, AUD, NZD`. L'univers Forex du dashboard peut contenir davantage de devises ou de paires, mais une paire ne peut recevoir un vrai score structurel que si **les deux devises** ont:

- des données macro et taux directeurs (`macro.country_indicators`, `macro.policy_rates`),
- un score de valorisation/carry (`pillars.currency_scores` via AG6),
- un positionnement COT ou un proxy robuste (`cot.speculative_positions`),
- et, pour l'onglet taux, des rendements souverains 2Y/10Y (`rates.yield_curve`).

Pour compléter des devises comme `MXN`, `SEK`, `NOK` ou `KRW`, il faut ajouter les mappings de séries dans les clients de données (`fred_client`, `cot_client`, `rates_client`), puis élargir la constante de scoring côté `services/macro-data-api/scoring.py`. Sans cette couverture, afficher la devise donnerait une impression de précision qui n'existe pas encore.
"""
        )


def _render_coverage_table(st, con: duckdb.DuckDBPyConnection):
    rows = []
    queries = [
        ("Scores 3 piliers", "SELECT DISTINCT currency FROM pillars.currency_scores"),
        ("Indicateurs macro", "SELECT DISTINCT currency FROM macro.country_indicators"),
        ("Taux directeurs", "SELECT DISTINCT currency FROM macro.policy_rates"),
        ("COT positionnement", "SELECT DISTINCT currency FROM cot.speculative_positions"),
        ("Courbes 2Y/10Y", "SELECT DISTINCT currency FROM rates.yield_curve"),
    ]
    for label, query in queries:
        df = _safe_df(con, query)
        currencies = sorted(df["currency"].dropna().astype(str).str.upper().unique().tolist()) if not df.empty and "currency" in df.columns else []
        missing = [c for c in PILLAR_UNIVERSE if c not in currencies]
        extra = [c for c in currencies if c not in PILLAR_UNIVERSE]
        rows.append(
            {
                "Bloc": label,
                "Devises couvertes": ", ".join(currencies) if currencies else "-",
                "Manquantes G10 monitor": ", ".join(missing) if missing else "Aucune",
                "Hors périmètre vues mais non scorées": ", ".join(extra) if extra else "-",
            }
        )
    render_wrapped_dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ── Section 0 : Santé pipeline ─────────────────────────────────────────────────

def _render_pipeline_health(st, con: duckdb.DuckDBPyConnection):
    st.subheader("🔧 Santé du Pipeline — Agents Intermédiaires")
    st.markdown(
        """
Cet onglet répond à une question opérationnelle simple: **est-ce que les données qui alimentent les piliers sont fraîches, complètes et utilisables ?** Un score macro ancien peut être acceptable sur certains indicateurs trimestriels, mais un COT ou une courbe de taux périmés changent directement la confiance à accorder au composite.
"""
    )

    run_df = _safe_df(con, """
        SELECT run_id, started_at, finished_at, status, error_msg, records_written
        FROM pillars.run_log
        ORDER BY started_at DESC
        LIMIT 50
    """)

    agents = {
        "AG5-FX-Macro":       ("AG5MACRO", "Pilier 1 — Macro/Flows"),
        "AG6-FX-Valuation":   ("AG6VAL",   "Pilier 2 — Valorisation"),
        "AG7-FX-Positioning": ("AG7POS",   "Pilier 3 — Positionnement COT"),
        "AG8-FX-Rates":       ("AG8RATES", "Courbe des Taux"),
    }

    cols = st.columns(len(agents))
    for idx, (agent_name, (prefix, description)) in enumerate(agents.items()):
        with cols[idx]:
            if run_df.empty:
                st.metric(agent_name, "Jamais exécuté")
                st.caption(description)
                continue
            agent_runs = run_df[run_df["run_id"].str.startswith(prefix, na=False)]
            if agent_runs.empty:
                st.metric(agent_name, "⚪ Jamais")
                st.caption(description)
                continue
            last = agent_runs.iloc[0]
            status = str(last.get("status", "")).lower()
            emoji = "🟢" if status == "ok" else "🔴"
            finished = str(last.get("finished_at") or last.get("started_at") or "")
            badge, msg = _freshness_badge(finished[:10] if finished else None, warn_days=1, error_days=3)
            st.metric(f"{emoji} {agent_name}", badge + " " + msg)
            st.caption(f"{description} | {last.get('records_written', 0)} enreg.")
            if status != "ok" and last.get("error_msg"):
                st.error(f"Erreur : {str(last['error_msg'])[:120]}")

    # Tableau complet des runs récents
    with st.expander("Voir tous les runs récents"):
        if not run_df.empty:
            render_wrapped_dataframe(run_df.head(20), use_container_width=True)
        else:
            st.caption("Aucun run enregistré — les agents n'ont pas encore été exécutés.")


# ── Section 1 : Vue synthèse ───────────────────────────────────────────────────

def _render_summary(st, con: duckdb.DuckDBPyConnection):
    st.subheader("📊 Vue Synthèse — Scores des 3 Piliers G10")
    _render_score_reading_guide(st)
    _render_data_universe_note(st)

    scores_df = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, as_of,
          macro_score, valuation_score, positioning_score,
          composite_score, all_pillars_aligned, crowded_flag,
          macro_growth_score, macro_policy_score, macro_ca_score, macro_inflation_score,
          carry_score, ppp_deviation, cot_z_score
        FROM pillars.currency_scores
        ORDER BY currency, as_of DESC
    """)

    if not scores_df.empty:
        scores_df = scores_df[scores_df["currency"].isin(PILLAR_UNIVERSE)]

    if scores_df.empty:
        st.info("Aucun score calculé. Lancez AG5 → AG6 → AG7 → `/pillars/compute`.")
        return

    # Fraîcheur globale
    latest_date = scores_df["as_of"].max()
    badge, msg = _freshness_badge(str(latest_date), warn_days=1, error_days=3)
    st.caption(f"Données au : {badge} {latest_date} — {msg}")

    col_heatmap, col_ops = st.columns([3, 1])

    with col_heatmap:
        try:
            import plotly.graph_objects as go
            pivot = scores_df.set_index("currency")[
                ["macro_score", "valuation_score", "positioning_score"]
            ].reindex(PILLAR_UNIVERSE).fillna(float("nan"))
            pivot.columns = ["P1 Macro/Flows", "P2 Valorisation", "P3 Positionnement"]

            fig = go.Figure(data=go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale="RdYlGn",
                zmid=0, zmin=-1, zmax=1,
                text=[[f"{v:.2f}" if v == v else "—" for v in row]
                      for row in pivot.values.tolist()],
                texttemplate="%{text}",
                showscale=True,
            ))
            fig.update_layout(title="Heatmap 3 Piliers G10", height=340,
                              margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            render_wrapped_dataframe(scores_df.set_index("currency")[
                ["macro_score", "valuation_score", "positioning_score"]
            ])

    with col_ops:
        st.markdown("**Opportunités alignées**")
        aligned = scores_df[scores_df["all_pillars_aligned"] == True].sort_values(
            "composite_score", key=lambda x: x.abs(), ascending=False
        )
        if not aligned.empty:
            for _, row in aligned.iterrows():
                s = row["composite_score"] or 0
                arrow = "▲" if s > 0 else "▼"
                color = "green" if s > 0 else "red"
                st.markdown(f"**{row['currency']}** — composite: `{s:.2f}` {arrow}")
        else:
            st.info("Aucune devise avec les 3 piliers alignés")

        st.markdown("---")
        st.markdown("**⚠️ Crowded (à éviter)**")
        crowded = scores_df[scores_df["crowded_flag"] == True]
        if not crowded.empty:
            for _, row in crowded.iterrows():
                st.warning(f"{row['currency']} — COT z={row.get('cot_z_score', '?'):.2f}")
        else:
            st.success("Aucune devise crowded")

    # Tableau complet scores
    show_cols = ["currency", "as_of", "macro_score", "valuation_score", "positioning_score",
                 "composite_score", "all_pillars_aligned", "crowded_flag"]
    show_cols = [c for c in show_cols if c in scores_df.columns]
    render_wrapped_dataframe(
        scores_df[show_cols].rename(columns={
            "currency": "Devise", "as_of": "Date", "macro_score": "P1 Macro",
            "valuation_score": "P2 Valeur", "positioning_score": "P3 Position",
            "composite_score": "Composite", "all_pillars_aligned": "Alignés",
            "crowded_flag": "Crowded"
        }),
        use_container_width=True, hide_index=True
    )
    with st.expander("Couverture des données par bloc", expanded=False):
        _render_coverage_table(st, con)


# ── Section 2 : AG5 — Détail Macro/Flows ──────────────────────────────────────

def _render_ag5_macro(st, con: duckdb.DuckDBPyConnection):
    st.subheader("🌍 AG5 — Pilier 1 : Macro/Flows par Pays")
    st.markdown(
        """
Le Pilier 1 mesure la **qualité fondamentale des flux macro** qui soutiennent une devise: croissance, inflation, politique monétaire et balance courante. Une devise est mieux notée quand l'économie reste robuste, que la banque centrale offre un rendement réel crédible, et que les flux externes ne créent pas de pression vendeuse structurelle.

Les graphiques ne doivent pas être lus isolément. Un taux directeur élevé peut attirer du carry, mais il devient moins positif si l'inflation est hors contrôle ou si la croissance ralentit vite. La table des sous-scores permet donc de comprendre **pourquoi** le composite monte ou baisse.
"""
    )

    # Taux directeurs
    rates_df = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, as_of, rate_pct
        FROM macro.policy_rates ORDER BY currency, as_of DESC
    """)

    # Indicateurs économiques
    inds_df = _safe_df(con, """
        SELECT DISTINCT ON (currency, indicator) currency, indicator, value, as_of
        FROM macro.country_indicators
        ORDER BY currency, indicator, as_of DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Taux Directeurs Banques Centrales**")
        st.caption("Plus le taux est élevé relativement au taux neutre estimé, plus le capital court terme est rémunéré. Attention: un taux élevé peut aussi refléter un risque inflationniste.")
        if not rates_df.empty:
            rates_sorted = rates_df.sort_values("rate_pct", ascending=False)
            try:
                import plotly.graph_objects as go
                colors = ["green" if r > 3.0 else "orange" if r > 1.0 else "red"
                          for r in rates_sorted["rate_pct"].tolist()]
                fig = go.Figure(go.Bar(
                    x=rates_sorted["currency"].tolist(),
                    y=rates_sorted["rate_pct"].tolist(),
                    marker_color=colors,
                    text=[f"{r:.2f}%" for r in rates_sorted["rate_pct"].tolist()],
                    textposition="outside",
                ))
                fig.update_layout(title="Taux directeurs (%)", height=280, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                render_wrapped_dataframe(rates_sorted[["currency", "rate_pct", "as_of"]])
        else:
            st.caption("Aucune donnée — AG5 non exécuté")

    with col2:
        st.markdown("**Indicateurs Macro (dernière valeur)**")
        st.caption("Croissance et momentum renseignent le cycle; CPI mesure la pression inflationniste; balance courante mesure les flux externes structurels.")
        if not inds_df.empty:
            # Pivot : currency × indicator
            pivot_inds = inds_df.pivot_table(
                index="currency", columns="indicator", values="value", aggfunc="first"
            ).reset_index()
            # Renommer les colonnes pour lisibilité
            rename_map = {
                "gdp_growth_qoq": "PIB QoQ (%)",
                "cpi_yoy": "CPI YoY (%)",
                "current_account_bn_usd": "Balance CA (Mds$)",
                "gdp_momentum": "Momentum PIB",
            }
            pivot_inds = pivot_inds.rename(columns=rename_map)
            render_wrapped_dataframe(pivot_inds, use_container_width=True, hide_index=True)

            # Highlight la balance courante
            if "Balance CA (Mds$)" in pivot_inds.columns:
                st.caption("Balance CA positive = excédent = support devise (JPY, EUR) | négatif = déficit (USD)")
        else:
            st.caption("Aucune donnée — AG5 non exécuté ou FRED API non configurée")

    # Sub-scores détaillés (macro_growth_score, macro_policy_score, etc.)
    sub_df = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, as_of,
          macro_growth_score, macro_inflation_score, macro_policy_score, macro_ca_score, macro_score
        FROM pillars.currency_scores
        ORDER BY currency, as_of DESC
    """)
    if not sub_df.empty:
        with st.expander("Voir le détail des sous-scores Pilier 1"):
            render_wrapped_dataframe(
                sub_df.rename(columns={
                    "currency": "Devise", "as_of": "Date",
                    "macro_growth_score": "Score Croissance",
                    "macro_inflation_score": "Score Inflation",
                    "macro_policy_score": "Score CB Policy",
                    "macro_ca_score": "Score Balance CA",
                    "macro_score": "Score P1 Composite",
                }),
                use_container_width=True, hide_index=True
            )
            st.markdown(
                """
- `Score Croissance`: positif si le PIB accélère ou reste au-dessus du régime neutre.
- `Score Inflation`: positif quand l'inflation est proche de la cible; négatif en inflation excessive ou déflation.
- `Score CB Policy`: compare le taux directeur au taux neutre estimé par devise.
- `Score Balance CA`: excédent courant = soutien structurel; déficit important = pression vendeuse potentielle.
"""
            )


# ── Section 3 : AG6 — Détail Valorisation ─────────────────────────────────────

def _render_ag6_valuation(st, con: duckdb.DuckDBPyConnection):
    st.subheader("💱 AG6 — Pilier 2 : Valorisation (Carry + PPP)")
    st.markdown(
        """
Le Pilier 2 répond à la question: **la devise est-elle suffisamment rémunératrice ou décotée pour justifier un biais structurel ?** Il combine le carry, qui attire les capitaux tant que le risque reste maîtrisé, et une approximation PPP, qui signale les devises potentiellement sous/surévaluées par rapport à l'inflation relative.

Un score élevé ne veut pas dire "acheter immédiatement": il indique que le prix payé pour détenir la devise est plus favorable. Une devise peut rester chère ou bon marché longtemps; l'intérêt du pilier est surtout de filtrer les trades qui vont contre la valeur.
"""
    )

    val_df = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, as_of,
          carry_score, ppp_deviation, valuation_score
        FROM pillars.currency_scores
        ORDER BY currency, as_of DESC
    """)

    if val_df.empty:
        st.info("Aucune donnée — AG6-FX-Valuation non exécuté")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Score Carry (différentiel de taux normalisé)**")
        st.caption("Positif = taux élevé vs. G10 = attractif pour capitaux carry trade")
        carry_sorted = val_df.sort_values("carry_score", ascending=True)
        try:
            import plotly.graph_objects as go
            colors = ["green" if c > 0.2 else "red" if c < -0.2 else "gray"
                      for c in carry_sorted["carry_score"].fillna(0).tolist()]
            fig = go.Figure(go.Bar(
                x=carry_sorted["currency"].tolist(),
                y=carry_sorted["carry_score"].tolist(),
                marker_color=colors,
                text=[f"{c:.2f}" for c in carry_sorted["carry_score"].fillna(0).tolist()],
                textposition="outside",
            ))
            fig.add_hline(y=0, line_color="black", line_width=1)
            fig.update_layout(title="Carry Score par Devise", height=280, yaxis_range=[-1.1, 1.1])
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            render_wrapped_dataframe(carry_sorted[["currency", "carry_score"]])

    with col2:
        st.markdown("**Déviation PPP (sous/surévaluation relative)**")
        st.caption("Positif = sous-évalué vs. USD (inflation plus basse) = upside potentiel")
        ppp_sorted = val_df.sort_values("ppp_deviation", ascending=True)
        try:
            import plotly.graph_objects as go
            ppp_vals = ppp_sorted["ppp_deviation"].fillna(0).tolist()
            colors = ["green" if p > 0.05 else "red" if p < -0.05 else "gray" for p in ppp_vals]
            fig = go.Figure(go.Bar(
                x=ppp_sorted["currency"].tolist(),
                y=ppp_vals,
                marker_color=colors,
                text=[f"{p:.2f}" for p in ppp_vals],
                textposition="outside",
            ))
            fig.add_hline(y=0, line_color="black", line_width=1)
            fig.update_layout(title="Déviation PPP (+ = sous-évalué)", height=280, yaxis_range=[-1.1, 1.1])
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            render_wrapped_dataframe(ppp_sorted[["currency", "ppp_deviation"]])

    render_wrapped_dataframe(
        val_df.rename(columns={
            "currency": "Devise", "as_of": "Date",
            "carry_score": "Score Carry", "ppp_deviation": "PPP Déviation",
            "valuation_score": "Score P2 Composite"
        }),
        use_container_width=True, hide_index=True
    )
    with st.expander("Comment compléter le Pilier 2", expanded=False):
        st.markdown(
            """
La version actuelle utilise `carry_score` et `ppp_deviation`. Pour être plus complète, il faudrait ajouter un vrai proxy REER/NEER ou un indice de taux de change effectif réel par devise. Cela permettrait de mieux distinguer une devise simplement rémunératrice d'une devise réellement décotée face à son panier commercial.
"""
        )


# ── Section 4 : AG7 — COT Positionnement ──────────────────────────────────────

def _render_ag7_positioning(st, con: duckdb.DuckDBPyConnection):
    st.subheader("📋 AG7 — Pilier 3 : COT Positionnement Spéculatif")
    st.markdown(
        """
Le Pilier 3 mesure le **positionnement spéculatif**: quand trop d'acteurs sont déjà du même côté, le risque de squeeze ou de prise de profit augmente. Le COT est hebdomadaire, donc il ne sert pas à timer une entrée intraday; il sert à éviter de poursuivre un trade déjà saturé ou, au contraire, à repérer une devise détestée qui peut rebondir si le narratif change.
"""
    )

    # Dernière snapshot
    cot_latest = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, report_date, net_spec,
          lev_money_long, lev_money_short, asset_mgr_long, asset_mgr_short,
          net_z_score, crowded_flag, crowded_direction, positioning_score
        FROM cot.speculative_positions
        ORDER BY currency, report_date DESC
    """)

    if cot_latest.empty:
        st.info("Aucune donnée COT — AG7-FX-Positioning non exécuté ou CFTC non disponible")
        return

    latest_date = cot_latest["report_date"].max()
    badge, msg = _freshness_badge(str(latest_date), warn_days=7, error_days=14)
    st.caption(f"Rapport COT au : {badge} {latest_date} — {msg} (données hebdomadaires CFTC)")
    st.caption("Lecture: z-score positif = marché déjà long la devise; z-score négatif = marché short/hated. Le score P3 inverse ce signal: crowded long devient plutôt négatif, hated short devient plutôt positif.")

    # Graphique principal z-scores
    try:
        import plotly.graph_objects as go
        cot_sorted = cot_latest.sort_values("net_z_score", ascending=True)
        z_vals = cot_sorted["net_z_score"].fillna(0).tolist()
        bar_colors = [
            "darkred" if z > 2.0 else "red" if z > 1.5 else
            "orange" if z > 1.0 else
            "darkgreen" if z < -2.0 else "green" if z < -1.5 else
            "lightgreen" if z < -1.0 else "gray"
            for z in z_vals
        ]
        fig = go.Figure(go.Bar(
            x=cot_sorted["currency"].tolist(),
            y=z_vals,
            marker_color=bar_colors,
            text=[f"{z:.2f}" for z in z_vals],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Z-score: %{y:.2f}<br>Net spec: %{customdata:,}",
            customdata=cot_sorted["net_spec"].fillna(0).tolist(),
        ))
        fig.add_hline(y=1.5, line_dash="dash", line_color="red",
                      annotation_text="Crowded Long ≥ 1.5 (DANGER)")
        fig.add_hline(y=-1.5, line_dash="dash", line_color="green",
                      annotation_text="Hated / Contrarian ≤ -1.5 (OPPORTUNITÉ)")
        fig.update_layout(
            title="COT Z-Score par Devise (52 semaines) — Positif = Crowded Long | Négatif = Hated",
            height=350, yaxis_title="Z-Score", yaxis_range=[-3.5, 3.5]
        )
        st.plotly_chart(fig, use_container_width=True)
    except ImportError:
        pass

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**🚨 Crowded Longs (éviter)**")
        cl = cot_latest[(cot_latest["crowded_flag"] == True) & (cot_latest["crowded_direction"] == "long")]
        if not cl.empty:
            for _, r in cl.iterrows():
                st.error(f"{r['currency']}: z={r['net_z_score']:.2f} | net={r['net_spec']:,}")
        else:
            st.success("Aucun")

    with col2:
        st.markdown("**✅ Hated / Contrarian (opportunité)**")
        cs = cot_latest[(cot_latest["crowded_flag"] == True) & (cot_latest["crowded_direction"] == "short")]
        if not cs.empty:
            for _, r in cs.iterrows():
                st.success(f"{r['currency']}: z={r['net_z_score']:.2f} | net={r['net_spec']:,}")
        else:
            st.info("Aucun")

    with col3:
        st.markdown("**📊 Score Positionnement P3**")
        ps = cot_latest.sort_values("positioning_score", ascending=False)
        for _, r in ps.iterrows():
            score = r.get("positioning_score")
            if score is not None:
                bar = "█" * int(abs(float(score)) * 5)
                color = "green" if float(score) > 0 else "red"
                st.write(f"{r['currency']}: `{float(score):.2f}` {bar}")

    # Tableau détaillé
    with st.expander("Détail positions COT (Leveraged Money + Asset Managers)"):
        display = cot_latest[[
            "currency", "report_date", "net_spec",
            "lev_money_long", "lev_money_short", "asset_mgr_long", "asset_mgr_short",
            "net_z_score", "crowded_direction", "positioning_score"
        ]].rename(columns={
            "currency": "Devise", "report_date": "Date rapport",
            "net_spec": "Position Nette", "lev_money_long": "LevMon Long",
            "lev_money_short": "LevMon Short", "asset_mgr_long": "AssMgr Long",
            "asset_mgr_short": "AssMgr Short", "net_z_score": "Z-Score 52S",
            "crowded_direction": "Direction", "positioning_score": "Score P3"
        })
        render_wrapped_dataframe(display, use_container_width=True, hide_index=True)
        st.markdown(
            """
`Leveraged Money` reflète les acteurs spéculatifs rapides; `Asset Managers` reflète des positions plus institutionnelles. Le `net_spec` agrège long moins short, puis le z-score compare la position actuelle à son historique 52 semaines. Les seuils `+/-1.5` marquent les zones où le positionnement devient suffisamment extrême pour influencer le risque du trade.
"""
        )

    # Historique COT pour les 3 devises les plus extrêmes
    if not cot_latest.empty:
        extremes = cot_latest.reindex(
            cot_latest["net_z_score"].abs().sort_values(ascending=False).index
        ).head(3)["currency"].tolist()

        with st.expander(f"Historique COT (52 semaines) pour {', '.join(extremes)}"):
            cutoff = (date.today() - timedelta(weeks=54)).isoformat()
            hist_df = _safe_df(con,
                f"""SELECT currency, report_date, net_spec, net_z_score
                    FROM cot.speculative_positions
                    WHERE currency IN ({','.join(f"'{c}'" for c in extremes)})
                      AND report_date >= '{cutoff}'
                    ORDER BY currency, report_date""")
            if not hist_df.empty:
                try:
                    import plotly.express as px
                    fig = px.line(hist_df, x="report_date", y="net_z_score", color="currency",
                                  title="Z-Score COT historique", height=280)
                    fig.add_hline(y=1.5, line_dash="dash", line_color="red")
                    fig.add_hline(y=-1.5, line_dash="dash", line_color="green")
                    st.plotly_chart(fig, use_container_width=True)
                except ImportError:
                    render_wrapped_dataframe(hist_df)


# ── Section 5 : AG8 — Courbe des Taux ─────────────────────────────────────────

def _render_ag8_rates(st, con: duckdb.DuckDBPyConnection):
    st.subheader("📈 AG8 — Courbe des Taux Souverains G10")
    st.markdown(
        """
L'onglet AG8 isole la **structure de courbe souveraine**. La pente `10Y-2Y` résume l'équilibre entre politique monétaire court terme, attentes de croissance et prime de terme. Une courbe très inversée peut signaler un stress de cycle; une pentification après inversion peut marquer un changement de régime important pour la devise et les obligations.
"""
    )

    yc_df = _safe_df(con, """
        SELECT DISTINCT ON (currency) currency, as_of,
          yield_2y_pct, yield_10y_pct, slope_10y2y, slope_change_30d,
          steepening, rates_signal
        FROM rates.yield_curve
        ORDER BY currency, as_of DESC
    """)

    if yc_df.empty:
        st.info("Aucune donnée — AG8-FX-Rates non exécuté")
        return

    # Graphique slopes
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Slope 10Y-2Y par Pays**")
        st.caption("Négatif = courbe inversée; retour vers le haut = pentification. Le signal steepener surveille surtout le changement de régime, pas seulement le niveau.")
        try:
            import plotly.graph_objects as go
            yc_sorted = yc_df.sort_values("slope_10y2y", ascending=True)
            colors = [
                "darkred" if s == "steepener" else
                "orange" if s == "watch_steepener" else
                "lightblue" if s == "neutral" else "steelblue"
                for s in yc_sorted["rates_signal"].tolist()
            ]
            fig = go.Figure(go.Bar(
                x=yc_sorted["currency"].tolist(),
                y=yc_sorted["slope_10y2y"].fillna(0).tolist(),
                marker_color=colors,
                text=[f"{s:.2f}%" for s in yc_sorted["slope_10y2y"].fillna(0).tolist()],
                textposition="outside",
            ))
            fig.add_hline(y=0, line_color="black", line_width=1.5)
            fig.update_layout(
                title="Slope 10Y-2Y (rouge=steepener détecté)",
                height=300, yaxis_title="%"
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            render_wrapped_dataframe(yc_df[["currency", "slope_10y2y", "rates_signal"]])

    with col2:
        st.markdown("**Yields 2Y vs. 10Y par Pays**")
        try:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=yc_df["currency"].tolist(),
                y=yc_df["yield_2y_pct"].fillna(0).tolist(),
                name="Yield 2Y", marker_color="steelblue"
            ))
            fig.add_trace(go.Bar(
                x=yc_df["currency"].tolist(),
                y=yc_df["yield_10y_pct"].fillna(0).tolist(),
                name="Yield 10Y", marker_color="darkblue", opacity=0.7
            ))
            fig.update_layout(
                barmode="group", title="Yields souverains (%)",
                height=300, yaxis_title="%"
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            pass

    # Signaux et alertes
    steepeners = yc_df[yc_df["rates_signal"].isin(["steepener", "watch_steepener"])]
    if not steepeners.empty:
        for _, row in steepeners.iterrows():
            signal_type = "⚡ STEEPENER ACTIF" if row["rates_signal"] == "steepener" else "👀 WATCH STEEPENER"
            slope = row.get("slope_10y2y")
            change = row.get("slope_change_30d")
            msg = f"{signal_type} — {row['currency']}: slope={slope:.2f}%"
            if change is not None:
                msg += f" (Δ30j: {change:+.2f}%)"
            msg += " → Stratégie: Long obligations 2Y / Short obligations 10Y"
            st.warning(msg)

    # Tableau complet
    display_yc = yc_df.rename(columns={
        "currency": "Devise", "as_of": "Date", "yield_2y_pct": "Yield 2Y (%)",
        "yield_10y_pct": "Yield 10Y (%)", "slope_10y2y": "Slope 10Y-2Y",
        "slope_change_30d": "Δ Slope 30j", "steepening": "Pentification",
        "rates_signal": "Signal Taux"
    })
    render_wrapped_dataframe(display_yc, use_container_width=True, hide_index=True)
    with st.expander("Comment interpréter les tendances de courbe", expanded=False):
        st.markdown(
            """
- Pente qui baisse: le marché price davantage de ralentissement ou de politique restrictive.
- Pente très négative: stress de cycle; risque de récession ou de future détente monétaire.
- Pente qui remonte depuis un niveau négatif: possible steepener; souvent associé à un changement d'anticipations sur la banque centrale ou la croissance.
- Conséquence FX: la courbe aide à distinguer un carry encore solide d'un carry qui devient dangereux parce que le cycle se retourne.
"""
        )

    # Contexte thèse USD
    usd = yc_df[yc_df["currency"] == "USD"]
    if not usd.empty:
        usd_slope = usd.iloc[0].get("slope_10y2y")
        usd_signal = usd.iloc[0].get("rates_signal", "neutral")
        if usd_slope is not None and usd_slope < 0:
            st.info(f"Contexte USD : courbe inversée ({usd_slope:.2f}%) — confirme thèse 'fin exceptionnalisme américain' + steepener à venir")
        elif usd_signal == "steepener":
            st.success(f"USD : signal steepener actif — slope change = {usd.iloc[0].get('slope_change_30d', '?'):.2f}%")


# ── Section 6 : Historique des scores ─────────────────────────────────────────

def _render_score_history(st, con: duckdb.DuckDBPyConnection):
    st.subheader("📅 Historique des Scores")
    st.markdown(
        """
Cet onglet sert à vérifier la **direction de voyage** des piliers. Une photographie quotidienne peut être trompeuse; une série historique montre si un avantage macro se renforce, se tasse ou bascule. Les seuils `+0.20` et `-0.20` matérialisent la zone où le signal commence à devenir exploitable.
"""
    )

    window = st.radio("Fenêtre d'observation", ["7j", "30j", "90j", "all"], index=1, horizontal=True, key="score_hist_window")
    where_clause = ""
    if window != "all":
        days = int(window.replace("j", ""))
        cutoff = (date.today() - timedelta(days=days + 2)).isoformat()
        where_clause = f"WHERE as_of >= '{cutoff}'"
    hist_df = _safe_df(con,
        f"""SELECT currency, as_of, macro_score, valuation_score, positioning_score, composite_score
            FROM pillars.currency_scores
            {where_clause}
            ORDER BY currency, as_of""")

    if hist_df.empty:
        st.info("Historique non disponible — les agents doivent avoir tourné pendant plusieurs jours.")
        return

    currencies_with_data = [c for c in PILLAR_UNIVERSE if c in hist_df["currency"].unique()]
    if not currencies_with_data:
        st.info("Aucune devise avec historique")
        return

    selected = st.multiselect(
        "Devises à afficher",
        options=currencies_with_data,
        default=currencies_with_data,
        key="score_hist_select"
    )
    metric = st.selectbox(
        "Métrique",
        ["composite_score", "macro_score", "valuation_score", "positioning_score"],
        key="score_hist_metric"
    )

    filtered = hist_df[hist_df["currency"].isin(selected)]
    if not filtered.empty:
        try:
            import plotly.express as px
            chart_col, guide_col = st.columns([1.15, 0.85])
            with chart_col:
                fig = px.line(
                    filtered, x="as_of", y=metric, color="currency",
                    title=f"Historique {metric} par devise - {window}",
                    height=820,
                    markers=True,
                )
                fig.add_hline(y=0.20, line_dash="dot", line_color="green", annotation_text="Seuil bullish")
                fig.add_hline(y=-0.20, line_dash="dot", line_color="red", annotation_text="Seuil bearish")
                fig.add_hline(y=0.0, line_dash="dash", line_color="rgba(255,255,255,0.45)")
                fig.update_yaxes(range=[-1.1, 1.1])
                fig.update_layout(
                    margin=dict(l=10, r=10, t=52, b=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                )
                st.plotly_chart(fig, use_container_width=True)
            with guide_col:
                st.markdown("#### Interprétation")
                st.markdown(
                    """
- Ligne qui monte: la devise gagne du soutien sur la métrique sélectionnée.
- Ligne qui baisse: le soutien se détériore ou le risque structurel augmente.
- Passage au-dessus de `+0.20`: signal positif exploitable si les autres piliers confirment.
- Passage sous `-0.20`: signal négatif exploitable ou alerte de dégradation.
- Lignes plates: soit stabilité réelle, soit manque de nouvelles observations dans les agents.
"""
                )
                st.markdown("#### Métriques")
                render_wrapped_dataframe(
                    pd.DataFrame(
                        [
                            {"Métrique": "composite_score", "Lecture": "Moyenne des 3 piliers; meilleur résumé multi-horizon."},
                            {"Métrique": "macro_score", "Lecture": "Croissance, inflation, politique monétaire, balance courante."},
                            {"Métrique": "valuation_score", "Lecture": "Carry + PPP; indique si la devise est rémunératrice ou décotée."},
                            {"Métrique": "positioning_score", "Lecture": "Signal contrarian issu du COT; positif quand le marché est trop short."},
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
        except ImportError:
            render_wrapped_dataframe(filtered[["currency", "as_of", metric]])


# ── Entry point ────────────────────────────────────────────────────────────────

def render_three_pillars_tab(st):
    """Point d'entrée principal — appelé depuis app.py."""
    macro_db_path = os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb")
    con = _connect(macro_db_path)

    st.header("🧭 Three Pillars Monitor — Framework Global Macro")

    if con is None:
        st.error(f"Base de données macro non disponible : `{macro_db_path}`")
        st.info("""
        **Pour initialiser :**
        1. Démarrer le service `macro-data-api` : `docker-compose up -d macro-data-api`
        2. Ajouter `FRED_API_KEY` dans le `.env` VPS
        3. Appeler `POST /macro/refresh_all` puis `POST /macro/cot/refresh`
        4. Appeler `POST /pillars/compute`
        """)
        return

    try:
        tabs = st.tabs([
            "🔧 Santé Pipeline",
            "📊 Vue Synthèse",
            "🌍 AG5 Macro/Flows",
            "💱 AG6 Valorisation",
            "📋 AG7 Positionnement COT",
            "📈 AG8 Courbe des Taux",
            "📅 Historique Scores",
        ])
        with tabs[0]:
            _render_pipeline_health(st, con)
        with tabs[1]:
            _render_summary(st, con)
        with tabs[2]:
            _render_ag5_macro(st, con)
        with tabs[3]:
            _render_ag6_valuation(st, con)
        with tabs[4]:
            _render_ag7_positioning(st, con)
        with tabs[5]:
            _render_ag8_rates(st, con)
        with tabs[6]:
            _render_score_history(st, con)
    finally:
        con.close()
