"""
Three Pillars Monitor — Onglet Dashboard
Affiche les scores des 3 piliers par devise + opportunités alignées + COT + yield curve.
À importer et appeler depuis app.py.
"""

import os
from datetime import date, timedelta

import duckdb
import pandas as pd


def load_macro_db(db_path: str) -> duckdb.DuckDBPyConnection | None:
    if not os.path.exists(db_path):
        return None
    try:
        return duckdb.connect(db_path, read_only=True)
    except Exception:
        return None


def render_three_pillars_tab(st):
    """Render the Three Pillars Monitor tab in Streamlit."""
    macro_db_path = os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb")
    con = load_macro_db(macro_db_path)

    st.header("Three Pillars Monitor — Framework Global Macro")

    if con is None:
        st.warning(f"Base de données macro non disponible : {macro_db_path}")
        st.info("Lancez AG5-FX-Macro, AG6-FX-Valuation et AG7-FX-Positioning pour alimenter les données.")
        return

    try:
        # ── Pillar Scores ──────────────────────────────────────────────────────
        st.subheader("Scores des 3 Piliers par Devise")
        scores_df = con.execute(
            """SELECT DISTINCT ON (currency) currency, as_of,
               macro_score, valuation_score, positioning_score,
               composite_score, all_pillars_aligned, crowded_flag
               FROM pillars.currency_scores
               ORDER BY currency, as_of DESC"""
        ).fetchdf()

        if not scores_df.empty:
            # Heatmap des scores
            G10 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
            scores_df = scores_df[scores_df["currency"].isin(G10)]

            pivot_data = scores_df.set_index("currency")[["macro_score", "valuation_score", "positioning_score"]].copy()
            pivot_data.columns = ["Pilier 1\nMacro/Flows", "Pilier 2\nValorisation", "Pilier 3\nPositionnement"]

            col1, col2 = st.columns([3, 1])
            with col1:
                try:
                    import plotly.graph_objects as go
                    fig = go.Figure(data=go.Heatmap(
                        z=pivot_data.values.tolist(),
                        x=pivot_data.columns.tolist(),
                        y=pivot_data.index.tolist(),
                        colorscale="RdYlGn",
                        zmid=0,
                        zmin=-1,
                        zmax=1,
                        text=[[f"{v:.2f}" if v is not None else "N/A" for v in row]
                              for row in pivot_data.values.tolist()],
                        texttemplate="%{text}",
                        showscale=True,
                    ))
                    fig.update_layout(
                        title="Heatmap des 3 Piliers G10",
                        height=350,
                        margin=dict(l=20, r=20, t=40, b=20),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except ImportError:
                    st.dataframe(pivot_data.style.background_gradient(cmap="RdYlGn", vmin=-1, vmax=1))

            with col2:
                st.markdown("**Opportunités alignées**")
                aligned = scores_df[scores_df["all_pillars_aligned"] == True].sort_values(
                    "composite_score", key=lambda x: x.abs(), ascending=False
                )
                if not aligned.empty:
                    for _, row in aligned.iterrows():
                        direction = "🟢 BULLISH" if (row["composite_score"] or 0) > 0 else "🔴 BEARISH"
                        st.write(f"**{row['currency']}** {direction}")
                        st.write(f"Score: {row['composite_score']:.2f}")
                else:
                    st.info("Aucune opportunité alignée actuellement")

            # Tableau détaillé
            display_df = scores_df[["currency", "as_of", "macro_score", "valuation_score",
                                     "positioning_score", "composite_score", "all_pillars_aligned", "crowded_flag"]].copy()
            display_df.columns = ["Devise", "Mis à jour", "Pilier 1 Macro", "Pilier 2 Valeur",
                                   "Pilier 3 Position", "Score Composite", "Alignés", "Crowded"]
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("Aucun score disponible. Lancez AG5/AG6/AG7 pour calculer les scores.")

        st.divider()

        # ── COT Positioning ────────────────────────────────────────────────────
        st.subheader("Positionnement COT (Commitment of Traders)")
        cot_df = con.execute(
            """SELECT DISTINCT ON (currency) currency, report_date, net_spec,
               net_z_score, crowded_flag, crowded_direction, positioning_score
               FROM cot.speculative_positions
               ORDER BY currency, report_date DESC"""
        ).fetchdf()

        if not cot_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Crowded Longs (à éviter)** 🚨")
                crowded_longs = cot_df[(cot_df["crowded_flag"] == True) & (cot_df["crowded_direction"] == "long")]
                if not crowded_longs.empty:
                    st.dataframe(crowded_longs[["currency", "net_z_score", "net_spec"]].rename(
                        columns={"currency": "Devise", "net_z_score": "Z-Score", "net_spec": "Position Nette"}
                    ))
                else:
                    st.success("Aucune devise crowded long")

            with col2:
                st.markdown("**Détestés (opportunités contrarian)** ✅")
                crowded_shorts = cot_df[(cot_df["crowded_flag"] == True) & (cot_df["crowded_direction"] == "short")]
                if not crowded_shorts.empty:
                    st.dataframe(crowded_shorts[["currency", "net_z_score", "net_spec"]].rename(
                        columns={"currency": "Devise", "net_z_score": "Z-Score", "net_spec": "Position Nette"}
                    ))
                else:
                    st.info("Aucune devise crowded short (hated)")

            try:
                import plotly.graph_objects as go
                fig = go.Figure(go.Bar(
                    x=cot_df["currency"].tolist(),
                    y=cot_df["net_z_score"].tolist(),
                    marker_color=["red" if abs(z) > 1.5 else "orange" if abs(z) > 1.0 else "green"
                                  for z in cot_df["net_z_score"].fillna(0).tolist()],
                    text=[f"{z:.2f}" for z in cot_df["net_z_score"].fillna(0).tolist()],
                    textposition="outside",
                ))
                fig.add_hline(y=1.5, line_dash="dash", line_color="red", annotation_text="Crowded Long")
                fig.add_hline(y=-1.5, line_dash="dash", line_color="green", annotation_text="Hated (contrarian)")
                fig.update_layout(title="Z-Score COT par Devise (52 semaines)", height=300, yaxis_title="Z-Score")
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.dataframe(cot_df)
        else:
            st.info("Aucune donnée COT disponible. Lancez AG7-FX-Positioning.")

        st.divider()

        # ── Yield Curve ────────────────────────────────────────────────────────
        st.subheader("Courbes des Taux Souverains G10")
        yc_df = con.execute(
            """SELECT DISTINCT ON (currency) currency, as_of,
               yield_2y_pct, yield_10y_pct, slope_10y2y, slope_change_30d, rates_signal
               FROM rates.yield_curve
               ORDER BY currency, as_of DESC"""
        ).fetchdf()

        if not yc_df.empty:
            try:
                import plotly.graph_objects as go
                yc_sorted = yc_df.sort_values("slope_10y2y", ascending=True)
                colors = ["red" if s == "steepener" else "orange" if s == "watch_steepener" else "steelblue"
                          for s in yc_sorted["rates_signal"].tolist()]
                fig = go.Figure(go.Bar(
                    x=yc_sorted["currency"].tolist(),
                    y=yc_sorted["slope_10y2y"].tolist(),
                    marker_color=colors,
                    text=[f"{s:.2f}%" for s in yc_sorted["slope_10y2y"].fillna(0).tolist()],
                    textposition="outside",
                ))
                fig.add_hline(y=0, line_color="black", line_width=1)
                fig.update_layout(title="Slope 10Y-2Y par Pays (négatif = courbe inversée = signal steepener)", height=300)
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                pass

            display_yc = yc_df[["currency", "yield_2y_pct", "yield_10y_pct", "slope_10y2y", "slope_change_30d", "rates_signal"]].copy()
            display_yc.columns = ["Devise", "Yield 2Y (%)", "Yield 10Y (%)", "Slope 10Y-2Y", "Δ Slope 30j", "Signal Taux"]
            st.dataframe(display_yc, use_container_width=True)

            steepeners = yc_df[yc_df["rates_signal"].isin(["steepener", "watch_steepener"])]
            if not steepeners.empty:
                st.warning(f"Signaux de pentification détectés : {', '.join(steepeners['currency'].tolist())} → Stratégie Long 2Y / Short 10Y")
        else:
            st.info("Aucune donnée de courbe des taux. Lancez AG8-FX-Rates.")

    finally:
        con.close()
