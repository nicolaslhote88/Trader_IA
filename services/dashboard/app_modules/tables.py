import html

import pandas as pd
import streamlit as st


def _coerce_for_sort(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")
    if num.notna().sum() >= max(3, int(len(series) * 0.5)):
        return num

    dt = pd.to_datetime(series, errors="coerce")
    if dt.notna().sum() >= max(3, int(len(series) * 0.5)):
        return dt

    return series.astype(str).str.lower()


def _apply_global_search(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query:
        return df
    as_str = df.astype(str)
    mask = as_str.apply(
        lambda col: col.str.contains(query, case=False, na=False, regex=False)
    ).any(axis=1)
    return df[mask]


def _apply_column_filters(df: pd.DataFrame, key_suffix: str) -> pd.DataFrame:
    if df.empty:
        return df

    columns = df.columns.tolist()
    selected_cols = st.multiselect(
        "Colonnes à filtrer",
        options=columns,
        key=f"tbl_filters_cols_{key_suffix}",
    )

    filtered = df

    for col in selected_cols:
        col_series = filtered[col]

        if pd.api.types.is_numeric_dtype(col_series):
            valid = pd.to_numeric(col_series, errors="coerce")
            if valid.notna().any():
                min_v = float(valid.min())
                max_v = float(valid.max())
                c1, c2 = st.columns(2)
                low = c1.number_input(
                    f"{col} min",
                    value=min_v,
                    key=f"tbl_{key_suffix}_{col}_num_min",
                )
                high = c2.number_input(
                    f"{col} max",
                    value=max_v,
                    key=f"tbl_{key_suffix}_{col}_num_max",
                )
                filtered = filtered[valid.between(low, high, inclusive="both")]
            continue

        dt = pd.to_datetime(col_series, errors="coerce")
        if dt.notna().sum() >= max(3, int(len(col_series) * 0.5)):
            min_d = dt.min().date()
            max_d = dt.max().date()
            rng = st.date_input(
                f"{col} période",
                value=(min_d, max_d),
                key=f"tbl_{key_suffix}_{col}_date",
            )
            if isinstance(rng, tuple) and len(rng) == 2:
                start_d, end_d = rng
                filtered = filtered[dt.dt.date.between(start_d, end_d, inclusive="both")]
            continue

        txt = st.text_input(
            f"{col} contient",
            value="",
            key=f"tbl_{key_suffix}_{col}_text",
        )
        if txt:
            filtered = filtered[col_series.astype(str).str.contains(txt, case=False, na=False, regex=False)]

        uniq = (
            col_series.dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        if 0 < len(uniq) <= 30:
            selected_values = st.multiselect(
                f"{col} valeurs",
                options=sorted(uniq),
                key=f"tbl_{key_suffix}_{col}_values",
            )
            if selected_values:
                filtered = filtered[col_series.astype(str).isin(selected_values)]

    return filtered


def _apply_sort(df: pd.DataFrame, key_suffix: str) -> pd.DataFrame:
    if df.empty:
        return df

    columns = df.columns.tolist()
    c1, c2 = st.columns([2, 1])
    sort_col = c1.selectbox(
        "Trier par",
        options=["(aucun)"] + columns,
        index=0,
        key=f"tbl_sort_col_{key_suffix}",
    )
    ascending = c2.toggle("Ascendant", value=False, key=f"tbl_sort_asc_{key_suffix}")

    if sort_col == "(aucun)":
        return df

    key_col = _coerce_for_sort(df[sort_col])
    out = df.assign(__sort_key=key_col).sort_values(
        "__sort_key", ascending=ascending, na_position="last"
    )
    return out.drop(columns=["__sort_key"])


def _format_cell_value(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, float):
        return f"{value:,.4g}" if abs(value) >= 10000 or (0 < abs(value) < 0.01) else f"{value:,.3f}".rstrip("0").rstrip(".")
    return str(value)


def _compact_columns(df: pd.DataFrame, max_compact_chars: int = 26) -> set[str]:
    compact: set[str] = set()
    if df is None or df.empty:
        return compact
    for col in df.columns:
        series = df[col]
        values = series.dropna().astype(str).head(80).tolist()
        longest = max([len(str(col)), *[len(v) for v in values]], default=0)
        mostly_numeric = pd.to_numeric(series, errors="coerce").notna().sum() >= max(1, int(len(series) * 0.70))
        mostly_bool = series.dropna().astype(str).str.lower().isin(["true", "false", "0", "1", "oui", "non"]).mean() >= 0.70 if len(series.dropna()) else False
        if mostly_numeric or mostly_bool or longest <= max_compact_chars:
            compact.add(str(col))
    return compact


def _is_total_candidate(column: str, series: pd.Series) -> bool:
    name = str(column).strip().lower()
    if not pd.to_numeric(series, errors="coerce").notna().any():
        return False
    non_additive_tokens = [
        "id", "prix", "price", "rate", "taux", "yield", "score", "rsi",
        "macd", "age", "date", "time", "at", "ratio", "leverage",
        "conviction", "confidence", "risk", "drawdown", "ret ",
        "return", "open_price", "close_price", "stop_loss_price",
        "take_profit_price", "last", "mid", "bid", "ask",
    ]
    additive_tokens = [
        "p&l", "pnl", "profit", "loss", "fees", "fee", "commission",
        "lot", "lots", "size", "qty", "quantity", "notional", "exposure",
        "solde", "balance", "cash", "equity", "margin", "ordre", "orders",
        "news", "impact", "source", "count", "nb ", "nombre", "montant",
        "amount", "valeur", "value", "part portefeuille", "poids portefeuille",
    ]
    if any(token in name for token in additive_tokens):
        return True
    return not any(token in name for token in non_additive_tokens)


def _build_total_row(df: pd.DataFrame) -> dict[str, object]:
    total: dict[str, object] = {str(col): "" for col in df.columns}
    if df.empty or len(df.columns) == 0:
        return total

    total[str(df.columns[0])] = "Total"
    numeric_total_cols = []
    for col in df.columns:
        series = pd.to_numeric(df[col], errors="coerce")
        if _is_total_candidate(str(col), df[col]):
            total[str(col)] = series.sum(skipna=True)
            numeric_total_cols.append(str(col))

    if len(df.columns) > 1:
        second_col = str(df.columns[1])
        if second_col not in numeric_total_cols:
            total[second_col] = f"{len(df)} lignes"
    return total


def _wrapped_table_css(table_id: str, height: int | None = None) -> str:
    max_h = f"max-height: {int(height)}px;" if height else ""
    return f"""
<style>
#{table_id}-wrap {{
  width: 100%;
  {max_h}
  overflow: auto;
  border: 1px solid rgba(148, 163, 184, 0.20);
  border-radius: 6px;
}}
#{table_id} {{
  width: 100%;
  border-collapse: collapse;
  table-layout: auto;
  font-size: 0.92rem;
}}
#{table_id} th {{
  position: sticky;
  top: 0;
  z-index: 1;
  background: rgb(30, 33, 42);
  color: rgba(226, 232, 240, 0.78);
  font-weight: 500;
}}
#{table_id} th,
#{table_id} td {{
  border: 1px solid rgba(148, 163, 184, 0.16);
  padding: 0.62rem 0.68rem;
  vertical-align: top;
  line-height: 1.42;
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: clip !important;
  overflow-wrap: anywhere;
  word-break: normal;
}}
#{table_id} th.fit,
#{table_id} td.fit {{
  width: auto;
  min-width: max-content;
  max-width: 24ch;
  white-space: nowrap !important;
  overflow-wrap: normal;
}}
#{table_id} th.wrap,
#{table_id} td.wrap {{
  min-width: 14rem;
}}
#{table_id} tbody tr:nth-child(even) {{
  background: rgba(148, 163, 184, 0.035);
}}
#{table_id} tfoot td {{
  position: sticky;
  bottom: 0;
  z-index: 1;
  background: rgb(24, 27, 35);
  color: #f8fafc;
  font-weight: 700;
  border-top: 2px solid rgba(148, 163, 184, 0.34);
}}
</style>
"""


def render_wrapped_dataframe(
    data,
    *,
    hide_index: bool = True,
    height: int | None = 420,
    key_suffix: str = "wrapped",
    fit_columns: list[str] | None = None,
    **_,
) -> None:
    """Render a dataframe-like object with readable wrapped text cells.

    Streamlit's native dataframe grid is excellent for dense numeric data, but it
    clips long text. This renderer is intentionally used for dashboard tables
    where readability matters more than spreadsheet-like virtualization.
    """
    if data is None:
        st.info("Aucune donnée.")
        return

    if hasattr(data, "data") and isinstance(getattr(data, "data", None), pd.DataFrame):
        df = data.data.copy()
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        try:
            df = pd.DataFrame(data)
        except Exception:
            st.write(data)
            return

    if df.empty:
        st.info("Aucune donnée.")
        return

    if not hide_index:
        df = df.reset_index()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(
                lambda v: "" if v is None else (str(v) if isinstance(v, (dict, list, tuple)) else v)
            )

    compact = _compact_columns(df)
    compact.update(str(c) for c in (fit_columns or []))
    table_id = (
        f"wrapped_tbl_{''.join(ch if ch.isalnum() else '_' for ch in str(key_suffix))}_"
        f"{abs(hash((tuple(map(str, df.columns)), len(df), id(df)))) % 1000000}"
    )

    header = "".join(
        f'<th class="{"fit" if str(col) in compact else "wrap"}">{html.escape(str(col))}</th>'
        for col in df.columns
    )
    rows = []
    for _, row in df.iterrows():
        cells = []
        for col in df.columns:
            cls = "fit" if str(col) in compact else "wrap"
            cells.append(f'<td class="{cls}">{html.escape(_format_cell_value(row.get(col)))}</td>')
        rows.append(f"<tr>{''.join(cells)}</tr>")
    total_row = _build_total_row(df)
    total_cells = []
    for col in df.columns:
        cls = "fit" if str(col) in compact else "wrap"
        total_cells.append(f'<td class="{cls}">{html.escape(_format_cell_value(total_row.get(str(col))))}</td>')

    st.markdown(
        f"""
{_wrapped_table_css(table_id, height)}
<div id="{table_id}-wrap">
  <table id="{table_id}">
    <thead><tr>{header}</tr></thead>
    <tbody>{''.join(rows)}</tbody>
    <tfoot><tr>{''.join(total_cells)}</tr></tfoot>
  </table>
</div>
""",
        unsafe_allow_html=True,
    )


def render_interactive_table(
    df: pd.DataFrame,
    key_suffix: str,
    *,
    hide_index: bool = True,
    height: int = 420,
    enable_controls: bool = True,
    styler_func=None,
) -> None:
    if df is None or df.empty:
        st.info("Aucune donnée.")
        return

    df_show = df.copy()

    # Harmonise les types affichables
    for col in df_show.columns:
        if df_show[col].dtype == "object":
            df_show[col] = df_show[col].map(
                lambda v: "" if v is None else (str(v) if isinstance(v, (dict, list, tuple)) else v)
            )

    if enable_controls:
        with st.expander("Filtres et tri", expanded=False):
            search = st.text_input(
                "Recherche globale",
                value="",
                key=f"tbl_search_{key_suffix}",
                help="Recherche sur toutes les colonnes",
            )
            df_show = _apply_global_search(df_show, search)
            df_show = _apply_column_filters(df_show, key_suffix)
            df_show = _apply_sort(df_show, key_suffix)

    st.caption(f"{len(df_show)} ligne(s) affichée(s) / {len(df)}")
    display_obj = styler_func(df_show) if styler_func is not None else df_show
    render_wrapped_dataframe(display_obj, hide_index=hide_index, height=height, key_suffix=key_suffix)
