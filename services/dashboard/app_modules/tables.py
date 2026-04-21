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


def render_interactive_table(
    df: pd.DataFrame,
    key_suffix: str,
    *,
    hide_index: bool = True,
    height: int = 420,
    enable_controls: bool = True,
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
    st.dataframe(df_show, use_container_width=True, hide_index=hide_index, height=height)
