"""
app_director.py — Dashboard interactivo para el Director Financiero.

Fuente de datos: datos_director.parquet (generado por etl_director.py).
NO requiere conexión a BD, ni Excel, ni Red.

Tabs:
  📊 Resumen   — 4 KPIs + gráficos (top gerencias, evolución mensual)
  🔍 Explorador — PyGWalker: el director arma sus propios KPIs

Variables de entorno:
  DATOS_DIRECTOR_PATH   ruta al parquet (fallback: output_director/ relativo)

Ejecución:
  streamlit run projects/report_direccion/src/dashboard/app_director.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Constantes y rutas
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).parent  # src/dashboard/
_DEFAULT_PARQUET = (
    _THIS_DIR.parents[3]  # richard_ia86_dev/
    / "report_gerencias"
    / "output_director"
    / "datos_director.parquet"
)

_VERSION_APP = "v1.0 — Demo DESPACHOS"
_FUENTE_LABEL = "Fuente: DESPACHOS CAC"


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def _cargar_datos() -> pd.DataFrame:
    """Lee el parquet desde env var o ruta por defecto."""
    env_path = os.environ.get("DATOS_DIRECTOR_PATH", "")
    ruta = Path(env_path) if env_path else _DEFAULT_PARQUET

    if not ruta.exists():
        st.error(
            f"Parquet no encontrado: `{ruta}`\n\n"
            "Ejecutá primero `etl_director.py` para generarlo."
        )
        st.stop()

    df = pd.read_parquet(ruta)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Encabezado fijo
# ---------------------------------------------------------------------------
def _encabezado_fijo() -> None:
    """Inyecta encabezado fijo y estilos globales."""
    st.markdown(
        f"""
        <style>
        .header-pose {{
            position: fixed;
            top: 2.875rem;
            left: 0;
            right: 0;
            z-index: 999;
            background: rgba(14, 17, 23, 0.96);
            border-bottom: 1px solid #333;
            padding: 0.45rem 1.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
        }}
        .header-pose .titulo {{
            font-size: 1.05rem;
            font-weight: 700;
            color: #ffffff;
        }}
        .header-pose .version {{
            font-size: 0.78rem;
            color: #888;
        }}
        .spacer-header {{ height: 3.2rem; }}
        /* ── Tabla pivot HTML nativa ─────────────────────── */
        .pose-table-wrap {{
            overflow-x: auto;
            overflow-y: auto;
            max-height: 520px;
            margin-bottom: 1rem;
            border-radius: 6px;
            border: 1px solid #333;
        }}
        table.pose-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.82rem;
            font-family: inherit;
        }}
        table.pose-table thead th {{
            position: sticky;
            top: 0;
            z-index: 2;
            text-align: center;
            background-color: #1e2030;
            color: #cdd6f4;
            padding: 7px 12px;
            border-bottom: 2px solid #414868;
            font-size: 0.78rem;
            font-weight: 600;
            white-space: nowrap;
        }}
        table.pose-table tbody td {{
            padding: 4px 12px 4px 8px;
            border-bottom: 1px solid #2a2a3a;
            text-align: right;
        }}
        table.pose-table tbody tr:last-child td {{
            font-weight: 700;
            border-top: 2px solid #414868;
            background-color: #1e2030;
        }}
        table.pose-table tbody tr:hover td {{
            background-color: rgba(74,158,255,0.06);
        }}
        /* ── Multiselect: tags azules ─────────────────────── */
        [data-baseweb="tag"] {{
            background-color: #1a3a5c !important;
            border: 1px solid #4a9eff !important;
        }}
        [data-baseweb="tag"] span {{
            color: #a8d4ff !important;
            font-size: 0.72rem !important;
        }}
        [data-baseweb="tag"] [role="presentation"] svg {{
            fill: #4a9eff !important;
        }}
        /* Fondo del área de texto del multiselect */
        [data-baseweb="select"] > div,
        [data-baseweb="base-input"] {{
            background-color: #0d1e30 !important;
            border-color: #2a4a6a !important;
        }}
        /* Texto de los ítems dentro del input */
        [data-baseweb="select"] input,
        [data-baseweb="select"] [data-testid="stMultiSelectChipContainer"] {{
            background-color: #0d1e30 !important;
            font-size: 0.72rem !important;
        }}
        </style>
        <div class="header-pose">
          <span class="titulo">
            &#128202; Dashboard Director Financiero &mdash; GRUPO POSE
          </span>
          <span class="version">{_VERSION_APP}</span>
        </div>
        <div class="spacer-header"></div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Sidebar — filtros globales
# ---------------------------------------------------------------------------
def _sidebar(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### ⚙️  FILTROS")
    st.sidebar.markdown("---")

    # Período Año-Mes
    periodos_disponibles: list[str] = sorted(
        df["periodo"].dropna().unique().tolist()
    )
    periodo_max = periodos_disponibles[-1] if periodos_disponibles else None
    anio_max_str = periodo_max[:4] if periodo_max else None
    default_periodos = (
        [p for p in periodos_disponibles if p.startswith(anio_max_str)]
        if anio_max_str
        else periodos_disponibles
    )
    st.sidebar.markdown("**📅 Período (Año-Mes)**")
    periodos_sel: list[str] = st.sidebar.multiselect(
        "periodo",
        options=periodos_disponibles,
        default=default_periodos,
        label_visibility="collapsed",
    )

    # Gerencia
    gerencias_disponibles: list[str] = sorted(
        df["gerencia"].dropna().unique().tolist()
    )
    st.sidebar.markdown("**🏢 Gerencia**")
    gerencias_sel: list[str] = st.sidebar.multiselect(
        "gerencia",
        options=gerencias_disponibles,
        default=gerencias_disponibles,
        label_visibility="collapsed",
    )

    # Fuente
    fuentes_disponibles: list[str] = sorted(
        df["fuente"].dropna().unique().tolist()
    )
    st.sidebar.markdown("**📂 Fuente de datos**")
    fuentes_sel: list[str] = st.sidebar.multiselect(
        "fuente",
        options=fuentes_disponibles,
        default=fuentes_disponibles,
        label_visibility="collapsed",
    )

    mask = (
        df["periodo"].isin(periodos_sel)
        & df["gerencia"].isin(gerencias_sel)
        & df["fuente"].isin(fuentes_sel)
    )
    n_fil = int(mask.sum())
    st.sidebar.markdown("---")
    st.sidebar.caption(f"{n_fil:,} / {len(df):,} registros".replace(",", "."))
    st.sidebar.markdown("---")
    if st.sidebar.button(
        "\U0001f534 Cerrar Interfaz",
        use_container_width=True,
    ):
        import streamlit.components.v1 as components
        import threading
        import time

        # Cierra la pestaña del navegador antes de matar el proceso.
        # window.open('about:blank','_self').close() es el método
        # más compatible entre navegadores para cerrar una pestaña
        # aunque no haya sido abierta por script.
        components.html(
            "<script>"
            "window.parent.open('about:blank','_self').close();"
            "</script>",
            height=0,
        )

        def _stop() -> None:
            time.sleep(0.8)
            os._exit(0)

        threading.Thread(target=_stop, daemon=True).start()

    return df[mask].copy()


# ---------------------------------------------------------------------------
# Tab 1 — Resumen ejecutivo
# ---------------------------------------------------------------------------
def _tab_resumen(df: pd.DataFrame) -> None:
    # KPIs
    total_ars = df["importe_ars"].sum()
    n_obras = df["obra"].nunique()
    n_gerencias = df["gerencia"].nunique()
    periodos = df["periodo"].nunique()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total ARS", f"${total_ars:,.0f}".replace(",", "."))
    k2.metric("Obras", str(n_obras))
    k3.metric("Gerencias", str(n_gerencias))
    k4.metric("Períodos", str(periodos))

    st.markdown("---")

    col_a, col_b = st.columns(2)

    # Importe por gerencia
    with col_a:
        st.subheader("Importe por Gerencia")
        top_g = (
            df.groupby("gerencia", observed=True)["importe_ars"]
            .sum()
            .reset_index()
            .sort_values("importe_ars")
        )
        fig_g = px.bar(
            top_g,
            x="importe_ars",
            y="gerencia",
            orientation="h",
            labels={"importe_ars": "", "gerencia": ""},
            color="importe_ars",
            color_continuous_scale="Blues",
        )
        fig_g.update_traces(
            hovertemplate=("<b>%{y}</b><br>" "$ %{x:,.0f}<extra></extra>"),
        )
        fig_g.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            margin={"t": 10, "b": 10, "l": 0, "r": 20},
            font={"size": 12},
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis={
                "tickformat": ",.0f",
                "title_text": "ARS",
                "gridcolor": "#2a2a2a",
            },
            yaxis={"title_text": ""},
        )
        st.plotly_chart(fig_g, use_container_width=True)

    # Evolución mensual
    with col_b:
        st.subheader("Evolución Mensual — ARS")
        evol = (
            df.groupby("periodo", observed=True)["importe_ars"]
            .sum()
            .reset_index()
            .sort_values("periodo")
        )
        fig_e = px.line(
            evol,
            x="periodo",
            y="importe_ars",
            markers=True,
            labels={"importe_ars": "", "periodo": ""},
        )
        fig_e.update_traces(
            line_color="#4a9eff",
            hovertemplate=("<b>%{x}</b><br>" "$ %{y:,.0f}<extra></extra>"),
        )
        fig_e.update_layout(
            margin={"t": 10, "b": 10, "l": 0, "r": 20},
            font={"size": 12},
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis={
                "title_text": "Período",
                "gridcolor": "#2a2a2a",
            },
            yaxis={
                "tickformat": ",.0f",
                "title_text": "ARS",
                "gridcolor": "#2a2a2a",
            },
        )
        st.plotly_chart(fig_e, use_container_width=True)

    st.caption(
        f"{_FUENTE_LABEL} — {len(df):,.0f} registros filtrados".replace(
            ",", "."
        )
    )


# ---------------------------------------------------------------------------
# Tab 2 — Explorador PyGWalker
# ---------------------------------------------------------------------------
def _tab_explorador(df: pd.DataFrame) -> None:
    st.info(
        "Arrastrá columnas para crear tus propios KPIs y gráficos. "
        "Al terminar podés exportar la vista como imagen."
    )
    try:
        from pygwalker.api.streamlit import (  # type: ignore[import-untyped]
            StreamlitRenderer,
        )

        if "_pyg_renderer" not in st.session_state:
            st.session_state["_pyg_renderer"] = StreamlitRenderer(
                df, appearance="light"
            )
        st.session_state["_pyg_renderer"].explorer()
    except ImportError:
        st.warning(
            "PyGWalker no está instalado. " "Ejecutá: `pip install pygwalker`"
        )
        st.dataframe(df, use_container_width=True)


# ---------------------------------------------------------------------------
# Helper compartido para tablas pivot
# ---------------------------------------------------------------------------
def _render_pivot(df: pd.DataFrame, col: str) -> None:
    """
    Pivot filas=Obra/Descripcion, columnas=col, valores=importe_ars.
    - observed=True: evita filas fantasma de Categorical.
    - Excluye obras con todos los importes en cero o NaN.
    - Ordena por Total general ascendente (mayor gasto arriba).
    - Fila de totales siempre al final.
    """
    pvt = pd.pivot_table(
        df,
        index=["obra", "descripcion_obra"],
        columns=col,
        values="importe_ars",
        aggfunc="sum",
        observed=True,
        margins=True,
        margins_name="Total general",
    )

    # Separar fila de totales del cuerpo
    total_row = pvt.loc[["Total general"]]
    body = pvt.drop(index="Total general")

    # Excluir filas todo-NaN o todo-cero
    body = body.dropna(how="all")
    body = body[(body.fillna(0).abs().sum(axis=1)) > 0]

    # Ordenar por Total general asc (mayor gasto = más negativo, va arriba)
    if "Total general" in body.columns:
        body = body.sort_values("Total general", ascending=True)

    pvt_clean = pd.concat([body, total_row])
    pvt_clean.index.names = ["OBRA PRONTO", "DESCRIPCION OBRA"]
    pvt_clean = pvt_clean.reset_index()
    pvt_clean.columns.name = None

    num_cols = [
        c
        for c in pvt_clean.columns
        if c not in ("OBRA PRONTO", "DESCRIPCION OBRA")
    ]

    def _fmt(v: object) -> str:
        if not isinstance(v, (int, float)) or pd.isna(v) or v == 0:
            return ""
        return (
            f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )

    def _color(v: object) -> str:
        if not isinstance(v, (int, float)) or pd.isna(v):
            return ""
        return "color: #ff4b4b; font-weight: bold;" if v < 0 else ""

    id_cols = ["OBRA PRONTO", "DESCRIPCION OBRA"]
    num_props = {
        "text-align": "right",
        "padding-right": "10px",
    }
    id_props = {
        "text-align": "left",
        "padding-left": "6px",
    }
    styled = (
        pvt_clean.style.format(_fmt, subset=num_cols, na_rep="")
        .map(_color, subset=num_cols)
        .set_properties(subset=id_cols, **id_props)
        .set_properties(subset=num_cols, **num_props)
        .hide(axis="index")
        .set_table_attributes('class="pose-table"')
    )
    html = styled.to_html(escape=False)
    st.markdown(
        f'<div class="pose-table-wrap">{html}</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"{_FUENTE_LABEL} — "
        f"{len(df):,.0f} registros filtrados".replace(",", ".")
    )


# ---------------------------------------------------------------------------
# Tab 3 — Pivot Obra / Fuente
# ---------------------------------------------------------------------------
def _tab_pivot_fuente(df: pd.DataFrame) -> None:
    """Pivot: filas=Obra, columnas=Fuente, valores=Importe ARS."""
    if df.empty:
        st.info("Sin datos para los filtros seleccionados.")
        return
    _render_pivot(df, "fuente")


# ---------------------------------------------------------------------------
# Tab 4 — Pivot Obra / Rubro Contable
# ---------------------------------------------------------------------------
def _tab_pivot_rubro(df: pd.DataFrame) -> None:
    """Pivot: filas=Obra, columnas=Rubro Contable, valores=Importe ARS."""
    if df.empty:
        st.info("Sin datos para los filtros seleccionados.")
        return
    _render_pivot(df, "rubro_contable")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    st.set_page_config(
        page_title="Dashboard Director Financiero",
        page_icon="📊",
        layout="wide",
    )

    _encabezado_fijo()

    df_raw = _cargar_datos()
    df = _sidebar(df_raw)

    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        return

    tab_resumen, tab_fuente, tab_rubro, tab_explorer = st.tabs(
        [
            "📊 Resumen",
            "📋 Obra / Fuente",
            "📋 Obra / Rubro",
            "🔍 Explorador",
        ]
    )

    with tab_resumen:
        _tab_resumen(df)

    with tab_fuente:
        _tab_pivot_fuente(df)

    with tab_rubro:
        _tab_pivot_rubro(df)

    with tab_explorer:
        _tab_explorador(df)


if __name__ == "__main__":
    main()
