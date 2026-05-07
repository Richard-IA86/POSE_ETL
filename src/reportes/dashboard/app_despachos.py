"""
app_despachos.py — Dashboard de auditoría del pipeline DESPACHOS.

Tabs:
  1. Resumen     — KPIs y gráficos desde PRODUCCION.despachos_validados
  2. Despachos   — Tabla filtrable (gerencia, período, obra)
  3. Pendientes  — Resolución de registros rechazados (APROBADO / BAJADO)
"""

import os
import sys
from pathlib import Path

# Garantiza que el root del workspace esté en sys.path
# independientemente de desde dónde se invoque streamlit.
_ROOT = Path(__file__).parents[4]  # .../Dev_Richard_IA86
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.reportes.loader.bd_loader_despachos import (  # noqa: E402,E501
    get_pendientes_df,
    get_validados_df,
    resolver_rechazado,
    bajar_pendientes_masivo,
)

st.set_page_config(
    page_title="Despachos — Auditoría",
    layout="wide",
    page_icon="📦",
)


# ── Helpers ────────────────────────────────────────────────────────────────────  # noqa: E501


@st.cache_data(ttl=300)
def _cargar_validados() -> pd.DataFrame:
    df = get_validados_df()
    if not df.empty:
        df["importe"] = pd.to_numeric(df["importe"], errors="coerce")
        df["importe_usd"] = pd.to_numeric(df["importe_usd"], errors="coerce")
    return df


@st.cache_data(ttl=60)
def _cargar_pendientes() -> pd.DataFrame:
    return get_pendientes_df()


def _fmt_ars(valor) -> str:
    try:
        return f"$ {float(valor):,.0f}"
    except Exception:
        return "-"


def _fmt_usd(valor) -> str:
    try:
        return f"USD {float(valor):,.0f}"
    except Exception:
        return "-"


# ── Encabezado ─────────────────────────────────────────────────────────────────  # noqa: E501

st.title("📦 Pipeline DESPACHOS — Auditoría")

if st.button("🔄 Refrescar datos"):
    st.cache_data.clear()
    st.rerun()

tab_resumen, tab_datos, tab_pendientes = st.tabs(
    ["📊 Resumen", "📋 Despachos", "⚠️ Pendientes"]
)

# ── Sidebar — filtros globales (Tab 1 y Tab 2) ────────────────────────────────  # noqa: E501
df_base = _cargar_validados()

with st.sidebar:
    st.header("🔎 Filtros")

    if df_base.empty:
        st.info("Sin datos cargados.")
        df_filtrado = df_base.copy()
    else:
        # Gerencia
        gerencias_disp = sorted(df_base["gerencia"].dropna().unique().tolist())
        ger_sel = st.multiselect("Gerencia", gerencias_disp)

        # Obra — cascada: si hay gerencias seleccionadas, solo muestra sus obras  # noqa: E501
        df_obras_src = (
            df_base
            if not ger_sel
            else df_base[df_base["gerencia"].isin(ger_sel)]
        )
        df_obras = (
            df_obras_src[["obra_pronto", "descripcion_obra"]]
            .drop_duplicates()
            .sort_values("obra_pronto")
        )
        df_obras["_label"] = (
            df_obras["obra_pronto"].fillna("?")
            + " — "
            + df_obras["descripcion_obra"].fillna("(sin descripción)").str[:50]
        )
        obra_opciones = df_obras["_label"].tolist()
        obras_sel = st.multiselect("Obra", obra_opciones)

        # Período
        periodos_disp = sorted(df_base["periodo"].dropna().unique().tolist())
        per_sel = st.multiselect("Período (YYYY-MM)", periodos_disp)

        st.divider()

        # Aplicar filtros
        df_filtrado = df_base.copy()
        if ger_sel:
            df_filtrado = df_filtrado[df_filtrado["gerencia"].isin(ger_sel)]
        if obras_sel:
            prontos_sel = df_obras.loc[
                df_obras["_label"].isin(obras_sel), "obra_pronto"
            ].tolist()
            df_filtrado = df_filtrado[
                df_filtrado["obra_pronto"].isin(prontos_sel)
            ]
        if per_sel:
            df_filtrado = df_filtrado[df_filtrado["periodo"].isin(per_sel)]

        # Resumen del filtro activo
        filtros_activos = bool(ger_sel or obras_sel or per_sel)
        if filtros_activos:
            st.caption(
                f"**{len(df_filtrado):,}** de {len(df_base):,} registros"
            )
        else:
            st.caption(f"Sin filtros — {len(df_base):,} registros")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RESUMEN
# ══════════════════════════════════════════════════════════════════════════════
with tab_resumen:
    df_val = df_filtrado

    if df_val.empty:
        st.warning("No hay datos en PRODUCCION.despachos_validados.")
    else:
        # ── KPIs ──────────────────────────────────────────────────────────────  # noqa: E501
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Registros", f"{len(df_val):,}")
        c2.metric("Importe ARS", _fmt_ars(df_val["importe"].abs().sum()))
        c3.metric("Importe USD", _fmt_usd(df_val["importe_usd"].abs().sum()))
        c4.metric("Obras únicas", df_val["obra_pronto"].nunique())
        c5.metric("Períodos", df_val["periodo"].nunique())

        col_desde, col_hasta = st.columns(2)
        col_desde.caption(f"Desde: **{df_val['fecha'].min()}**")
        col_hasta.caption(f"Hasta: **{df_val['fecha'].max()}**")

        st.divider()

        # ── Importe ARS por Gerencia (top 15 por monto absoluto) ──────────────  # noqa: E501
        st.subheader("Importe ARS por Gerencia — top 15")
        df_ger = (
            df_val.groupby("gerencia")["importe"]
            .sum()
            .abs()
            .sort_values(ascending=True)
            .tail(15)
            .reset_index()
        )
        df_ger.columns = ["Gerencia", "Importe ARS"]
        st.bar_chart(df_ger.set_index("Gerencia"))

        st.divider()

        # ── Importe ARS por Período ────────────────────────────────────────────  # noqa: E501
        st.subheader("Importe ARS por Período")
        df_per = (
            df_val.groupby("periodo")["importe"]
            .sum()
            .abs()
            .sort_index()
            .reset_index()
        )
        df_per.columns = ["Período", "Importe ARS"]
        st.line_chart(df_per.set_index("Período"))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DESPACHOS (tabla filtrable)
# ══════════════════════════════════════════════════════════════════════════════
with tab_datos:
    if df_filtrado.empty:
        st.warning("No hay datos en PRODUCCION.despachos_validados.")
    else:
        st.caption(
            f"{len(df_filtrado):,} registros  ·  "
            f"ARS: {_fmt_ars(df_filtrado['importe'].sum())}  ·  "
            f"USD: {_fmt_usd(df_filtrado['importe_usd'].sum())}"
        )

        cols_tabla = [
            "periodo",
            "fecha",
            "obra_pronto",
            "descripcion_obra",
            "gerencia",
            "importe",
            "importe_usd",
            "tipo_comprobante",
            "nro_comprobante",
            "observacion",
            "proveedor",
            "detalle",
        ]
        cols_pres = [c for c in cols_tabla if c in df_filtrado.columns]
        st.dataframe(
            df_filtrado[cols_pres], use_container_width=True, hide_index=True
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — PENDIENTES (resolución)
# ══════════════════════════════════════════════════════════════════════════════
with tab_pendientes:
    df_pend = _cargar_pendientes()

    if df_pend.empty:
        st.success("✅ Sin pendientes. BD al día.")
    else:
        st.info(f"**{len(df_pend)}** registros requieren resolución.")

        # ── Tabla resumen ─────────────────────────────────────────────────────  # noqa: E501
        cols_vis = [
            "id",
            "periodo",
            "obra_pronto",
            "descripcion_obra",
            "gerencia",
            "importe",
            "proveedor",
            "nro_comprobante",
            "observacion",
            "motivo_rechazo",
        ]
        cols_pres = [c for c in cols_vis if c in df_pend.columns]
        st.dataframe(
            df_pend[cols_pres], use_container_width=True, hide_index=True
        )

        st.divider()
        st.subheader("✏️ Resolver")

        # ── Selector de fila ──────────────────────────────────────────────────  # noqa: E501
        opciones = {
            row["id"]: (
                f"{row['id']} "
                f"— {str(row.get('descripcion_obra', '?'))[:40]} "
                f"| {row.get('periodo', '?')}"
            )
            for _, row in df_pend.iterrows()
        }
        id_sel = st.selectbox(
            "Seleccionar fila",
            options=list(opciones.keys()),
            format_func=lambda x: opciones[x],
        )

        # ── Detalle de la fila seleccionada ───────────────────────────────────  # noqa: E501
        if id_sel is not None:
            fila = df_pend[df_pend["id"] == id_sel].iloc[0]
            with st.expander("📄 Detalle del registro", expanded=True):
                dc1, dc2, dc3 = st.columns(3)
                dc1.markdown(
                    f"**Obra:** `{fila.get('obra_pronto', '?')}`  \n"
                    f"**Gerencia:** {fila.get('gerencia', '?')}"
                )
                dc2.markdown(
                    f"**Importe ARS:** {_fmt_ars(fila.get('importe', 0))}  \n"
                    f"**Período:** {fila.get('periodo', '?')}"
                )
                dc3.markdown(
                    f"**Proveedor:** {fila.get('proveedor', '?')}  \n"
                    f"**Comprobante:** {fila.get('nro_comprobante', '?')}"
                )
                st.markdown(
                    f"**Motivo rechazo:** {fila.get('motivo_rechazo', '?')}"
                )
                if fila.get("detalle"):
                    st.markdown(f"**Detalle:** {fila.get('detalle', '')}")

        # ── Formulario de resolución ───────────────────────────────────────────  # noqa: E501
        with st.form("form_resolver"):
            fc1, fc2 = st.columns(2)
            accion = fc1.selectbox(
                "Acción",
                options=["APROBADO", "BAJADO", "MODIFICAR"],
                help="APROBADO → migra a despachos_validados  |  BAJADO → descartado definitivo",  # noqa: E501
            )
            usuario = fc2.text_input(
                "Usuario", value=os.getenv("USERNAME", "operador")
            )
            comentario = st.text_input("Comentario (opcional)")
            submitted = st.form_submit_button("Confirmar", type="primary")

        if submitted and id_sel is not None:
            resultado = resolver_rechazado(
                id_fila=int(id_sel),
                accion=accion,
                comentario=comentario or None,
                usuario=usuario,
            )
            if resultado["ok"]:
                msg = f"Fila {id_sel} → **{resultado['accion_aplicada']}**"
                if accion == "APROBADO":
                    msg += " — migrada a despachos_validados ✅"
                st.success(msg)
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"Error: {resultado['error']}")

        st.divider()

        # ── Baja masiva ──────────────────────────────────────────────────
        with st.expander("⚠️ Baja masiva — duplicados confirmados"):
            st.warning(
                "Marca como **BAJADO** todos los registros PENDIENTE."
                " Usar solo cuando todos son duplicados reales"
                " confirmados."
            )
            bm_c1, bm_c2 = st.columns(2)
            usu_masivo = bm_c1.text_input(
                "Usuario",
                value=os.getenv("USERNAME", "operador"),
                key="usu_masivo",
            )
            coment_masivo = bm_c2.text_input(
                "Comentario",
                value="Duplicados reales confirmados",
                key="coment_masivo",
            )
            if st.button(
                f"Bajar todos ({len(df_pend)})",
                type="secondary",
            ):
                res = bajar_pendientes_masivo(
                    comentario=coment_masivo,
                    usuario=usu_masivo,
                )
                if res["ok"]:
                    st.success(
                        f"{res['registros_actualizados']}"
                        " registros → BAJADO ✅"
                    )
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Error: {res['error']}")
