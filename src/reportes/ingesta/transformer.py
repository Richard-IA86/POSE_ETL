"""
transformer.py — Transforma y enriquece los datos DESPACHOS para report_direccion.  # noqa: E501

Recibe InspectorOutput (con excel_path), carga los lookups desde input_raw/ y
retorna (df_staging, df_pendientes) con columnas COLS_STAGING.

  df_staging    → filas validadas (sin duplicados), listas para BD
  df_pendientes → filas duplicadas para revisión en Streamlit

El transformer es la única etapa que acede al Excel crudo; lo hace usando
NOMBRE_HOJA y FILA_HEADERS definidos en _mapeo.py.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.contracts import InspectorOutput
from src.reportes.ingesta._mapeo import (
    NOMBRE_HOJA,
    FILA_HEADERS,
    COLS_STAGING,
    CONSTANTES_STAGING,
    DUP_COLS,
    MESES_ES,
    LOOCKUPS_FILE,
    HOJA_OBRAS_GERENCIAS,
    HOJA_GERENCIA_EQUIV,
    HOJA_TIPO_CAMBIO,
    HOJA_EXCEPCIONES_GERENCIA,
)

# ── Rutas de lookups ─────────────────────────────────────────────────────────
# parents[0]=ingesta/ parents[1]=src/ parents[2]=report_direccion/
# parents[3]=projects/ parents[4]=richard_ia86_dev/
_ROOT = Path(__file__).parents[4]
_LOOCKUPS_PATH = (
    _ROOT / "report_gerencias" / "data" / "lookups" / LOOCKUPS_FILE
)

# ── Punto de entrada público ──────────────────────────────────────────────────  # noqa: E501


def transformar(
    inspector_output: InspectorOutput,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica todas las transformaciones del pipeline DESPACHOS.

    Parameters
    ----------
    inspector_output : InspectorOutput
        Salida del Inspector con excel_path validado.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (df_staging, df_pendientes) — ambos con columnas COLS_STAGING.
    """
    # 1. Cargar lookups desde data/lookups/
    df_obras = _leer_obras_gerencias(_LOOCKUPS_PATH)
    excepciones = _leer_excepciones_gerencia(_LOOCKUPS_PATH)
    df_tc = _leer_tipo_cambio(_LOOCKUPS_PATH)
    gerencia_equiv = _leer_gerencia_equivalente(_LOOCKUPS_PATH)

    # 2. Leer crudo (se guarda snapshot para detección de duplicados)
    df_crudo = pd.read_excel(
        inspector_output.excel_path,
        sheet_name=NOMBRE_HOJA,
        header=FILA_HEADERS,
    )
    df_crudo.columns = [str(c).strip() for c in df_crudo.columns]
    df_crudo = df_crudo.dropna(how="all").reset_index(drop=True)

    if "$_DESPACH" in df_crudo.columns:
        df_crudo["$_DESPACH"] = pd.to_numeric(
            df_crudo["$_DESPACH"], errors="coerce"
        ).fillna(0)

    df = df_crudo.copy()

    # 3. Columnas base derivadas
    df["FECHA*"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df["OBRA PRONTO*"] = (
        df["N° OBRA"]
        .astype(str)
        .str.strip()
        .str.lstrip("'")
        .apply(lambda v: v.zfill(8) if v.isdigit() else v)
    )
    df["MES"] = df["FECHA*"].dt.month.map(MESES_ES)
    df["IMPORTE*"] = (
        pd.to_numeric(df["$_DESPACH"], errors="coerce").fillna(0) * -1
    )
    df["N° COMPROBANTE*"] = df["NRO_RTO"].fillna("").astype(str)
    df["OBSERVACION*"] = df["FACTURA"].apply(
        lambda v: f"FACTURA {v}" if pd.notna(v) else ""
    )
    df["DETALLE*"] = _construir_detalle(df)

    # 4. Constantes de fuente (REMITO, Materiales, CAC, etc.)
    for col, val in CONSTANTES_STAGING.items():
        df[col] = val
    df["PROVEEDOR*"] = df["FABRICA/CORRALON"].fillna("").astype(str)

    # 5. Join Obras_Gerencias
    df_obras_idx = df_obras.drop_duplicates(subset=["OBRA_PRONTO"]).set_index(
        "OBRA_PRONTO"
    )
    descs, gerencias, compensables = [], [], []
    for obra in df["OBRA PRONTO*"]:
        if obra in df_obras_idx.index:
            row = df_obras_idx.loc[obra]
            descs.append(row["DESCRIPCION_OBRA"])
            gerencias.append(row["GERENCIA"])
            compensables.append(
                row["COMPENSABLE"] if pd.notna(row["COMPENSABLE"]) else ""
            )
        else:
            descs.append("")
            gerencias.append("SIN OBRA ASIGNADA")
            compensables.append("")
    df["DESCRIPCION OBRA"] = descs
    df["GERENCIA"] = gerencias
    df["COMPENSABLE"] = compensables

    # 6. Excepciones de gerencia (sobreescribir por rango de fechas)
    df["GERENCIA"] = _aplicar_excepciones_gerencia(df, excepciones)

    # 7. Normalización GERENCIA → equivalentes canónicos
    if gerencia_equiv:
        df["GERENCIA"] = (
            df["GERENCIA"].map(gerencia_equiv).fillna(df["GERENCIA"])
        )

    # 8. Tipo de cambio (merge_asof — día hábil anterior)
    fechas_idx = (
        df[["FECHA*"]].rename(columns={"FECHA*": "Fecha"}).reset_index()
    )
    fechas_idx["Fecha"] = pd.to_datetime(fechas_idx["Fecha"], errors="coerce")
    tc_merge = (
        pd.merge_asof(
            fechas_idx.sort_values("Fecha"),
            df_tc.rename(columns={"TC comprador": "TC"}),
            on="Fecha",
            direction="backward",
        )
        .set_index("index")
        .sort_index()
    )
    df["TC"] = tc_merge["TC"].values

    # 9. Importe USD
    df["IMPORTE USD"] = (df["IMPORTE*"] / df["TC"]).round(6)

    # 10. Formatear FECHA* como string para el CSV (evita "2023-08-01 00:00:00")  # noqa: E501
    df["FECHA*"] = df["FECHA*"].dt.strftime("%Y-%m-%d")

    # 11. Segregar duplicados (se evalúa sobre el crudo antes de transformaciones)  # noqa: E501
    dup_mask = _detectar_duplicados(df_crudo)
    df_all = df[COLS_STAGING].copy()
    df_staging = df_all[~dup_mask].reset_index(drop=True)
    df_pendientes = df_all[dup_mask].reset_index(drop=True)

    return df_staging, df_pendientes


# ── Helpers de lectura de lookups ─────────────────────────────────────────────  # noqa: E501


def _leer_obras_gerencias(loockups_path: Path) -> pd.DataFrame:
    ruta = loockups_path
    if not ruta.exists():
        raise FileNotFoundError(
            f"Lookup obligatorio no encontrado: {ruta}\n"
            "Copiá Loockups.xlsx a report_gerencias/data/lookups/"
        )
    df = pd.read_excel(ruta, sheet_name=HOJA_OBRAS_GERENCIAS, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    df["OBRA_PRONTO"] = df["OBRA_PRONTO"].astype(str).str.strip()
    return df[["OBRA_PRONTO", "DESCRIPCION_OBRA", "GERENCIA", "COMPENSABLE"]]


def _leer_excepciones_gerencia(loockups_path: Path) -> pd.DataFrame:
    ruta = loockups_path
    if not ruta.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(
            ruta, sheet_name=HOJA_EXCEPCIONES_GERENCIA, header=0
        )
        df.columns = [str(c).strip() for c in df.columns]
        df = df[df["ACTIVO"].astype(str).str.upper() == "SI"].copy()
        df["OBRA_PRONTO"] = df["OBRA_PRONTO"].astype(str).str.zfill(8)
        df["FECHA_DESDE"] = pd.to_datetime(df["FECHA_DESDE"], errors="coerce")
        df["FECHA_HASTA"] = pd.to_datetime(df["FECHA_HASTA"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def _leer_tipo_cambio(loockups_path: Path) -> pd.DataFrame:
    ruta = loockups_path
    if not ruta.exists():
        raise FileNotFoundError(
            f"Lookup obligatorio no encontrado: {ruta}\n"
            "Copiá Loockups.xlsx a report_gerencias/data/lookups/"
        )
    df = pd.read_excel(ruta, sheet_name=HOJA_TIPO_CAMBIO, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = (
        df.dropna(subset=["Fecha"]).sort_values("Fecha").reset_index(drop=True)
    )
    return df[["Fecha", "TC comprador"]]


def _leer_gerencia_equivalente(loockups_path: Path) -> dict[str, str]:
    ruta = loockups_path
    if not ruta.exists():
        return {}
    try:
        df = pd.read_excel(ruta, sheet_name=HOJA_GERENCIA_EQUIV, header=0)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(subset=["GERENCIA", "GERENCIA_EQUIV"])
        df = df.drop_duplicates(subset=["GERENCIA"])
        df["GERENCIA"] = df["GERENCIA"].astype(str).str.strip()
        df["GERENCIA_EQUIV"] = df["GERENCIA_EQUIV"].astype(str).str.strip()
        return dict(zip(df["GERENCIA"], df["GERENCIA_EQUIV"]))
    except Exception:
        return {}


# ── Helpers de transformación ────────────────────────────────────────────────


def _construir_detalle(df: pd.DataFrame) -> pd.Series:
    """Construye DETALLE* desde múltiples columnas del crudo."""
    fecha_str = pd.to_datetime(df["FECHA*"], errors="coerce").dt.strftime(
        "%d/%m/%Y"
    )
    nv = df["NV"].fillna("").astype(str)
    cod = df["COD"].fillna("").astype(str)
    desc = df["DESC_PROD"].fillna("").astype(str)
    cant = df["CANT_DESP"].fillna("").astype(str)
    return (
        fecha_str
        + " - NV "
        + nv
        + " - PROD "
        + cod
        + " "
        + desc
        + " - CAN "
        + cant
    )


def _aplicar_excepciones_gerencia(
    df: pd.DataFrame, excepciones: pd.DataFrame
) -> pd.Series:
    """Sobreescribe GERENCIA para obras con excepción activa en el rango de fechas."""  # noqa: E501
    gerencia = df["GERENCIA"].copy()
    if excepciones.empty:
        return gerencia
    fecha_col = pd.to_datetime(df["FECHA*"], errors="coerce")
    obra_col = df["OBRA PRONTO*"].astype(str)
    for _, exc in excepciones.iterrows():
        mask = (
            (obra_col == exc["OBRA_PRONTO"])
            & (fecha_col >= exc["FECHA_DESDE"])
            & (fecha_col <= exc["FECHA_HASTA"])
        )
        gerencia[mask] = exc["GERENCIA_OVERRIDE"]
    return gerencia


def _detectar_duplicados(df_crudo: pd.DataFrame) -> pd.Series:
    """Retorna máscara: True = fila duplicada según DUP_COLS (solo entre filas con FACTURA)."""  # noqa: E501
    dup_cols_real = [c for c in DUP_COLS if c in df_crudo.columns]
    mask = pd.Series(False, index=df_crudo.index)
    if len(dup_cols_real) < len(DUP_COLS):
        return mask
    df_con_fact = df_crudo[df_crudo["FACTURA"].notna()]
    dup_idx = df_con_fact.index[
        df_con_fact.duplicated(subset=dup_cols_real, keep=False)
    ]
    mask.loc[dup_idx] = True
    return mask
