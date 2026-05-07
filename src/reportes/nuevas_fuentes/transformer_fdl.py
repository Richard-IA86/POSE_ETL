"""
transformer_fdl.py — Transforma DataFrames crudos FDL al schema staging.

Función pública:
  transformar_fdl(archivos, loockups_path) → pd.DataFrame

Procesa FACTURACION FDL y GG FDL de cada archivo MM-YYYY.xlsx,
aplica el lookup Obras_Gerencias, inyecta constantes contables
y devuelve un DataFrame consolidado con COLS_STAGING.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ._constantes import (
    COLS_STAGING,
    CUENTAS_POR_FUENTE,
    FACTURACION_FDL_CUENTAS,
    HOJA_CENTRO_COSTO_OBRA,
    HOJA_OBRAS_GERENCIAS,
    MESES_ES,
    OBRAS_PROVISIONALES,
    TIPOS_EROGACION_GG,
)
from .reader_fdl import leer_tabla_gg_fdl

# ── Entrada pública ──────────────────────────────────────────────────────────


def transformar_fdl(
    archivos: list[Path],
    loockups_path: Path,
) -> pd.DataFrame:
    """
    Procesa una lista de archivos MM-YYYY.xlsx y devuelve el staging
    consolidado listo para escribir a CSV.

    Parameters
    ----------
    archivos : list[Path]
        Archivos 'MM-YYYY.xlsx' en orden (ej: [01-2026.xlsx, ...]).
    loockups_path : Path
        Ruta al Loockups.xlsx con las hojas 'Obras_Gerencias'
        y 'GG_FDL_CentroCosto'.

    Returns
    -------
    pd.DataFrame
        Staging con columnas COLS_STAGING, todas las fuentes y meses.
    """
    df_obras = _leer_obras_gerencias(loockups_path)
    cc_obra = _leer_centro_costo_obra(loockups_path)
    partes: list[pd.DataFrame] = []

    for archivo in archivos:
        df_raw = _normalizar_obra(leer_tabla_gg_fdl(archivo))
        partes.append(_enriquecer(df_raw, df_obras, cc_obra))

    if not partes:
        return pd.DataFrame(columns=COLS_STAGING)

    df = pd.concat(partes, ignore_index=True)
    return df[COLS_STAGING]


# ── Helpers de lectura ───────────────────────────────────────────────────────


def _leer_obras_gerencias(loockups_path: Path) -> pd.DataFrame:
    """Carga la hoja Obras_Gerencias del Loockups y devuelve el lookup."""
    if not loockups_path.exists():
        raise FileNotFoundError(f"Loockups no encontrado: {loockups_path}")
    df = pd.read_excel(loockups_path, sheet_name=HOJA_OBRAS_GERENCIAS)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    df["OBRA_PRONTO"] = df["OBRA_PRONTO"].astype(str).str.strip()
    # Deduplicar por clave antes de indexar
    df = df.drop_duplicates(subset="OBRA_PRONTO")
    return df[["OBRA_PRONTO", "DESCRIPCION_OBRA", "GERENCIA", "COMPENSABLE"]]


def _leer_centro_costo_obra(
    loockups_path: Path,
) -> dict[str, str]:
    """
    Lee la hoja GG_FDL_CentroCosto del Loockups.

    Devuelve {CENTRO_COSTO (upper) : OBRA_PRONTO (str)}.
    Retorna dict vacío si la hoja aún no existe
    (graceful fallback — no rompe el pipeline).

    Columnas requeridas en la hoja:
      CENTRO_COSTO  |  OBRA_PRONTO
    """
    if not loockups_path.exists():
        return {}
    try:
        df = pd.read_excel(loockups_path, sheet_name=HOJA_CENTRO_COSTO_OBRA)
    except Exception:
        return {}
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    if "CENTRO_COSTO" not in df.columns or "OBRA_PRONTO" not in df.columns:
        return {}
    df["CENTRO_COSTO"] = df["CENTRO_COSTO"].astype(str).str.strip().str.upper()
    df["OBRA_PRONTO"] = df["OBRA_PRONTO"].astype(str).str.strip()
    df = df[(df["CENTRO_COSTO"] != "") & (df["OBRA_PRONTO"] != "")]
    return dict(zip(df["CENTRO_COSTO"], df["OBRA_PRONTO"]))


def _normalizar_obra(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el campo OBRA_PRONTO según Archiv_Consolidado_Final.pq:
    - Elimina comilla inicial (formato '00000004)
    - Numérico puro → zero-pad a 8 dígitos (ej: "143" → "00000143")
    - Alfanumérico → UPPER + strip (ej: "sin obra" → "SIN OBRA")
    - null / vacío → se preserva como cadena vacía (descartado en reader)
    """

    def _normalizar(v: str) -> str:
        v = v.lstrip("'").strip()
        if v.isdigit():
            return v.zfill(8)
        return v.upper()

    df = df.copy()
    df["OBRA_PRONTO"] = df["OBRA_PRONTO"].apply(_normalizar)
    return df


# ── Enriquecimiento ──────────────────────────────────────────────────────────


def _enriquecer(
    df: pd.DataFrame,
    df_obras: pd.DataFrame,
    cc_obra: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Aplica lookup Obras_Gerencias e inyecta constantes contables.

    Determina FUENTE*, IMPORTE* y DETALLE* según TIPO_EROGACION:
      GG FDL (OBRA)        → IMPORTE = SALIDA3 * -1
      FACTURACION FDL (VENTA*) → IMPORTE = ENT2 - SALIDA3

    Para filas OBRA con OBRA_PRONTO vacío y CENTRO_COSTO relleno:
      se resuelve primero el código de obra via cc_obra antes del
      lookup Obras_Gerencias.

    PROVEEDOR*, TIPO COMPROBANTE*, N° COMPROBANTE* y OBSERVACION*
    son fijos ("POSE" / "-") conforme al PowerQuery ggfdf.pq.
    TC e IMPORTE USD quedan en None (no disponibles en la fuente).

    Devuelve DataFrame con exactamente las columnas de COLS_STAGING.
    """
    obras_idx = df_obras.set_index("OBRA_PRONTO")
    rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        tipo_er = str(row.get("TIPO_EROGACION", "")).strip()
        obra = str(row.get("OBRA_PRONTO", "")).strip()

        # GG FDL sin N° OBRA → resolver por CENTRO_COSTO
        if tipo_er in TIPOS_EROGACION_GG and obra == "" and cc_obra:
            cc = str(row.get("CENTRO_COSTO", "")).strip().upper()
            obra = cc_obra.get(cc, "")
        fecha = row.get("FECHA")
        mes_num: int | None = (
            fecha.month if isinstance(fecha, pd.Timestamp) else None
        )
        fecha_fmt: str | None = (
            fecha.strftime("%d/%m/%Y")
            if isinstance(fecha, pd.Timestamp)
            else None
        )
        fecha_staging: str | None = (
            fecha.strftime("%Y-%m-%d")
            if isinstance(fecha, pd.Timestamp)
            else None
        )

        # Fuente y constantes contables
        cuentas: dict[str, Any]
        if tipo_er in TIPOS_EROGACION_GG:
            fuente = "GG FDL"
            cuentas = CUENTAS_POR_FUENTE["GG FDL"]
        else:
            fuente = "FACTURACION FDL"
            cuentas = FACTURACION_FDL_CUENTAS.get(tipo_er, {})

        # IMPORTE por tipo de erogación
        salida3 = float(row.get("SALIDA3") or 0.0)
        ent2_raw = row.get("ENT2")
        ent2 = (
            float(ent2_raw)
            if ent2_raw is not None and not pd.isna(ent2_raw)
            else 0.0
        )
        if tipo_er in TIPOS_EROGACION_GG:
            importe = salida3 * -1
        else:
            importe = ent2 - salida3

        # DETALLE dinámico según tipo
        detalle: str | None
        if fecha_fmt is None:
            detalle = None
        elif tipo_er == "OBRA":
            detalle = f"{fecha_fmt} - Gastos Generales"
        elif tipo_er == "VENTA DEPTO":
            detalle = f"{fecha_fmt} - Cobranza Deptos."
        elif tipo_er == "VENTA LOTE":
            detalle = f"{fecha_fmt} - Cobranza Lotes"
        elif tipo_er == "VENTA PRODUCTO":
            detalle = f"{fecha_fmt} - Cobranza Productos"
        else:
            detalle = fecha_fmt

        # Lookup Obras_Gerencias
        if obra in obras_idx.index:
            lk = obras_idx.loc[obra]
            desc_obra: Any = lk["DESCRIPCION_OBRA"]
            gerencia: Any = lk["GERENCIA"]
            compensable: Any = lk["COMPENSABLE"]
        elif obra in OBRAS_PROVISIONALES:
            prov = OBRAS_PROVISIONALES[obra]
            desc_obra = prov["DESCRIPCION_OBRA"]
            gerencia = prov["GERENCIA"]
            compensable = prov["COMPENSABLE"] or None
        else:
            desc_obra = obra  # obra sin alta en ningún lookup
            gerencia = "SIN OBRA ASIGNADA"
            compensable = None

        rows.append(
            {
                "MES": MESES_ES.get(mes_num, "") if mes_num else "",
                "FECHA*": fecha_staging,
                "OBRA PRONTO*": obra,
                "DESCRIPCION OBRA": desc_obra,
                "GERENCIA": gerencia,
                "DETALLE*": detalle,
                "IMPORTE*": importe,
                "TIPO COMPROBANTE*": "-",
                "N° COMPROBANTE*": "-",
                "OBSERVACION*": "-",
                "PROVEEDOR*": "POSE",
                "RUBRO CONTABLE*": cuentas.get("RUBRO_CONTABLE"),
                "CUENTA CONTABLE*": cuentas.get("CUENTA_CONTABLE"),
                "CODIGO CUENTA*": cuentas.get("CODIGO_CUENTA"),
                "FUENTE*": fuente,
                "COMPENSABLE": compensable,
                "TC": None,
                "IMPORTE USD": None,
            }
        )

    return pd.DataFrame(rows, columns=COLS_STAGING)
