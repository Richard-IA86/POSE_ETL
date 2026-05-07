"""
transformer_mensuales.py — Transforma DataFrames crudos MENSUALES al
schema staging compartido (COLS_STAGING).

Función pública:
  transformar_mensuales(archivos, loockups_path) → pd.DataFrame

Referencia: PowerQuery mensuales.pq (fuente autoritativa).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ._constantes import (
    COLS_STAGING,
    MESES_ES,
)
from ._constantes_mensuales import CUENTA_MENSUALES
from .reader_fdl import parse_nombre_archivo
from .reader_mensuales import leer_tabla_mensuales
from .transformer_fdl import _leer_obras_gerencias


def transformar_mensuales(
    archivos: list[Path],
    loockups_path: Path,
) -> pd.DataFrame:
    """
    Procesa una lista de archivos MM-YYYY.xlsx y devuelve el staging
    MENSUALES consolidado.

    Parameters
    ----------
    archivos : list[Path]
        Archivos 'MM-YYYY.xlsx' en orden (ej: [01-2026.xlsx, ...]).
    loockups_path : Path
        Ruta al Loockups.xlsx con la hoja 'Obras_Gerencias'.

    Returns
    -------
    pd.DataFrame
        Staging con columnas COLS_STAGING.
    """
    df_obras = _leer_obras_gerencias(loockups_path)
    partes: list[pd.DataFrame] = []

    for archivo in archivos:
        df_raw = leer_tabla_mensuales(archivo)
        if not df_raw.empty:
            partes.append(_enriquecer_mensuales(df_raw, df_obras, archivo))

    if not partes:
        return pd.DataFrame(columns=COLS_STAGING)

    df = pd.concat(partes, ignore_index=True)
    return df[COLS_STAGING]


# ── Enriquecimiento ──────────────────────────────────────────────────────────


def _enriquecer_mensuales(
    df: pd.DataFrame,
    df_obras: pd.DataFrame,
    archivo: Path,
) -> pd.DataFrame:
    """
    Aplica lookup Obras_Gerencias e inyecta constantes MENSUALES.

    IMPORTE* = TOTAL_COSTO * -1  (costo → negativo, alinea con PQ)
    FECHA*   = último día del mes derivado del nombre de archivo
    DETALLE* = "dd/MM/yyyy - Mensuales"
    FUENTE*  = "MENSUALES"

    Devuelve DataFrame con exactamente las columnas de COLS_STAGING.
    """
    mes, anio = parse_nombre_archivo(archivo)

    # Último día del mes (alinea con Date.EndOfMonth del PQ)
    import calendar

    ultimo_dia = calendar.monthrange(anio, mes)[1]
    fecha_fin = pd.Timestamp(anio, mes, ultimo_dia)
    fecha_staging = fecha_fin.strftime("%Y-%m-%d")
    fecha_fmt = fecha_fin.strftime("%d/%m/%Y")
    mes_nombre = MESES_ES.get(mes, "")

    obras_idx = df_obras.set_index("OBRA_PRONTO")
    rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        obra = str(row.get("OBRA_PRONTO", "")).strip()
        total_costo = float(row.get("TOTAL_COSTO") or 0.0)
        desc_local: Any = row.get("DESCRIPCION_OBRA")

        # Lookup Obras_Gerencias
        if obra in obras_idx.index:
            lk = obras_idx.loc[obra]
            desc_obra: Any = lk["DESCRIPCION_OBRA"]
            gerencia: Any = lk["GERENCIA"]
            compensable: Any = lk["COMPENSABLE"]
        else:
            # Para copias locales: usar descripción parseada del
            # "Etiquetas de fila" si el lookup no resuelve
            desc_obra = desc_local if desc_local else None
            gerencia = None
            compensable = None

        rows.append(
            {
                "MES": mes_nombre,
                "FECHA*": fecha_staging,
                "OBRA PRONTO*": obra,
                "DESCRIPCION OBRA": desc_obra,
                "GERENCIA": gerencia,
                "DETALLE*": f"{fecha_fmt} - Mensuales",
                "IMPORTE*": total_costo * -1,
                "TIPO COMPROBANTE*": "-",
                "N° COMPROBANTE*": "-",
                "OBSERVACION*": "-",
                "PROVEEDOR*": "POSE",
                "RUBRO CONTABLE*": CUENTA_MENSUALES["RUBRO_CONTABLE"],
                "CUENTA CONTABLE*": CUENTA_MENSUALES["CUENTA_CONTABLE"],
                "CODIGO CUENTA*": CUENTA_MENSUALES["CODIGO_CUENTA"],
                "FUENTE*": "MENSUALES",
                "COMPENSABLE": compensable,
                "TC": None,
                "IMPORTE USD": None,
            }
        )

    return pd.DataFrame(rows, columns=COLS_STAGING)
