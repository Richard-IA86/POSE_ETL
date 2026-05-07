"""
reader_quincenas.py — Módulo de lectura para informes QUINCENAS.

Lee las hojas '1ER QUINCENA ...' y '2DA QUINCENA ...', ignorando
la cabecera con skiprows=2.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def leer_hojas_quincenas(archivo: Path) -> tuple[pd.DataFrame, str]:
    """
    Lee un archivo de QUINCENAS y extrae y concatena las
    hojas de la primera y segunda quincenas.

    Ignora la hoja 'TICKET - AUMENTOS' y 'BANCOS HABILITADOS'.

    Parameters
    ----------
    archivo : Path
        Ruta al archivo Excel de quincenas.

    Returns
    -------
    tuple[pd.DataFrame, str]
        DataFrame crudo concatenado y string con hojas encontradas
        (para logging en el caller).
    """
    xls = pd.ExcelFile(archivo)
    hojas_quincenas = [h for h in xls.sheet_names if "QUINCENA" in h.upper()]

    if not hojas_quincenas:
        logger.warning("No se encontraron hojas de quincenas en %s", archivo)
        return pd.DataFrame(), ""

    dfs: list[pd.DataFrame] = []
    for hoja in hojas_quincenas:
        logger.debug("Leyendo hoja '%s' de %s", hoja, archivo.name)
        df_hoja = pd.read_excel(xls, sheet_name=hoja, skiprows=2)

        # Quitar columnas que sean todo NaNs
        df_hoja = df_hoja.dropna(axis=1, how="all")
        # Quitar filas que sean todo NaNs
        df_hoja = df_hoja.dropna(how="all")

        if not df_hoja.empty:
            dfs.append(df_hoja)

    if not dfs:
        return pd.DataFrame(), ""

    df_final = pd.concat(dfs, ignore_index=True)

    # Limpiar espacios en nombres de columnas para accesibilidad
    df_final.columns = [str(c).strip().upper() for c in df_final.columns]

    # Forzar QUINCENA a int (puede llegar como float o str en Excel)
    if "QUINCENA" in df_final.columns:
        df_final["QUINCENA"] = (
            pd.to_numeric(df_final["QUINCENA"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    hojas_info = ", ".join(hojas_quincenas)
    return df_final, hojas_info
