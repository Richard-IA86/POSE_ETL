"""
writer_quincenas.py — Persiste el staging QUINCENAS consolidado como CSV.

  staging_quincenas.csv → filas listas para cargar a BD

El archivo se escribe en report_gerencias/ con separador ';'
y encoding utf-8-sig, igual que FDL y MENSUALES.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

_STAGING_DIR = Path(__file__).parents[2] / "report_gerencias"
_STAGING_CSV = _STAGING_DIR / "staging_quincenas.csv"


def escribir_staging_quincenas(df_staging: pd.DataFrame) -> Path:
    """
    Guarda el staging QUINCENAS como CSV separado por ';' (utf-8-sig).

    Parameters
    ----------
    df_staging : pd.DataFrame
        DataFrame con columnas ya filtradas/transformadas.

    Returns
    -------
    Path
        Ruta al archivo generado (staging_quincenas.csv).

    Raises
    ------
    ValueError
        Si el DataFrame está vacío.
    """
    if df_staging.empty:
        raise ValueError(
            "El DataFrame de staging QUINCENAS está vacío "
            "— nada que guardar."
        )
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df_staging.to_csv(_STAGING_CSV, sep=";", index=False, encoding="utf-8-sig")
    return _STAGING_CSV
