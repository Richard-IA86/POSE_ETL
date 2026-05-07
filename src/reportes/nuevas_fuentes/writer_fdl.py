"""
writer_fdl.py — Persiste el staging FDL consolidado como CSV.

  staging_fdl.csv → filas listas para cargar a BD (FACTURACION + GG FDL)

El archivo se escribe en report_gerencias/ al mismo nivel que
staging_despachos.csv, con separador ';' y encoding utf-8-sig.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# Directorio de salida: writer_fdl.py → parents[2] = report_direccion/
_STAGING_DIR = Path(__file__).parents[2] / "report_gerencias"
_STAGING_CSV = _STAGING_DIR / "staging_fdl.csv"


def escribir_staging_fdl(df_staging: pd.DataFrame) -> Path:
    """
    Guarda el staging FDL como CSV separado por ';' (utf-8-sig).

    Parameters
    ----------
    df_staging : pd.DataFrame
        DataFrame con columnas COLS_STAGING a persistir.

    Returns
    -------
    Path
        Ruta al archivo generado (staging_fdl.csv).

    Raises
    ------
    ValueError
        Si el DataFrame está vacío.
    """
    if df_staging.empty:
        raise ValueError(
            "El DataFrame de staging FDL está vacío — nada que guardar."
        )
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df_staging.to_csv(_STAGING_CSV, sep=";", index=False, encoding="utf-8-sig")
    return _STAGING_CSV
