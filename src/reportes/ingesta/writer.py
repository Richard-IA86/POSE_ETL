"""
writer.py — Persiste los DataFrames de staging y pendientes como CSV.

  staging_despachos.csv  → filas validadas listas para cargar a BD
  pendientes_carga.csv   → filas duplicadas para revisión en Streamlit

El Validador y el Cargador consumen staging_despachos.csv vía
ConstructorOutput.staging_path.
"""

from pathlib import Path
from datetime import date

import pandas as pd

from src.pipeline.contracts import ConstructorOutput

# report_gerencias/ relativo a este módulo
# writer.py → parents[2] = projects/report_direccion/
_STAGING_DIR = Path(__file__).parents[2] / "report_gerencias"
_STAGING_CSV = _STAGING_DIR / "staging_despachos.csv"
_PENDIENTES_CSV = _STAGING_DIR / "pendientes_carga.csv"


def escribir_staging(
    df_staging: pd.DataFrame,
    df_pendientes: pd.DataFrame,
    informe: str,  # reservado para extensión futura (multi-informe)
    run_id: str,  # reservado para extensión futura (audit trail)
) -> ConstructorOutput:
    """
    Guarda el staging limpio y los pendientes como CSV separados por ';'.

    Returns
    -------
    ConstructorOutput
        staging_path apunta a staging_despachos.csv.
        registros_descartados = filas segregadas como pendientes.
    """
    errores: list[str] = []
    advertencias: list[str] = []

    _STAGING_DIR.mkdir(parents=True, exist_ok=True)

    if df_staging.empty:
        errores.append(
            "El DataFrame de staging está vacío — nada que guardar."
        )
        return ConstructorOutput(
            staging_path=_STAGING_CSV,
            registros_ok=0,
            registros_descartados=len(df_pendientes),
            errores=errores,
        )

    # Guardar staging principal
    df_staging.to_csv(_STAGING_CSV, sep=";", index=False, encoding="utf-8-sig")

    # Guardar pendientes (si hay), con fecha de creación para control de antigüedad  # noqa: E501
    if not df_pendientes.empty:
        df_pend = df_pendientes.copy()
        df_pend["_FECHA_CREACION"] = date.today().isoformat()
        df_pend.to_csv(
            _PENDIENTES_CSV, sep=";", index=False, encoding="utf-8-sig"
        )
        advertencias.append(
            f"{len(df_pendientes)} fila(s) segregadas como pendientes de aprobación."  # noqa: E501
        )

    return ConstructorOutput(
        staging_path=_STAGING_CSV,
        registros_ok=len(df_staging),
        registros_descartados=len(df_pendientes),
        errores=errores,
        advertencias=advertencias,
    )
