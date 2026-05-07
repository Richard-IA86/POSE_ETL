"""
validador.py — Valida el CSV de staging antes de la carga a BD.

Reglas de validación para report_direccion:
  - El archivo existe y no está vacío.
  - Las columnas obligatorias de staging están presentes.
  - No hay nulos en columnas obligatorias de staging.
  - IMPORTE* es numérico (no texto no parseable).
  - FECHA* tiene formato parseable.
"""

from pathlib import Path

import pandas as pd

from src.pipeline.contracts import ValidadorOutput
from src.reportes.ingesta._mapeo import (
    COLUMNAS_OBLIGATORIAS_STAGING,
)


def validar_staging(staging_path: Path) -> ValidadorOutput:
    """
    Aplica reglas de validación al CSV de staging.

    Returns
    -------
    ValidadorOutput
        validacion_ok=True si todas las reglas pasan.
    """
    errores: list[str] = []
    advertencias: list[str] = []

    # Regla 1: el archivo existe
    if not staging_path.exists():
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"El archivo staging no existe: {staging_path}"],
        )

    # Regla 2: readable y no vacío
    try:
        df = pd.read_csv(
            staging_path, sep=";", dtype=str, encoding="utf-8-sig"
        )
    except Exception as exc:
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"No se pudo leer el staging: {exc}"],
        )

    if df.empty:
        return ValidadorOutput(
            validacion_ok=False,
            errores=["El archivo staging está vacío."],
        )

    # Regla 3: columnas obligatorias presentes
    cols_presentes = set(df.columns)
    for col in COLUMNAS_OBLIGATORIAS_STAGING:
        if col not in cols_presentes:
            errores.append(f"Columna obligatoria ausente en staging: '{col}'")

    if errores:
        return ValidadorOutput(validacion_ok=False, errores=errores)

    # Regla 4: sin nulos en columnas obligatorias
    for col in COLUMNAS_OBLIGATORIAS_STAGING:
        nulos = df[col].isna().sum()
        if nulos > 0:
            errores.append(f"Columna '{col}' tiene {nulos} valor(es) nulo(s).")

    # Regla 5: IMPORTE* numérico
    if "IMPORTE*" in df.columns:
        no_numericos = (
            pd.to_numeric(df["IMPORTE*"], errors="coerce").isna().sum()
        )
        if no_numericos > 0:
            advertencias.append(
                f"'IMPORTE*' tiene {no_numericos} valor(es) no numérico(s)."
            )

    # Regla 6: FECHA* parseable
    if "FECHA*" in df.columns:
        no_parseables = (
            pd.to_datetime(df["FECHA*"], errors="coerce").isna().sum()
        )
        if no_parseables > 0:
            errores.append(
                f"'FECHA*' tiene {no_parseables} valor(es) no parseables como fecha."  # noqa: E501
            )

    return ValidadorOutput(
        validacion_ok=len(errores) == 0,
        errores=errores,
        advertencias=advertencias,
    )
