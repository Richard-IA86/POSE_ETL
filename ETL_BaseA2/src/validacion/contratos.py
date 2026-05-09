"""
Contratos de Validacion — ETL Base A2.

Modulo reutilizable para validar DataFrames normalizados antes
de ser escritos a disco o cargados a BD.

Cada funcion retorna una lista de strings con los errores
encontrados. Lista vacia = contrato cumplido.

Uso tipico:
    from src.validacion.contratos import (
        validar_schema,
        validar_integridad,
        resumen_calidad,
    )
    errores = validar_schema(df) + validar_integridad(df)
    if errores:
        raise ValueError("\\n".join(errores))
"""

from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Schema canónico (16 columnas + columnas de auditoria _*)
# ---------------------------------------------------------------------------

COLUMNAS_NEGOCIO: list[str] = [
    "OBRA_PRONTO",
    "DESCRIPCION_OBRA",
    "FECHA",
    "FUENTE",
    "TIPO_COMPROBANTE",
    "NRO_COMPROBANTE",
    "PROVEEDOR",
    "DETALLE",
    "CODIGO_CUENTA",
    "IMPORTE",
    "OBSERVACION",
    "RUBRO_CONTABLE",
    "CUENTA_CONTABLE",
]

COLUMNAS_AUDITORIA: list[str] = [
    "_ARCHIVO_ORIGEN",
    "_HOJA_ORIGEN",
    "_RUTA_ORIGEN",
]

COLUMNAS_CRITICAS: list[str] = [
    "OBRA_PRONTO",
    "FECHA",
    "FUENTE",
    "IMPORTE",
]

COLUMNAS_CANONICAS: list[str] = COLUMNAS_NEGOCIO + COLUMNAS_AUDITORIA


# ---------------------------------------------------------------------------
# Contrato 1 — Schema
# ---------------------------------------------------------------------------


def validar_schema(df: pd.DataFrame) -> list[str]:
    """
    Verifica que el DataFrame contenga todas las columnas
    canonicas de negocio. Las columnas de auditoria (_*) son
    opcionales.

    Retorna lista de errores (vacia = OK).
    """
    errores: list[str] = []
    presentes = {c.upper() for c in df.columns}

    for col in COLUMNAS_NEGOCIO:
        if col.upper() not in presentes:
            errores.append(f"schema: columna ausente — {col}")

    extras = presentes - {c.upper() for c in COLUMNAS_CANONICAS}
    for col in sorted(extras):
        errores.append(f"schema: columna no reconocida — {col}")

    return errores


# ---------------------------------------------------------------------------
# Contrato 2 — Integridad
# ---------------------------------------------------------------------------


def validar_integridad(df: pd.DataFrame) -> list[str]:
    """
    Verifica integridad de datos:
    - Columnas criticas sin nulos
    - IMPORTE debe ser numerico
    - DataFrame no puede estar vacio

    Retorna lista de errores (vacia = OK).
    """
    errores: list[str] = []

    if df.empty:
        errores.append("integridad: DataFrame vacio (0 filas)")
        return errores

    cols_up = {c.upper(): c for c in df.columns}

    for col in COLUMNAS_CRITICAS:
        if col.upper() not in cols_up:
            continue
        col_real = cols_up[col.upper()]
        nulos = int(df[col_real].isna().sum())
        if nulos > 0:
            errores.append(
                f"integridad: {col} tiene {nulos} nulos"
                f" ({nulos / len(df) * 100:.1f}%)"
            )

    if "IMPORTE" in cols_up:
        col_imp = cols_up["IMPORTE"]
        no_num = pd.to_numeric(df[col_imp], errors="coerce").isna().sum()
        if no_num > 0:
            errores.append(
                f"integridad: IMPORTE tiene {no_num}" " valores no numericos"
            )

    return errores


# ---------------------------------------------------------------------------
# Contrato 3 — Rango de fechas
# ---------------------------------------------------------------------------

RANGOS_SEGMENTO: dict[str, tuple[str, str]] = {
    "2021_2022_Historico": ("2021-01-01", "2022-12-31"),
    "2023_2025_Hist": ("2023-01-01", "2025-07-31"),
    "2025": ("2025-08-01", "2025-12-31"),
    "2025_Ajustes": ("2020-01-01", "2025-12-31"),
    "2025_Compensaciones": ("2020-01-01", "2025-12-31"),
    "2026": ("2026-01-01", "2026-12-31"),
    "Modificaciones": ("2019-01-01", "2026-12-31"),
}


def validar_rango_fecha(df: pd.DataFrame, segmento: str) -> list[str]:
    """
    Verifica que las fechas del DataFrame esten dentro del
    rango esperado para el segmento.
    Solo valida si el segmento esta en RANGOS_SEGMENTO.

    Retorna lista de errores (vacia = OK).
    """
    errores: list[str] = []

    if segmento not in RANGOS_SEGMENTO:
        return errores

    cols_up = {c.upper(): c for c in df.columns}
    if "FECHA" not in cols_up:
        return errores

    col_fecha = cols_up["FECHA"]
    fechas = pd.to_datetime(df[col_fecha], errors="coerce", dayfirst=True)

    fecha_min_str, fecha_max_str = RANGOS_SEGMENTO[segmento]
    fecha_min = pd.Timestamp(fecha_min_str)
    fecha_max = pd.Timestamp(fecha_max_str)

    fuera = ((fechas < fecha_min) | (fechas > fecha_max)).sum()
    if fuera > 0:
        errores.append(
            f"rango_fecha [{segmento}]: {fuera} filas fuera de"
            f" [{fecha_min_str}, {fecha_max_str}]"
        )

    return errores


# ---------------------------------------------------------------------------
# Calidad — resumen de completitud
# ---------------------------------------------------------------------------


def resumen_calidad(df: pd.DataFrame) -> dict[str, Any]:
    """
    Calcula metricas de completitud del DataFrame normalizado.

    Retorna dict con:
      filas        — total de filas
      columnas     — total de columnas
      completitud  — dict col -> % no nulo (solo columnas negocio)
      importe_sum  — suma de IMPORTE (None si no existe)
      obras_unicas — valores unicos de OBRA_PRONTO (None si no existe)
    """
    cols_up = {c.upper(): c for c in df.columns}
    n = len(df)

    completitud: dict[str, float] = {}
    for col in COLUMNAS_NEGOCIO:
        if col.upper() in cols_up:
            col_real = cols_up[col.upper()]
            no_nulos = int(df[col_real].notna().sum())
            completitud[col] = round(no_nulos / n * 100, 1) if n > 0 else 0.0
        else:
            completitud[col] = 0.0

    importe_sum: float | None = None
    if "IMPORTE" in cols_up:
        importe_sum = float(
            pd.to_numeric(df[cols_up["IMPORTE"]], errors="coerce").sum()
        )

    obras_unicas: int | None = None
    if "OBRA_PRONTO" in cols_up:
        obras_unicas = int(df[cols_up["OBRA_PRONTO"]].dropna().nunique())

    return {
        "filas": n,
        "columnas": len(df.columns),
        "completitud": completitud,
        "importe_sum": importe_sum,
        "obras_unicas": obras_unicas,
    }
