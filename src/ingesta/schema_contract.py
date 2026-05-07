"""
Schema Contract — Maestro de columnas del normalizador POSE.

Define el orden y tipo canónicos de columnas que todo DataFrame
debe tener antes de ser exportado a Excel/CSV.
Uso: aplicar_schema_contract(df) -> DataFrame
"""

import pandas as pd
from typing import Any

# ---------------------------------------------------------------------------
# Columnas canónicas en el orden de salida esperado por Power Query.
# Columnas de auditoría (prefijo _) van al final.
# ---------------------------------------------------------------------------
COLUMNAS_CANONICAS: list[str] = [
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
    # Auditoría
    "_ARCHIVO_ORIGEN",
    "_HOJA_ORIGEN",
    "_RUTA_ORIGEN",
]

# Tipos destino por columna (usados en conversión y validación).
TIPOS_CANONICOS: dict[str, str] = {
    "OBRA_PRONTO": "string",
    "DESCRIPCION_OBRA": "string",
    "FECHA": "string",
    "FUENTE": "string",
    "TIPO_COMPROBANTE": "string",
    "NRO_COMPROBANTE": "string",
    "PROVEEDOR": "string",
    "DETALLE": "string",
    "CODIGO_CUENTA": "string",
    "IMPORTE": "float64",
    "OBSERVACION": "string",
    "RUBRO_CONTABLE": "string",
    "CUENTA_CONTABLE": "string",
    "_ARCHIVO_ORIGEN": "string",
    "_HOJA_ORIGEN": "string",
    "_RUTA_ORIGEN": "string",
}


def aplicar_schema_contract(
    df: pd.DataFrame,
    rellenar_faltantes: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Alinea *df* al schema canónico antes de exportar.

    Pasos:
    1. Añade columnas faltantes con valor nulo.
    2. Descarta columnas extra que no pertenecen al schema
       (excepto las de auditoría _ID_INGESTA que ya se eliminan
       en writer.py).
    3. Reordena al orden canónico.
    4. Devuelve (df_ajustado, informe) donde *informe* registra
       columnas añadidas y descartadas.

    Args:
        df: DataFrame de entrada post-normalización.
        rellenar_faltantes: Si True, añade columnas ausentes con None.

    Returns:
        Tupla (DataFrame ajustado, dict con informe de cambios).
    """
    if df is None or df.empty:
        return df, {}

    columnas_entrada = set(df.columns)

    # Columnas internas que NO forman parte del schema pero
    # que el pipeline aún necesita (se preservan tal cual).
    preservar = {"_ID_INGESTA"}

    # Columnas del schema que faltan en el DataFrame
    faltantes = [c for c in COLUMNAS_CANONICAS if c not in columnas_entrada]

    # Columnas extra no contempladas en el schema ni preservadas
    extras = sorted(columnas_entrada - set(COLUMNAS_CANONICAS) - preservar)

    informe: dict[str, Any] = {
        "columnas_faltantes_agregadas": faltantes,
        "columnas_extra_descartadas": extras,
    }

    # 1. Añadir columnas faltantes
    if rellenar_faltantes and faltantes:
        for col in faltantes:
            df[col] = None

    # 2. Descartar extras
    if extras:
        df = df.drop(columns=extras, errors="ignore")

    # 3. Reordenar: canónicas primero, luego las preservadas que existan
    presentes_canonicas = [c for c in COLUMNAS_CANONICAS if c in df.columns]
    presentes_preservadas = [c for c in preservar if c in df.columns]
    df = df[presentes_canonicas + presentes_preservadas]

    return df, informe
