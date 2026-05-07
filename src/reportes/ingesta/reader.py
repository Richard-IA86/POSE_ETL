"""
reader.py — Lee el Excel crudo y produce InspectorOutput.

Responsabilidad única:
  - Abrir el Excel, calcular hash (o recibirlo del orquestador),
    detectar encabezados y mapearlos al catálogo BD,
    evaluar calidad mínima (columnas obligatorias presentes).

El hash lo RECIBE del pipeline_runner (ya calculado antes de abrir el archivo)
para no calcular dos veces.
"""

from pathlib import Path

import pandas as pd

from src.pipeline.contracts import InspectorOutput
from src.reportes.ingesta._mapeo import (
    MAPEO_COLUMNAS,
    COLUMNAS_OBLIGATORIAS,
    NOMBRE_HOJA,
    FILA_HEADERS,
)


def leer_excel_crudo(excel_path: Path, hash_archivo: str) -> InspectorOutput:
    """
    Lee el Excel crudo, mapea encabezados y valida presencia de columnas obligatorias.  # noqa: E501

    Parameters
    ----------
    excel_path : Path
        Ruta al archivo Excel crudo (sin normalizar).
    hash_archivo : str
        SHA-256 pre-calculado por pipeline_runner.

    Returns
    -------
    InspectorOutput
        calidad_ok=True si todas las columnas obligatorias están presentes y mapeadas.  # noqa: E501
    """
    errores: list[str] = []
    advertencias: list[str] = []

    # --- Leer encabezados del Excel (solo primera fila real de datos, sin cargar filas) ---  # noqa: E501
    try:
        df_header = pd.read_excel(
            excel_path,
            sheet_name=NOMBRE_HOJA,
            header=FILA_HEADERS,
            nrows=0,
        )
    except Exception as exc:
        return InspectorOutput(
            excel_path=excel_path,
            hash_sha256=hash_archivo,
            headers_mapeados={},
            calidad_ok=False,
            errores=[f"No se pudo leer el Excel: {exc}"],
        )

    columnas_excel = [str(c).strip() for c in df_header.columns]

    # --- Mapear encabezados ---
    headers_mapeados: dict[str, str] = {}
    for col_excel in columnas_excel:
        col_bd = MAPEO_COLUMNAS.get(col_excel)
        if col_bd:
            headers_mapeados[col_excel] = col_bd
        else:
            advertencias.append(
                f"Columna sin mapeo (se ignorará): '{col_excel}'"
            )

    # --- Validar columnas obligatorias ---
    mapeadas_bd = set(headers_mapeados.values())
    for col_obligatoria in COLUMNAS_OBLIGATORIAS:
        if col_obligatoria not in mapeadas_bd:
            errores.append(
                f"Columna obligatoria ausente en el Excel: '{col_obligatoria}'"
            )

    calidad_ok = len(errores) == 0

    return InspectorOutput(
        excel_path=excel_path,
        hash_sha256=hash_archivo,
        headers_mapeados=headers_mapeados,
        calidad_ok=calidad_ok,
        errores=errores,
        advertencias=advertencias,
    )
