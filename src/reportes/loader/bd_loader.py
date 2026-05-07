"""
bd_loader.py — Carga el archivo staging a SQL Express en transacción atómica.

Estrategia: DELETE + INSERT dentro de una sola transacción.
Si algo falla → ROLLBACK automático. No contiene lógica de negocio.

Tabla destino: PRODUCCION.report_direccion
"""

from pathlib import Path

import pandas as pd
import pyodbc

from src.pipeline.contracts import CargadorOutput
from src.reportes.loader._conexion import (
    get_connection_string,
)

# Tabla destino en SQL Express
_SCHEMA = "PRODUCCION"
_TABLA = "report_direccion"
_TABLA_FULL = f"{_SCHEMA}.{_TABLA}"

# Columna que identifica el periodo para DELETE selectivo
_COL_PERIODO = "periodo"


def cargar_a_bd(
    staging_path: Path, run_id: str, informe: str
) -> CargadorOutput:
    """
    Carga el staging a la BD con estrategia DELETE + INSERT por periodo.

    - Lee todos los periodos únicos del staging.
    - Elimina esos periodos de la tabla destino.
    - Inserta todos los registros del staging.
    - Commit si todo OK, Rollback si cualquier error.

    Parameters
    ----------
    staging_path : Path
        Ruta al Excel de staging generado por el Constructor.
    run_id : str
        UUID del run para trazabilidad.
    informe : str
        Nombre del informe.

    Returns
    -------
    CargadorOutput
        Estado: 'OK' | 'ERROR' | 'ROLLBACK'
    """
    df = pd.read_excel(staging_path, dtype=str)

    conn_str = get_connection_string()
    conn = None

    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()

        # --- DELETE selectivo por periodos presentes en el staging ---
        periodos_unicos = df[_COL_PERIODO].dropna().unique().tolist()
        placeholders = ",".join("?" * len(periodos_unicos))
        cursor.execute(
            f"DELETE FROM {_TABLA_FULL} WHERE {_COL_PERIODO} IN ({placeholders})",  # noqa: E501
            periodos_unicos,
        )
        eliminados = cursor.rowcount

        # --- INSERT fila a fila (remplazar por bulk cuando el volumen lo justifique) ---  # noqa: E501
        columnas = [
            c for c in df.columns if c != "informe"
        ]  # 'informe' es columna de staging, no de BD
        col_list = ", ".join(columnas)
        placeholders_insert = ", ".join("?" * len(columnas))
        sql_insert = f"INSERT INTO {_TABLA_FULL} ({col_list}) VALUES ({placeholders_insert})"  # noqa: E501

        registros = 0
        for _, fila in df[columnas].iterrows():
            cursor.execute(sql_insert, fila.tolist())
            registros += 1

        conn.commit()

        return CargadorOutput(
            run_id=run_id,
            registros_cargados=registros,
            estado="OK",
            mensaje=f"DELETE {eliminados} registros previos. INSERT {registros} registros.",  # noqa: E501
        )

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return CargadorOutput(
            run_id=run_id,
            registros_cargados=0,
            estado="ROLLBACK",
            mensaje=str(exc),
        )
    finally:
        if conn:
            conn.close()
