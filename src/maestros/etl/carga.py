"""
Módulo de Carga (Load).

Persiste los datos transformados en el destino final:
  - Base de datos relacional (upsert por llave primaria).
  - Archivo CSV de salida.
  - Archivo Excel de salida.
  - Registro de rechazos en archivo de auditoría.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from .config import ConfigETL

logger = logging.getLogger(__name__)


class Carga:
    """Carga DataFrames al destino configurado."""

    def __init__(self, config: Optional[ConfigETL] = None):
        self.config = config or ConfigETL()

    # ------------------------------------------------------------------
    # Carga a base de datos
    # ------------------------------------------------------------------

    def a_bd(
        self,
        df: pd.DataFrame,
        tabla: str,
        schema: Optional[str] = None,
        if_exists: str = "append",
    ) -> int:
        """
        Carga el DataFrame en la tabla indicada.

        Parámetros:
            if_exists – 'append' (por defecto) | 'replace' | 'fail'

        Retorna el número de filas insertadas.
        """
        engine = create_engine(self.config.db.url)
        n = len(df)
        df.to_sql(
            name=tabla,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=self.config.batch_size,
        )
        logger.info(
            "Cargados %d registros en %s.%s", n, schema or "public", tabla
        )
        return n

    # ------------------------------------------------------------------
    # Carga a archivos
    # ------------------------------------------------------------------

    def a_csv(self, df: pd.DataFrame, nombre_archivo: str) -> Path:
        """Guarda el DataFrame como CSV en el directorio de datos
        configurado."""
        ruta = self.config.directorio_datos / nombre_archivo
        df.to_csv(
            ruta,
            sep=self.config.separador_csv,
            encoding=self.config.encoding,
            index=False,
        )
        logger.info("Archivo CSV generado: %s (%d registros)", ruta, len(df))
        return ruta

    def a_excel(
        self,
        df: pd.DataFrame,
        nombre_archivo: str,
        nombre_hoja: str = "Datos",
    ) -> Path:
        """Guarda el DataFrame como Excel en el directorio de datos
        configurado."""
        ruta = self.config.directorio_datos / nombre_archivo
        with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        logger.info("Archivo Excel generado: %s (%d registros)", ruta, len(df))
        return ruta

    # ------------------------------------------------------------------
    # Auditoría de rechazos
    # ------------------------------------------------------------------

    def registrar_rechazos(
        self, df_invalidos: pd.DataFrame, fecha: Optional[str] = None
    ) -> Optional[Path]:
        """
        Persiste los registros rechazados en un CSV de auditoría.
        Si no hay rechazos, no genera archivo.

        Retorna la ruta del archivo creado o None.
        """
        if df_invalidos.empty:
            logger.info(
                "Sin registros rechazados. No se genera archivo de auditoría."
            )
            return None

        fecha_str = fecha or date.today().strftime("%Y%m%d")
        nombre = f"rechazos_{fecha_str}.csv"
        ruta = self.config.directorio_datos / nombre
        df_invalidos.to_csv(
            ruta,
            sep=self.config.separador_csv,
            encoding=self.config.encoding,
            index=False,
        )
        logger.warning(
            "Auditoría de rechazos: %d registros → %s", len(df_invalidos), ruta
        )
        return ruta
