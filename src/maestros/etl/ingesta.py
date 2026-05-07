"""
Módulo de Ingesta (Extract).

Responsabilidades:
  - Leer datos desde archivos CSV / Excel / JSON.
  - Leer datos desde base de datos relacional via SQLAlchemy.
  - Leer datos desde APIs REST externas.
  - Devolver DataFrames listos para la etapa de Transformación.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests  # type: ignore[import-untyped]
from sqlalchemy import create_engine, text

from .config import ConfigETL

logger = logging.getLogger(__name__)


class Ingesta:
    """Extrae datos de distintas fuentes y los devuelve como DataFrame."""

    def __init__(self, config: Optional[ConfigETL] = None):
        self.config = config or ConfigETL()

    # ------------------------------------------------------------------
    # Fuentes de archivos
    # ------------------------------------------------------------------

    def desde_csv(self, ruta: str | Path) -> pd.DataFrame:
        """Lee un archivo CSV con el encoding y separador configurados."""
        ruta = Path(ruta)
        logger.info("Ingesta CSV: %s", ruta)
        df = pd.read_csv(
            ruta,
            sep=self.config.separador_csv,
            encoding=self.config.encoding,
            dtype=str,  # Leer todo como texto; transformación normaliza tipos
        )
        logger.info("Registros leídos: %d", len(df))
        return df

    def desde_excel(
        self, ruta: str | Path, hoja: str | int = 0
    ) -> pd.DataFrame:
        """Lee una hoja de un archivo Excel (.xlsx / .xls)."""
        ruta = Path(ruta)
        logger.info("Ingesta Excel: %s [hoja=%s]", ruta, hoja)
        df = pd.read_excel(ruta, sheet_name=hoja, dtype=str)
        logger.info("Registros leídos: %d", len(df))
        return df

    def desde_json(self, ruta: str | Path) -> pd.DataFrame:
        """Lee un archivo JSON y lo convierte en DataFrame."""
        ruta = Path(ruta)
        logger.info("Ingesta JSON: %s", ruta)
        df = pd.read_json(ruta, dtype=str)
        logger.info("Registros leídos: %d", len(df))
        return df

    # ------------------------------------------------------------------
    # Fuente: Base de datos
    # ------------------------------------------------------------------

    def desde_bd(self, consulta: str) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL contra la base de datos configurada
        y devuelve el resultado como DataFrame.
        """
        engine = create_engine(self.config.db.url)
        logger.info("Ingesta BD: ejecutando consulta")
        with engine.connect() as conexion:
            df = pd.read_sql(text(consulta), conexion)
        logger.info("Registros leídos: %d", len(df))
        return df

    def desde_bd_tabla(
        self, tabla: str, schema: Optional[str] = None
    ) -> pd.DataFrame:
        """Lee una tabla completa desde la base de datos."""
        nombre_completo = f"{schema}.{tabla}" if schema else tabla
        return self.desde_bd(f"SELECT * FROM {nombre_completo}")

    # ------------------------------------------------------------------
    # Fuente: API REST
    # ------------------------------------------------------------------

    def desde_api(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Consume un endpoint REST que devuelve JSON y lo convierte
        en DataFrame.  Incluye el token de autorización configurado.
        """
        url = f"{self.config.reportes.api_url}/{endpoint.lstrip('/')}"
        _headers = {
            "Authorization": f"Bearer {self.config.reportes.api_token}"
        }
        if headers:
            _headers.update(headers)

        logger.info("Ingesta API: GET %s", url)
        respuesta = requests.get(
            url, params=params, headers=_headers, timeout=30
        )
        respuesta.raise_for_status()

        datos = respuesta.json()
        # Acepta lista de objetos o objeto con clave "data"
        if isinstance(datos, list):
            df = pd.DataFrame(datos)
        elif isinstance(datos, dict) and "data" in datos:
            df = pd.DataFrame(datos["data"])
        else:
            df = pd.json_normalize(datos)

        logger.info("Registros leídos: %d", len(df))
        return df
