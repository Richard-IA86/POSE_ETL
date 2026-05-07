"""
Pipeline ETL principal.

Orquesta las etapas Extract → Transform → Load → Reportes
para la distribución diaria de variables de negocio.

Uso desde línea de comandos:

    python -m src.etl.pipeline --fuente csv --archivo datos/variables.csv
    python -m src.etl.pipeline --fuente bd --consulta "SELECT * FROM variables WHERE fecha = CURRENT_DATE"  # noqa: E501
    python -m src.etl.pipeline --fuente api --endpoint variables/diarias

El proceso genera:
  1. Registros válidos cargados en destino (BD o archivo).
  2. Archivo de auditoría con registros rechazados.
  3. Reportes de gestión (resumen, alertas, distribución, completo).
"""

import argparse
import logging
import sys
from datetime import date
from typing import Optional

import pandas as pd

from .carga import Carga
from .config import ConfigETL
from .ingesta import Ingesta
from .reportes import GeneradorReportes
from .transformacion import Transformacion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class Pipeline:
    """Orquestador del proceso ETL completo."""

    def __init__(self, config: Optional[ConfigETL] = None):
        self.config = config or ConfigETL()
        self.ingesta = Ingesta(self.config)
        self.transformacion = Transformacion()
        self.carga = Carga(self.config)
        self.reportes = GeneradorReportes(self.config)

    # ------------------------------------------------------------------
    # Métodos de extracción según fuente
    # ------------------------------------------------------------------

    def _extraer(
        self,
        fuente: str,
        archivo: Optional[str] = None,
        consulta: Optional[str] = None,
        endpoint: Optional[str] = None,
        hoja: str | int = 0,
    ) -> pd.DataFrame:
        """Delega la extracción al módulo de Ingesta según la fuente
        indicada."""
        fuente = fuente.lower()
        if fuente == "csv":
            if not archivo:
                raise ValueError("Debe indicar --archivo para fuente 'csv'.")
            return self.ingesta.desde_csv(archivo)
        if fuente == "excel":
            if not archivo:
                raise ValueError("Debe indicar --archivo para fuente 'excel'.")
            return self.ingesta.desde_excel(archivo, hoja=hoja)
        if fuente == "json":
            if not archivo:
                raise ValueError("Debe indicar --archivo para fuente 'json'.")
            return self.ingesta.desde_json(archivo)
        if fuente == "bd":
            if not consulta:
                raise ValueError("Debe indicar --consulta para fuente 'bd'.")
            return self.ingesta.desde_bd(consulta)
        if fuente == "api":
            if not endpoint:
                raise ValueError("Debe indicar --endpoint para fuente 'api'.")
            return self.ingesta.desde_api(endpoint)
        raise ValueError(
            f"Fuente no soportada: '{fuente}'."
            " Use: csv, excel, json, bd, api."
        )

    # ------------------------------------------------------------------
    # Pipeline completo
    # ------------------------------------------------------------------

    def ejecutar(
        self,
        fuente: str,
        archivo: Optional[str] = None,
        consulta: Optional[str] = None,
        endpoint: Optional[str] = None,
        destino: str = "archivo",
        tabla_destino: str = "variables_diarias",
        fecha: Optional[str] = None,
    ) -> dict:
        """
        Ejecuta el pipeline ETL completo.

        Parámetros:
            fuente         – Origen: csv | excel | json | bd | api.
            archivo        – Ruta al archivo (para fuente csv/excel/json).
            consulta       – Consulta SQL (para fuente bd).
            endpoint       – Endpoint de la API (para fuente api).
            destino        – Destino de carga: 'archivo' | 'bd'.
            tabla_destino  – Nombre de tabla cuando destino='bd'.
            fecha          – Proceso (YYYYMMDD). Usa hoy si no se indica.

        Retorna un diccionario con estadísticas del proceso.
        """
        fecha = fecha or date.today().strftime("%Y%m%d")
        logger.info("=" * 60)
        logger.info(
            "INICIO PIPELINE ETL  |  fecha=%s  |  fuente=%s", fecha, fuente
        )
        logger.info("=" * 60)

        # ---- EXTRACT ----
        logger.info("[1/4] Extracción")
        df_raw = self._extraer(
            fuente=fuente,
            archivo=archivo,
            consulta=consulta,
            endpoint=endpoint,
        )

        # ---- TRANSFORM ----
        logger.info("[2/4] Transformación")
        df_validos, df_invalidos = self.transformacion.transformar(df_raw)

        # ---- LOAD ----
        logger.info("[3/4] Carga")
        ruta_rechazos = self.carga.registrar_rechazos(
            df_invalidos, fecha=fecha
        )

        if destino == "bd":
            n_cargados = self.carga.a_bd(df_validos, tabla=tabla_destino)
            ruta_salida = None
        else:
            ruta_salida = self.carga.a_csv(
                df_validos, f"variables_{fecha}.csv"
            )
            n_cargados = len(df_validos)

        # ---- REPORTES ----
        logger.info("[4/4] Generación de reportes")
        ruta_reporte = self.reportes.reporte_completo(
            df_validos, df_invalidos, fecha=fecha
        )

        estadisticas = {
            "fecha": fecha,
            "registros_extraidos": len(df_raw),
            "registros_validos": len(df_validos),
            "registros_rechazados": len(df_invalidos),
            "registros_cargados": n_cargados,
            "ruta_salida": str(ruta_salida) if ruta_salida else None,
            "ruta_rechazos": str(ruta_rechazos) if ruta_rechazos else None,
            "ruta_reporte": str(ruta_reporte),
        }

        logger.info("=" * 60)
        logger.info("FIN PIPELINE ETL")
        for clave, valor in estadisticas.items():
            logger.info("  %-28s %s", clave + ":", valor)
        logger.info("=" * 60)

        return estadisticas


# ---------------------------------------------------------------------------
# Punto de entrada CLI
# ---------------------------------------------------------------------------


def _construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline ETL para distribución diaria de variables de negocio."
        )
    )
    parser.add_argument(
        "--fuente",
        required=True,
        choices=["csv", "excel", "json", "bd", "api"],
        help="Fuente de los datos de entrada.",
    )
    parser.add_argument(
        "--archivo", help="Ruta al archivo de entrada (csv/excel/json)."
    )
    parser.add_argument("--consulta", help="Consulta SQL (fuente=bd).")
    parser.add_argument("--endpoint", help="Endpoint de la API (fuente=api).")
    parser.add_argument(
        "--destino",
        choices=["archivo", "bd"],
        default="archivo",
        help="Destino de carga de los datos válidos.",
    )
    parser.add_argument(
        "--tabla",
        default="variables_diarias",
        help="Tabla destino (destino=bd).",
    )
    parser.add_argument(
        "--fecha",
        help="Fecha del proceso en formato YYYYMMDD. Por defecto: hoy.",
    )
    return parser


if __name__ == "__main__":
    args = _construir_parser().parse_args()
    pipeline = Pipeline()
    resultado = pipeline.ejecutar(
        fuente=args.fuente,
        archivo=args.archivo,
        consulta=args.consulta,
        endpoint=args.endpoint,
        destino=args.destino,
        tabla_destino=args.tabla,
        fecha=args.fecha,
    )
    sys.exit(0 if resultado["registros_rechazados"] == 0 else 1)
