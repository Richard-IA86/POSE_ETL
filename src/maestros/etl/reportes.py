"""
Módulo de Reportes.

Genera y descarga los reportes de gestión en formatos estándar
(Excel y CSV).  Cada reporte encapsula una vista del negocio:

  - ResumenDiario  : totales y participaciones por fecha y variable.
  - AlertasDiarias : registros que superaron el umbral de variación.
  - DistribucionCentroCosto: distribución por centro de costo.
  - ReporteCompleto: descarga consolidada con todas las vistas.
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import ConfigETL

logger = logging.getLogger(__name__)


class GeneradorReportes:
    """Genera reportes de gestión a partir de DataFrames transformados."""

    def __init__(self, config: Optional[ConfigETL] = None):
        self.config = config or ConfigETL()
        self._dir = self.config.reportes.directorio_salida

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _nombre_archivo(
        self, prefijo: str, fecha: Optional[str] = None
    ) -> str:
        fecha_str = fecha or date.today().strftime("%Y%m%d")
        return f"{prefijo}_{fecha_str}.{self.config.reportes.formato}"

    def _guardar_excel(
        self, hojas: dict[str, pd.DataFrame], nombre_archivo: str
    ) -> Path:
        """Guarda múltiples DataFrames en hojas de un mismo Excel."""
        ruta = self._dir / nombre_archivo
        with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
            for nombre_hoja, df in hojas.items():
                df.to_excel(writer, sheet_name=nombre_hoja[:31], index=False)
        logger.info("Reporte generado: %s", ruta)
        return ruta

    def _guardar_csv(self, df: pd.DataFrame, nombre_archivo: str) -> Path:
        """Guarda un DataFrame como CSV."""
        ruta = self._dir / nombre_archivo
        df.to_csv(
            ruta,
            sep=self.config.separador_csv,
            encoding=self.config.encoding,
            index=False,
        )
        logger.info("Reporte CSV generado: %s", ruta)
        return ruta

    # ------------------------------------------------------------------
    # Reportes de gestión
    # ------------------------------------------------------------------

    def resumen_diario(
        self, df: pd.DataFrame, fecha: Optional[str] = None
    ) -> Path:
        """
        Genera el reporte de resumen diario:
        totales, promedios y participación por variable y fecha.
        """
        columnas_grupo = ["fecha", "variable"]
        resumen = (
            df.groupby(columnas_grupo)["valor_num"]
            .agg(total="sum", promedio="mean", cantidad="count")
            .reset_index()
        )
        resumen["total_general"] = resumen.groupby("fecha")["total"].transform(
            "sum"
        )
        resumen["participacion_pct"] = (
            resumen["total"] / resumen["total_general"] * 100
        ).round(4)
        resumen["generado_en"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        nombre = self._nombre_archivo("resumen_diario", fecha)
        if self.config.reportes.formato == "csv":
            return self._guardar_csv(resumen, nombre)
        return self._guardar_excel({"Resumen Diario": resumen}, nombre)

    def alertas_diarias(
        self, df: pd.DataFrame, fecha: Optional[str] = None
    ) -> Path:
        """
        Genera el reporte de alertas: registros con variación superior
        al umbral definido en las reglas de negocio.
        """
        if "alerta" not in df.columns:
            logger.warning(
                "El DataFrame no contiene columna 'alerta'. Reporte vacío."
            )
            alertas = pd.DataFrame()
        else:
            alertas = df[df["alerta"]].copy()

        alertas["generado_en"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nombre = self._nombre_archivo("alertas_diarias", fecha)

        if self.config.reportes.formato == "csv":
            return self._guardar_csv(alertas, nombre)
        return self._guardar_excel({"Alertas": alertas}, nombre)

    def distribucion_centro_costo(
        self, df: pd.DataFrame, fecha: Optional[str] = None
    ) -> Path:
        """
        Genera el reporte de distribución por centro de costo,
        con participación y categoría de cada centro.
        """
        columnas_grupo = ["fecha", "centro_costo", "variable"]
        disponibles = [c for c in columnas_grupo if c in df.columns]

        dist = (
            df.groupby(disponibles)["valor_num"]
            .agg(total="sum", cantidad="count")
            .reset_index()
        )
        if "participacion_pct" in df.columns:
            dist = dist.merge(
                df[
                    disponibles + ["participacion_pct", "categoria"]
                ].drop_duplicates(),
                on=disponibles,
                how="left",
            )
        dist["generado_en"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        nombre = self._nombre_archivo("distribucion_cc", fecha)
        if self.config.reportes.formato == "csv":
            return self._guardar_csv(dist, nombre)
        return self._guardar_excel({"Distribución CC": dist}, nombre)

    def reporte_completo(
        self,
        df: pd.DataFrame,
        df_invalidos: pd.DataFrame,
        fecha: Optional[str] = None,
    ) -> Path:
        """
        Descarga consolidada con todas las vistas del negocio en un
        único archivo Excel con múltiples hojas.
        """
        columnas_grupo_resumen = ["fecha", "variable"]
        disponibles = [c for c in columnas_grupo_resumen if c in df.columns]

        resumen = (
            (
                df.groupby(disponibles)["valor_num"]
                .agg(total="sum", promedio="mean", cantidad="count")
                .reset_index()
            )
            if disponibles
            else pd.DataFrame()
        )

        alertas = (
            df[df["alerta"]].copy()
            if "alerta" in df.columns
            else pd.DataFrame()
        )

        dist_cc_cols = [
            c for c in ["fecha", "centro_costo", "variable"] if c in df.columns
        ]
        dist_cc = (
            (
                df.groupby(dist_cc_cols)["valor_num"]
                .agg(total="sum")
                .reset_index()
            )
            if dist_cc_cols
            else pd.DataFrame()
        )

        hojas = {
            "Resumen Diario": resumen,
            "Alertas": alertas,
            "Distribución CC": dist_cc,
            "Rechazados": df_invalidos,
        }

        nombre = self._nombre_archivo("reporte_completo", fecha)
        return self._guardar_excel(hojas, nombre)
