"""
Módulo de Transformación (Transform).

Aplica limpieza, normalización y las reglas de negocio
sobre el DataFrame extraído durante la ingesta.

Flujo de transformación:
  1. Normalizar nombres de columnas.
  2. Convertir tipos de datos.
  3. Validar campos obligatorios y rangos.
  4. Calcular variables derivadas (distribución, variación, alertas).
  5. Enriquecer con categorías.
  6. Separar registros válidos de inválidos.
"""

import logging
from typing import Tuple

import pandas as pd

from src.maestros.reglas_negocio.reglas import (
    asignar_categoria_distribucion,
    calcular_distribucion_diaria,
    calcular_variacion_respecto_anterior,
    clasificar_alerta,
    validar_campos_obligatorios,
    validar_categoria,
    validar_rango_valor,
)

logger = logging.getLogger(__name__)


class Transformacion:
    """Aplica el conjunto completo de transformaciones ETL a un DataFrame."""

    # ------------------------------------------------------------------
    # Limpieza
    # ------------------------------------------------------------------

    @staticmethod
    def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        """Convierte nombres de columnas a snake_case minúsculas sin
        espacios."""
        df = df.copy()
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[áàä]", "a", regex=True)
            .str.replace(r"[éèë]", "e", regex=True)
            .str.replace(r"[íìï]", "i", regex=True)
            .str.replace(r"[óòö]", "o", regex=True)
            .str.replace(r"[úùü]", "u", regex=True)
            .str.replace(r"[ñ]", "n", regex=True)
            .str.replace(r"[^a-z0-9_]", "_", regex=True)
        )
        return df

    @staticmethod
    def limpiar_espacios(df: pd.DataFrame) -> pd.DataFrame:
        """Elimina espacios al inicio/fin en columnas de texto."""
        df = df.copy()
        cols_texto = df.select_dtypes(include=["object", "str"]).columns
        df[cols_texto] = df[cols_texto].apply(lambda s: s.str.strip())
        return df

    @staticmethod
    def convertir_fecha(
        df: pd.DataFrame, columna: str = "fecha"
    ) -> pd.DataFrame:
        """Convierte la columna de fecha al tipo datetime64."""
        df = df.copy()
        if columna in df.columns:
            df[columna] = pd.to_datetime(
                df[columna], errors="coerce", format="mixed", dayfirst=True
            )
            n_invalidos = df[columna].isna().sum()
            if n_invalidos:
                logger.warning("Fechas no convertidas: %d", n_invalidos)
        return df

    @staticmethod
    def convertir_valor_numerico(
        df: pd.DataFrame, columna: str = "valor"
    ) -> pd.DataFrame:
        """
        Normaliza separadores de miles/decimales y convierte la columna
        'valor' a numérico.  Acepta formatos como '1.234,56' y '1,234.56'.
        """
        df = df.copy()
        if columna in df.columns:
            serie = df[columna].astype(str)
            # Detectar si usa punto como separador de miles y coma como decimal
            usa_coma_decimal = serie.str.contains(
                r"\d\.\d{3},\d", na=False
            ).any()
            if usa_coma_decimal:
                serie = serie.str.replace(".", "", regex=False).str.replace(
                    ",", ".", regex=False
                )
            else:
                serie = serie.str.replace(",", "", regex=False)
            df[columna] = pd.to_numeric(serie, errors="coerce")
        return df

    # ------------------------------------------------------------------
    # Validación + reglas de negocio
    # ------------------------------------------------------------------

    @staticmethod
    def aplicar_validaciones(df: pd.DataFrame) -> pd.DataFrame:
        """Ejecuta todas las validaciones de integridad de datos."""
        df = validar_campos_obligatorios(df)
        df = validar_rango_valor(df)
        df = validar_categoria(df)
        return df

    @staticmethod
    def aplicar_calculos(df: pd.DataFrame) -> pd.DataFrame:
        """Ejecuta los cálculos derivados definidos en las reglas de
        negocio."""
        df = calcular_distribucion_diaria(df)
        df = calcular_variacion_respecto_anterior(df)
        df = clasificar_alerta(df)
        df = asignar_categoria_distribucion(df)
        return df

    # ------------------------------------------------------------------
    # Pipeline completo
    # ------------------------------------------------------------------

    def transformar(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Ejecuta todas las etapas de transformación.

        Retorna:
            validos   – registros que superaron todas las validaciones.
            invalidos – registros rechazados con columna 'motivo_rechazo'.
        """
        logger.info("Inicio transformación: %d registros", len(df))

        df = self.normalizar_columnas(df)
        df = self.limpiar_espacios(df)
        df = self.convertir_fecha(df)
        df = self.convertir_valor_numerico(df)
        df = self.aplicar_validaciones(df)
        df = self.aplicar_calculos(df)

        # Separar válidos e inválidos
        mascara_invalidos = (
            (df.get("error_campos", pd.Series("", index=df.index)) != "")
            | df.get("fuera_rango", pd.Series(False, index=df.index))
            | df.get("categoria_invalida", pd.Series(False, index=df.index))
        )

        invalidos = df[mascara_invalidos].copy()
        invalidos["motivo_rechazo"] = (
            invalidos.get("error_campos", pd.Series("", index=invalidos.index))
            .where(
                invalidos.get(
                    "error_campos", pd.Series("", index=invalidos.index)
                )
                != ""
            )
            .fillna("")
            + invalidos.get(
                "fuera_rango", pd.Series(False, index=invalidos.index)
            ).map({True: " | valor fuera de rango", False: ""})
            + invalidos.get(
                "categoria_invalida", pd.Series(False, index=invalidos.index)
            ).map({True: " | categoría inválida", False: ""})
        ).str.strip(" |")

        validos = df[~mascara_invalidos].copy()

        logger.info(
            "Transformación completada: %d válidos, %d rechazados",
            len(validos),
            len(invalidos),
        )
        return validos, invalidos
