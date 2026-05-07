"""
Módulo de Reglas de Negocio.

Define las reglas que gobiernan el proceso ETL:
  - Validaciones de campos obligatorios y rangos aceptables.
  - Cálculos de variables de distribución diaria.
  - Clasificaciones y categorías del negocio.
  - Indicadores derivados (KPIs).

Principios de diseño:
  - Cada regla es una función pura (sin efectos secundarios).
  - Los nombres describen la intención del negocio, no la implementación.
  - Todas las funciones están documentadas y son fácilmente verificables.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes de negocio
# ---------------------------------------------------------------------------

CAMPOS_OBLIGATORIOS = [
    "id_registro",
    "fecha",
    "variable",
    "valor",
    "unidad",
    "centro_costo",
]

CATEGORIAS_VALIDAS = {"A", "B", "C", "D"}

UMBRAL_ALERTA_PORCENTAJE = Decimal("0.20")  # 20 % de desviación dispara alerta
VALOR_MINIMO_VARIABLE = Decimal("0")
VALOR_MAXIMO_VARIABLE = Decimal("9_999_999")


# ---------------------------------------------------------------------------
# Validaciones
# ---------------------------------------------------------------------------


def validar_campos_obligatorios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna 'error_campos' con los nombres de campos obligatorios
    que están vacíos o nulos en cada fila.
    """

    def _faltantes(fila: pd.Series) -> str:
        return ", ".join(
            campo
            for campo in CAMPOS_OBLIGATORIOS
            if campo in fila.index and pd.isna(fila[campo])
        )

    df = df.copy()
    df["error_campos"] = df.apply(_faltantes, axis=1)
    n_errores = (df["error_campos"] != "").sum()
    if n_errores:
        logger.warning("Registros con campos faltantes: %d", n_errores)
    return df


def validar_rango_valor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Marca en 'fuera_rango' los registros cuyo campo 'valor' no cae
    dentro del rango de negocio [VALOR_MINIMO_VARIABLE, VALOR_MAXIMO_VARIABLE].
    Si la columna 'valor' no existe, marca todos como fuera de rango.
    """
    df = df.copy()
    if "valor" not in df.columns:
        df["fuera_rango"] = True
        logger.warning(
            "Columna 'valor' ausente: todos los registros marcados"
            " como fuera de rango"
        )
        return df
    serie = pd.to_numeric(df["valor"], errors="coerce")
    df["fuera_rango"] = (
        serie.isna()
        | (serie < float(VALOR_MINIMO_VARIABLE))
        | (serie > float(VALOR_MAXIMO_VARIABLE))
    )
    n_fuera = df["fuera_rango"].sum()
    if n_fuera:
        logger.warning("Registros fuera de rango de valor: %d", n_fuera)
    return df


def validar_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """Marca 'categoria_invalida' para valores de categoría no reconocidos."""
    df = df.copy()
    if "categoria" in df.columns:
        df["categoria_invalida"] = ~df["categoria"].isin(CATEGORIAS_VALIDAS)
    else:
        df["categoria_invalida"] = False
    return df


# ---------------------------------------------------------------------------
# Cálculos de distribución diaria
# ---------------------------------------------------------------------------


def calcular_distribucion_diaria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la participación porcentual de cada registro respecto
    al total diario de la variable.

    Entrada esperada: columnas ['fecha', 'variable', 'valor'].
    Salida: agrega columna 'participacion_pct'.
    """
    df = df.copy()
    df["valor_num"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)

    total_por_fecha_variable = df.groupby(["fecha", "variable"])[
        "valor_num"
    ].transform("sum")
    df["participacion_pct"] = (
        df["valor_num"].div(total_por_fecha_variable.replace(0, pd.NA)) * 100
    ).round(4)
    return df


def calcular_variacion_respecto_anterior(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula la variación porcentual de 'valor_num' respecto al periodo
    inmediato anterior para cada combinación (variable, centro_costo).

    Precondición: el DataFrame debe estar ordenado por fecha.
    """
    df = df.copy()
    df["valor_num"] = pd.to_numeric(
        df.get("valor_num", df["valor"]), errors="coerce"
    )
    df = df.sort_values(["variable", "centro_costo", "fecha"])
    df["valor_anterior"] = df.groupby(["variable", "centro_costo"])[
        "valor_num"
    ].shift(1)
    df["variacion_pct"] = (
        (df["valor_num"] - df["valor_anterior"])
        / df["valor_anterior"].abs()
        * 100
    ).round(4)
    return df


def clasificar_alerta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Establece 'alerta' = True cuando la variación absoluta supera
    el umbral de negocio definido en UMBRAL_ALERTA_PORCENTAJE.
    """
    df = df.copy()
    if "variacion_pct" not in df.columns:
        df = calcular_variacion_respecto_anterior(df)
    umbral = float(UMBRAL_ALERTA_PORCENTAJE) * 100
    df["alerta"] = df["variacion_pct"].abs() > umbral
    return df


# ---------------------------------------------------------------------------
# Enriquecimiento
# ---------------------------------------------------------------------------


def asignar_categoria_distribucion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna una categoría de distribución (A/B/C/D) según
    la participación porcentual calculada (regla de Pareto adaptada):

      A  → participacion_pct >= 70 %
      B  → participacion_pct >= 30 % y < 70 %
      C  → participacion_pct >= 10 % y < 30 %
      D  → participacion_pct <  10 %
    """
    df = df.copy()
    if "participacion_pct" not in df.columns:
        df = calcular_distribucion_diaria(df)

    df["categoria"] = pd.Categorical(
        pd.Series(
            pd.cut(
                df["participacion_pct"],
                bins=[-float("inf"), 10, 30, 70, float("inf")],
                labels=["D", "C", "B", "A"],
                right=False,
            )
        ).astype(str),
        categories=list(CATEGORIAS_VALIDAS),
    )
    return df
