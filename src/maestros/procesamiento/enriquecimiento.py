"""
src/procesamiento/enriquecimiento.py
─────────────────────────────────────
Lee los archivos Excel de input_raw/, aplica transformaciones y cruces
de datos, y genera DataFrames enriquecidos listos para informes.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from config.settings import INPUT_RAW_DIR

# ─── Carga de archivos raw ───────────────────────────────────────────────────


def cargar_archivos() -> dict[str, pd.DataFrame]:
    """
    Lee todos los Excel de input_raw/ y devuelve un dict:
        { "nombre_sin_extension": DataFrame }
    """
    dfs: dict[str, pd.DataFrame] = {}
    archivos = list(INPUT_RAW_DIR.glob("*.xlsx")) + list(
        INPUT_RAW_DIR.glob("*.xls")
    )

    if not archivos:
        logger.warning(f"No se encontraron archivos en {INPUT_RAW_DIR}")
        return dfs

    for archivo in archivos:
        try:
            df = pd.read_excel(archivo, engine="openpyxl")
            clave = archivo.stem
            dfs[clave] = df
            logger.info(
                f"  Cargado: {archivo.name}"
                f" → {len(df)} filas, {len(df.columns)} columnas"
            )
        except Exception as exc:
            logger.error(f"  Error al leer {archivo.name}: {exc}")

    return dfs


# ─── Transformaciones por reporte ────────────────────────────────────────────


def procesar_cuenta_corriente(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y enriquece el reporte de Cuenta Corriente."""
    df = df.copy()

    # TODO: ajustar nombres de columnas reales del archivo
    # Ejemplo: parsear fechas, normalizar importes, calcular saldo acumulado
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(
            df["Fecha"], dayfirst=True, errors="coerce"
        )

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip() if hasattr(df[col], "str") else df[col]

    logger.debug("Cuenta corriente procesada")
    return df


def procesar_gastos(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y enriquece el reporte de Gastos."""
    df = df.copy()

    # TODO: ajustar columnas reales
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(
            df["Fecha"], dayfirst=True, errors="coerce"
        )

    logger.debug("Gastos procesados")
    return df


def procesar_ordenes_pago(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y enriquece las Órdenes de Pago."""
    df = df.copy()

    # TODO: ajustar columnas reales
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(
            df["Fecha"], dayfirst=True, errors="coerce"
        )

    logger.debug("Órdenes de pago procesadas")
    return df


def procesar_listado_ordenes(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y enriquece el Listado Detallado de Órdenes de Pago."""
    df = df.copy()

    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(
            df["Fecha"], dayfirst=True, errors="coerce"
        )

    logger.debug("Listado detallado de órdenes procesado")
    return df


def procesar_obras(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y enriquece el reporte histórico de Obras."""
    df = df.copy()

    for col_fecha in ("Fecha", "FechaInicio", "FechaFin"):
        if col_fecha in df.columns:
            df[col_fecha] = pd.to_datetime(
                df[col_fecha], dayfirst=True, errors="coerce"
            )

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    logger.debug("Obras procesadas")
    return df


# ─── Orquestador ─────────────────────────────────────────────────────────────


def enriquecer_datos() -> dict[str, pd.DataFrame]:
    """
    Carga todos los archivos raw, aplica las transformaciones correspondientes
    y devuelve un dict con los DataFrames enriquecidos.
    """
    logger.info("Iniciando enriquecimiento de datos...")
    raw = cargar_archivos()
    resultado: dict[str, pd.DataFrame] = {}

    for clave, df in raw.items():
        clave_lower = clave.lower()
        if "cuenta corriente" in clave_lower:
            resultado["cuenta_corriente"] = procesar_cuenta_corriente(df)
        elif "gasto" in clave_lower:
            resultado["gastos"] = procesar_gastos(df)
        elif "listado" in clave_lower and (
            "orden" in clave_lower or "órdenes" in clave_lower
        ):
            resultado["listado_ordenes"] = procesar_listado_ordenes(df)
        elif "orden" in clave_lower or "órdenes" in clave_lower:
            resultado["ordenes_pago"] = procesar_ordenes_pago(df)
        elif "obra" in clave_lower:
            resultado["obras"] = procesar_obras(df)
        else:
            resultado[clave] = df
            logger.warning(
                f"  Reporte sin transformación específica: '{clave}'"
            )

    logger.success(f"Enriquecimiento completo: {list(resultado.keys())}")
    return resultado
