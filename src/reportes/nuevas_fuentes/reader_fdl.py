"""
reader_fdl.py — Lectores crudos para fuentes FDL mensuales.

Los archivos siguen la convención 'MM-YYYY.xlsx' (ej: 01-2026.xlsx).
Cada archivo contiene una tabla nombrada 'gg_fdl' con filas de
ambas fuentes, distinguidas por la columna TIPO DE EROGACION:
  "OBRA"         → GG FDL
  "VENTA DEPTO"  → FACTURACION FDL
  "VENTA LOTE"   → FACTURACION FDL
  "VENTA PRODUCTO" → FACTURACION FDL

Referencia: PowerQuery ggfdf.pq (fuente autoritativa).
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from ._constantes import TIPOS_EROGACION_FACTURACION, TIPOS_EROGACION_GG


def parse_nombre_archivo(path: Path) -> tuple[int, int]:
    """
    Extrae (mes, año) del nombre 'MM-YYYY.xlsx'.

    Raises
    ------
    ValueError
        Si el nombre no coincide con el formato esperado.
    """
    m = re.match(r"^(\d{2})-(\d{4})\.xlsx$", path.name, re.IGNORECASE)
    if not m:
        raise ValueError(
            f"Nombre inesperado: '{path.name}'. "
            "Formato esperado: MM-YYYY.xlsx"
        )
    return int(m.group(1)), int(m.group(2))


def leer_tabla_gg_fdl(path: Path) -> pd.DataFrame:
    """
    Lee la tabla del archivo mensual MM-YYYY.xlsx.

    Columnas devueltas:
      TIPO_EROGACION, FECHA, OBRA_PRONTO, CENTRO_COSTO,
      SALIDA3, ENT2

    Reglas OBRA_PRONTO vacío:
      - Filas no-OBRA (VENTA*): ENT vacío → RETIRO; SALIDA vacía → APORTE.
      - Filas OBRA con CENTRO_COSTO relleno: se conservan tal cual;
        el transformer resuelve el código via GG_FDL_CentroCosto.
      - Filas OBRA sin CENTRO_COSTO: se descartan (no hay forma
        de resolver la obra).
    """
    mes, _ = parse_nombre_archivo(path)
    df_raw = pd.read_excel(path, sheet_name=str(mes), header=0)
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    df = df_raw.dropna(how="all").reset_index(drop=True)

    df = df.rename(
        columns={
            "TIPO DE EROGACION": "TIPO_EROGACION",
            "NUMERO OBRA": "OBRA_PRONTO",
            "ENT": "ENT2",
            "SALIDA": "SALIDA3",
            "CENTRO DE COSTO": "CENTRO_COSTO",
        }
    )

    tipos_validos = TIPOS_EROGACION_GG + TIPOS_EROGACION_FACTURACION
    df = df[df["TIPO_EROGACION"].isin(tipos_validos)].reset_index(drop=True)

    # CENTRO_COSTO: puede no existir en archivos anteriores
    if "CENTRO_COSTO" not in df.columns:
        df["CENTRO_COSTO"] = ""
    df["CENTRO_COSTO"] = (
        df["CENTRO_COSTO"].fillna("").astype(str).str.strip().str.upper()
    )

    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")

    # Convertir sin fillna para poder detectar blancos originales
    df["SALIDA3"] = pd.to_numeric(df["SALIDA3"], errors="coerce")
    if "ENT2" not in df.columns:
        df["ENT2"] = float("nan")
    df["ENT2"] = pd.to_numeric(df["ENT2"], errors="coerce")

    # Capturar blancos ANTES del fillna
    _salida_blanco = df["SALIDA3"].isna()
    _ent_blanco = df["ENT2"].isna()

    df["OBRA_PRONTO"] = (
        pd.to_numeric(df["OBRA_PRONTO"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .str.replace("<NA>", "", regex=False)
        .str.strip()
    )

    # Regla de negocio: OBRA_PRONTO vacío:
    #   - Filas no-OBRA: ENT vacío → RETIRO; SALIDA vacía → APORTE.
    #   - Filas OBRA con CENTRO_COSTO: conservar (transformer resuelve).
    _obra_blanco = df["OBRA_PRONTO"] == ""
    _es_obra = df["TIPO_EROGACION"].isin(TIPOS_EROGACION_GG)
    df.loc[_obra_blanco & _ent_blanco & ~_es_obra, "OBRA_PRONTO"] = "RETIRO"
    df.loc[_obra_blanco & _salida_blanco & ~_es_obra, "OBRA_PRONTO"] = "APORTE"

    df["SALIDA3"] = df["SALIDA3"].fillna(0.0)
    df["ENT2"] = df["ENT2"].fillna(0.0)

    # Descartar filas irresolvibles:
    #   - no-OBRA con obra vacía (RETIRO/APORTE no asignado)
    #   - OBRA con obra vacía Y sin CENTRO_COSTO
    _obra_blanco_final = df["OBRA_PRONTO"] == ""
    _conservar_obra_cc = _es_obra & (df["CENTRO_COSTO"] != "")
    df = df[~_obra_blanco_final | _conservar_obra_cc].reset_index(drop=True)

    return df[
        [
            "TIPO_EROGACION",
            "FECHA",
            "OBRA_PRONTO",
            "CENTRO_COSTO",
            "SALIDA3",
            "ENT2",
        ]
    ]
