"""
escritor_csv.py — Escribe el output B52 en CSV.

Genera dos archivos en output/b52/:
  costos_b52_YYYYMMDD_HHMMSS.csv   → todos los registros con _estado_carga
  costos_b52_YYYYMMDD_HHMMSS_delta.csv → solo NUEVO + MODIFICADO

El delta es lo que viaja al staging de Hetzner para carga incremental.
El CSV completo es el dataset de pruebas sin PQ.

Separador: pipe (|) — coherente con los .dat del pipeline.
Encoding: utf-8-sig (BOM) para apertura limpia en Excel/Windows.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd

DIR_B52_DEFAULT = "output/b52"


class ArchivosB52(TypedDict):
    completo: str
    delta: str
    filas_total: int
    filas_delta: int


# Columnas de salida en orden canónico B52
# Las de auditoría van al final, _estado_carga es la primera de control
COLS_SALIDA: list[str] = [
    "ANIO",
    "MES",
    "OBRA_PRONTO",
    "DESCRIPCION_OBRA",
    "FECHA",
    "FUENTE",
    "TIPO_COMPROBANTE",
    "NRO_COMPROBANTE",
    "PROVEEDOR",
    "DETALLE",
    "CODIGO_CUENTA",
    "IMPORTE",
    "OBSERVACION",
    "RUBRO_CONTABLE",
    "CUENTA_CONTABLE",
    "COMPENSABLE",
    "_estado_carga",
    "_hash_fila",
    "_hash_importe",
]


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ordenar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Reordena columnas según COLS_SALIDA, tolera ausentes."""
    presentes = [c for c in COLS_SALIDA if c in df.columns]
    resto = [c for c in df.columns if c not in presentes]
    return df[presentes + resto]


def escribir_csv(
    df: pd.DataFrame,
    directorio: str = DIR_B52_DEFAULT,
    ts: str | None = None,
) -> ArchivosB52:
    """
    Escribe CSV completo + CSV delta en output/b52/.
    Retorna dict con rutas generadas:
      {'completo': ruta, 'delta': ruta, 'filas_delta': n}
    """
    os.makedirs(directorio, exist_ok=True)
    ts = ts or _timestamp()

    df_out = _ordenar_columnas(df)

    # ── Archivo completo ─────────────────────────────────────
    nombre_completo = f"costos_b52_{ts}.csv"
    ruta_completo = str(Path(directorio) / nombre_completo)
    df_out.to_csv(
        ruta_completo,
        index=False,
        sep="|",
        encoding="utf-8-sig",
    )

    # ── Delta (solo carga incremental) ───────────────────────
    if "_estado_carga" in df_out.columns:
        df_delta = df_out[
            df_out["_estado_carga"].isin(["NUEVO", "MODIFICADO"])
        ]
    else:
        df_delta = df_out.copy()

    nombre_delta = f"costos_b52_{ts}_delta.csv"
    ruta_delta = str(Path(directorio) / nombre_delta)
    df_delta.to_csv(
        ruta_delta,
        index=False,
        sep="|",
        encoding="utf-8-sig",
    )

    return {
        "completo": ruta_completo,
        "delta": ruta_delta,
        "filas_total": len(df_out),
        "filas_delta": len(df_delta),
    }
