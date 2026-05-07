"""
src/etl/dim_loader.py
─────────────────────────────────────────────────────────────
Carga las dimensiones de Loockups.xlsx → PostgreSQL.

Fuente única: Loockups.xlsx (actualizado por GestionComp).
Destino: tablas dim_* en dw_grupopose_b52_dev / prod.

Orden (replicando flujo A2 — dims antes que datos):
  1. dim_obras_gerencias  ← hoja Obras_Gerencias (PK: obra_pronto)

Uso standalone:
  python -m src.etl.dim_loader
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from src.loader.loader import PostgresLoader

load_dotenv(dotenv_path="config/.env")

log = logging.getLogger(__name__)

_LOOCKUPS_DEFAULT = Path(
    os.getenv(
        "LOOCKUPS_PATH",
        "/home/richard/Dev/auditoria_ecosauron"
        "/sistema/Loockups.xlsx",
    )
)
_SCHEMA = "public"
_TABLA_OBRAS = "dim_obras_gerencias"
_PK_OBRAS = "obra_pronto"


def _leer_hoja(ruta: Path, hoja: str) -> pd.DataFrame:
    """Lee una hoja del Loockups y normaliza columnas a snake_case."""
    if not ruta.exists():
        raise FileNotFoundError(
            f"Loockups.xlsx no encontrado: {ruta}\n"
            "Verificar LOOCKUPS_PATH en config/.env"
        )
    df = pd.read_excel(ruta, sheet_name=hoja)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    # Eliminar columnas duplicadas post-normalización (primera ocurrencia gana)
    df = df.loc[:, ~df.columns.duplicated()]
    return df.dropna(how="all")


def cargar_obras_gerencias(
    loockups: Path | None = None,
) -> dict[str, Any]:
    """
    Lee hoja Obras_Gerencias y hace UPSERT en dim_obras_gerencias.
    Retorna resultado del upsert.
    """
    ruta = loockups or _LOOCKUPS_DEFAULT
    log.info("Cargando dim_obras_gerencias desde %s", ruta.name)

    df = _leer_hoja(ruta, "Obras_Gerencias")

    cols_esperadas = [
        "obra_pronto",
        "descripcion_obra",
        "compensable",
        "gerencia",
    ]
    cols_presentes = [c for c in cols_esperadas if c in df.columns]
    df = df[cols_presentes].copy()
    df["obra_pronto"] = df["obra_pronto"].astype(str).str.strip().str.zfill(8)

    loader = PostgresLoader()
    resultado: dict[str, Any] = loader.upsert_tabla(
        df,
        schema=_SCHEMA,
        tabla=_TABLA_OBRAS,
        constraint_cols=[_PK_OBRAS],
    )
    log.info(
        "dim_obras_gerencias: %d filas procesadas.",
        resultado.get("filas_afectadas", len(df)),
    )
    return resultado


def cargar_dims(loockups: Path | None = None) -> None:
    """
    Paso 0 del pipeline: carga todas las dims antes que los datos.
    Orden equivalente al flujo A2 (dims → datos).
    """
    log.info("=== PASO 0 — Carga de dimensiones ===")
    cargar_obras_gerencias(loockups)
    log.info("=== PASO 0 completado ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    cargar_dims()
