"""
lector.py — Lee BaseCostosPOSE.xlsx y normaliza columnas.

Fuente: output/director/BaseCostosPOSE.xlsx
  → DataFrame con schema canónico A2 (13 cols de negocio)
  → Columnas ANIO (int) y MES (int) extraídas de FECHA

No modifica el archivo fuente.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
FUENTE_DEFAULT = str(
    _PROJECT_ROOT / "output" / "director" / "BaseCostosPOSE.xlsx"
)

# Columnas de negocio que deben existir en el Excel
COLS_NEGOCIO: list[str] = [
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
]


def leer_base_costos(
    ruta: str = FUENTE_DEFAULT,
) -> pd.DataFrame:
    """
    Lee BaseCostosPOSE.xlsx.
    Retorna DataFrame con columnas de negocio + ANIO + MES.
    Lanza ValueError si faltan columnas críticas.
    """
    p = Path(ruta)
    if not p.exists():
        raise FileNotFoundError(
            f"Archivo no encontrado: {ruta}\n"
            "Ejecutar primero ETL_A2 (Paso2) para generarlo."
        )

    df = pd.read_excel(p, engine="openpyxl")

    # Normalizar nombres a mayúsculas para matching robusto
    df.columns = [c.strip().upper() for c in df.columns]

    # Verificar columnas críticas
    ausentes = [c for c in ["FECHA", "IMPORTE"] if c not in df.columns]
    if ausentes:
        raise ValueError(f"Columnas críticas ausentes en {p.name}: {ausentes}")

    # Extraer ANIO y MES
    df["_FECHA_DT"] = pd.to_datetime(
        df["FECHA"], errors="coerce", dayfirst=True
    )
    df["ANIO"] = df["_FECHA_DT"].dt.year.astype("Int64")
    df["MES"] = df["_FECHA_DT"].dt.month.astype("Int64")

    sin_fecha = df["_FECHA_DT"].isna().sum()
    if sin_fecha > 0:
        print(
            f"  [AVISO] {sin_fecha} filas con FECHA no parseable"
            " — se incluyen con ANIO/MES nulos."
        )

    df = df.drop(columns=["_FECHA_DT"])

    # Retener solo columnas conocidas + ANIO + MES
    # (tolera columnas extra en el Excel sin romper)
    cols_presentes = [c for c in COLS_NEGOCIO if c in df.columns]
    extra = ["ANIO", "MES"]
    return df[cols_presentes + extra].copy()


def resumen_lectura(df: pd.DataFrame) -> dict[str, object]:
    """
    Resumen rápido del DataFrame leído.
    """
    total_filas = len(df)
    anio_min: object = int(df["ANIO"].min()) if "ANIO" in df.columns else None
    anio_max: object = int(df["ANIO"].max()) if "ANIO" in df.columns else None
    importe_total: object = (
        float(df["IMPORTE"].sum()) if "IMPORTE" in df.columns else None
    )
    return {
        "filas": total_filas,
        "anio_min": anio_min,
        "anio_max": anio_max,
        "importe_total": importe_total,
    }
