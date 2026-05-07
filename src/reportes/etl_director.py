"""
etl_director.py — ETL de salida para el dashboard del Director Financiero.

Lee staging_despachos.csv (generado por el pipeline operativo),
aplica tipado y columnas derivadas, y exporta datos_director.parquet
en la carpeta output_director/.

Uso:
    python -m projects.report_direccion.src.etl_director
    python -m projects.report_direccion.src.etl_director --staging ruta.csv

El parquet resultante es la única fuente de datos de app_director.py.
No requiere conexión a la BD ni librerías de Excel.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Rutas por defecto (relativas a este módulo)
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).parent  # src/
_ROOT = _SRC_DIR.parents[3]  # richard_ia86_dev/
_GERENCIAS_DIR = _ROOT / "report_gerencias"
_STAGING_DEFAULT = _GERENCIAS_DIR / "staging_despachos.csv"
_STAGING_FDL = _GERENCIAS_DIR / "staging_fdl.csv"
_STAGING_MENSUALES = _GERENCIAS_DIR / "staging_mensuales.csv"
_OUTPUT_DIR = _GERENCIAS_DIR / "output_director"
_PARQUET_DEFAULT = _OUTPUT_DIR / "datos_director.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("etl_director")

# ---------------------------------------------------------------------------
# Meses en español (para columna derivada)
# ---------------------------------------------------------------------------
_MESES_ES: dict[int, str] = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}

# ---------------------------------------------------------------------------
# Mapeo: columnas BASE_TOTAL → nombres limpios para el dashboard
# ---------------------------------------------------------------------------
_RENAME: dict[str, str] = {
    "MES": "mes_nombre",
    "FECHA*": "fecha",
    "OBRA PRONTO*": "obra",
    "DESCRIPCION OBRA": "descripcion_obra",
    "GERENCIA": "gerencia",
    "DETALLE*": "detalle",
    "IMPORTE*": "importe_ars",
    "TIPO COMPROBANTE*": "tipo_comprobante",
    "N° COMPROBANTE*": "nro_comprobante",
    "OBSERVACION*": "observacion",
    "PROVEEDOR*": "proveedor",
    "RUBRO CONTABLE*": "rubro_contable",
    "CUENTA CONTABLE*": "cuenta_contable",
    "CODIGO CUENTA*": "codigo_cuenta",
    "FUENTE*": "fuente",
    "COMPENSABLE": "compensable",
    "TC": "tipo_cambio",
    "IMPORTE USD": "importe_usd",
}


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------
def generar_parquet(
    staging_path: Path = _STAGING_DEFAULT,
    output_path: Path = _PARQUET_DEFAULT,
    extra_stagings: list[Path] | None = None,
) -> Path:
    """
    Lee staging CSV, tipifica, agrega columnas derivadas y exporta parquet.

    Parameters
    ----------
    staging_path:
        Ruta al staging principal (despachos).
    output_path:
        Ruta destino del parquet.
    extra_stagings:
        Stagings adicionales a concatenar. Si es None (default) usa
        [_STAGING_FDL, _STAGING_MENSUALES]. Pasar [] para aislar en tests.

    Returns
    -------
    Path
        Ruta del parquet generado.

    Raises
    ------
    FileNotFoundError
        Si staging_path no existe.
    ValueError
        Si el staging está vacío o le faltan columnas obligatorias.
    """
    if not staging_path.exists():
        raise FileNotFoundError(
            f"staging no encontrado: {staging_path}\n"
            "Ejecutá primero el pipeline ETL para generarlo."
        )

    _extras = (
        [_STAGING_FDL, _STAGING_MENSUALES]
        if extra_stagings is None
        else extra_stagings
    )
    partes: list[pd.DataFrame] = []
    for ruta in (staging_path, *_extras):
        if not ruta.exists():
            log.info("staging no encontrado (omitido): %s", ruta)
            continue
        log.info("Leyendo staging: %s", ruta)
        partes.append(
            pd.read_csv(ruta, sep=";", encoding="utf-8-sig", low_memory=False)
        )

    df = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()

    if df.empty:
        raise ValueError("El staging está vacío — nada que exportar.")

    # Validar columnas mínimas
    cols_requeridas = {"FECHA*", "OBRA PRONTO*", "IMPORTE*", "GERENCIA"}
    faltantes = cols_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(f"Columnas faltantes en staging: {sorted(faltantes)}")

    log.info("Filas recibidas: %d", len(df))

    # Normalizar tipos mixtos post-concat
    df["OBRA PRONTO*"] = df["OBRA PRONTO*"].astype(str).str.strip()

    # --- Tipado -------------------------------------------------------
    df["FECHA*"] = pd.to_datetime(df["FECHA*"], errors="coerce")
    df["IMPORTE*"] = pd.to_numeric(df["IMPORTE*"], errors="coerce")
    df["IMPORTE USD"] = pd.to_numeric(df.get("IMPORTE USD"), errors="coerce")
    df["TC"] = pd.to_numeric(df.get("TC"), errors="coerce")

    # --- Columnas derivadas -------------------------------------------
    anio_s = df["FECHA*"].dt.year
    mes_s = df["FECHA*"].dt.month
    dia_sem_s = df["FECHA*"].dt.dayofweek + 1  # Lun=1 … Dom=7
    df["anio"] = anio_s.astype("Int16")
    df["mes"] = mes_s.astype("Int8")
    df["periodo"] = df["FECHA*"].dt.to_period("M").astype(str)
    df["mes_nombre_ord"] = mes_s  # para ordenar correctamente en gráficos

    # --- Columnas dim_calendario (convención Calendario.xlsm) ---------
    # Sector construcción: Lun-Sáb=1 laborable, Dom=0
    df["es_laborable"] = (dia_sem_s != 7).astype("int8")
    df["trimestre"] = df["FECHA*"].dt.quarter.astype("Int8")
    df["trimestre_label"] = "Q " + df["trimestre"].astype(str)

    _ABREV_DIA: dict[int, str] = {
        1: "lu",
        2: "ma",
        3: "mi",
        4: "ju",
        5: "vi",
        6: "sá",
        7: "do",
    }
    _ABREV_MES: dict[int, str] = {
        1: "ene",
        2: "feb",
        3: "mar",
        4: "abr",
        5: "may",
        6: "jun",
        7: "jul",
        8: "ago",
        9: "sep",
        10: "oct",
        11: "nov",
        12: "dic",
    }
    df["abrev_dia_es"] = dia_sem_s.map(_ABREV_DIA)
    df["abrev_mes_es"] = mes_s.map(_ABREV_MES)

    # --- Renombrar a nombres limpios ----------------------------------
    df = df.rename(columns=_RENAME)

    # --- Categorías (reduce tamaño parquet) --------------------------
    for col in (
        "gerencia",
        "fuente",
        "tipo_comprobante",
        "rubro_contable",
        "cuenta_contable",
    ):
        if col in df.columns:
            df[col] = df[col].astype("category")

    # --- Exportar -----------------------------------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)

    log.info(
        "Parquet generado: %s (%d filas, %d cols)",
        output_path,
        len(df),
        len(df.columns),
    )
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETL → parquet para dashboard del Director Financiero"
    )
    p.add_argument(
        "--staging",
        type=Path,
        default=_STAGING_DEFAULT,
        help="Ruta al staging_despachos.csv",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=_PARQUET_DEFAULT,
        help="Ruta de salida del parquet",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        ruta = generar_parquet(
            staging_path=args.staging,
            output_path=args.output,
        )
        print(f"OK — parquet en: {ruta}")
    except (FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        sys.exit(1)
