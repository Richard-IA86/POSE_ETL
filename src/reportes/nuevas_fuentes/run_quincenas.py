"""
run_quincenas.py — Punto de entrada del pipeline QUINCENAS.

Descubre archivos QUINCENAS MM-YYYY.xlsx en input_raw/,
transforma con nuevas_fuentes y persiste staging_quincenas.csv en
report_gerencias/.

Uso:
    python -m projects.report_direccion.src.nuevas_fuentes.run_quincenas
    python -m projects.report_direccion.src.nuevas_fuentes.run_quincenas \
        --periodo 2026
    python -m projects.report_direccion.src.nuevas_fuentes.run_quincenas \
        --archivo "QUINCENAS 03-2026.xlsx"
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd

from ._constantes import LOOCKUPS_FILE
from .reader_quincenas import leer_hojas_quincenas
from .transformer_quincenas import transformar_quincenas
from .writer_quincenas import escribir_staging_quincenas

# parents[0]=nuevas_fuentes/ parents[1]=src/ parents[2]=report_direccion/
# parents[3]=projects/ parents[4]=richard_ia86_dev/
_ROOT = Path(__file__).parents[4]
_INPUT_RAW_ROOT = _ROOT / "report_gerencias" / "input_raw"
_INPUT_RAW = _INPUT_RAW_ROOT / "quincenas"
_LOOCKUPS = _ROOT / "report_gerencias" / "data" / "lookups" / LOOCKUPS_FILE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_PATRON_QUINCENAS = re.compile(
    r"^QUINCENAS\s+\d{2}-\d{4}\.xlsx$",
    re.IGNORECASE,
)


def _descubrir_archivos(
    input_raw: Path,
    periodo: str | None = None,
    nombre: str | None = None,
) -> list[Path]:
    """Descubre archivos QUINCENAS en input_raw/."""
    if not input_raw.exists():
        raise FileNotFoundError(
            f"Carpeta input_raw no encontrada: {input_raw}"
        )

    carpetas = [input_raw]

    if nombre:
        ruta_encontrada: Path | None = None
        for carpeta in carpetas:
            ruta = carpeta / nombre
            if ruta.exists():
                ruta_encontrada = ruta
                break

        if ruta_encontrada is None:
            raise FileNotFoundError(f"Archivo no encontrado: {nombre}")

        if not _PATRON_QUINCENAS.match(ruta_encontrada.name):
            raise ValueError(
                "Nombre de archivo inválido para QUINCENAS. "
                "Formato esperado: 'QUINCENAS MM-YYYY.xlsx'."
            )
        return [ruta_encontrada]

    encontrados: dict[str, Path] = {}
    for carpeta in carpetas:
        if not carpeta.exists():
            continue
        for f in carpeta.iterdir():
            if _PATRON_QUINCENAS.match(f.name) and f.name not in encontrados:
                encontrados[f.name] = f

    archivos = list(encontrados.values())
    if periodo:
        archivos = [f for f in archivos if f"-{periodo}" in f.name]
    return sorted(archivos)


def procesar_lote_quincenas(
    archivos_crudos: list[Path],
    lookups_path: Path,
) -> pd.DataFrame:
    """Invoca reader -> transformer."""
    if not archivos_crudos:
        log.info("QUINCENAS: No hay archivos para procesar.")
        return pd.DataFrame()

    dfs_transformados: list[pd.DataFrame] = []

    for archivo in archivos_crudos:
        log.info("QUINCENAS: Procesando archivo %s...", archivo.name)
        df_crudo, hojas_info = leer_hojas_quincenas(archivo)

        if df_crudo.empty:
            log.warning(
                "  -> Archivo vacío o estructura irreconocible "
                "en %s. Se ignora.",
                archivo.name,
            )
            continue

        try:
            log.info("  -> Aplicando transformaciones (%s)", hojas_info)
            df_transformado = transformar_quincenas(
                df_crudo, lookups_path, archivo.name
            )
            dfs_transformados.append(df_transformado)
            log.info("  -> OK. Transformadas %d filas.", len(df_transformado))
        except ValueError as err:
            log.error("  -> ABORTADO %s: %s", archivo.name, err)
            raise

    if not dfs_transformados:
        log.warning(
            "QUINCENAS: Se procesaron archivos pero todos resultaron "
            "en DataFrames vacíos o ignorados."
        )
        return pd.DataFrame()

    df_final = pd.concat(dfs_transformados, ignore_index=True)
    log.info(
        "QUINCENAS: Concatenación final resultante " "(%d registros totales).",
        len(df_final),
    )

    return df_final


def ejecutar(
    periodo: str | None = None,
    nombre: str | None = None,
    input_raw: Path = _INPUT_RAW,
    loockups_path: Path = _LOOCKUPS,
) -> Path:
    """
    Orquesta el pipeline QUINCENAS completo.

    1. Descubre archivos MM-YYYY.xlsx en input_raw/.
    2. Transforma y consolida el staging.
    3. Persiste staging_quincenas.csv.

    Returns
    -------
    Path
        Ruta al staging_quincenas.csv generado.
    """
    archivos = _descubrir_archivos(input_raw, periodo, nombre)
    if not archivos:
        filtro = f" (periodo={periodo})" if periodo else ""
        raise FileNotFoundError(
            f"No se encontraron archivos QUINCENAS{filtro} en: " f"{input_raw}"
        )

    if not loockups_path.exists():
        raise FileNotFoundError(f"Loockups no encontrado: {loockups_path}")

    log.info("Archivos QUINCENAS a procesar: %d", len(archivos))

    df_consolidado = procesar_lote_quincenas(archivos, loockups_path)
    if df_consolidado.empty:
        raise ValueError("No se generó data de staging válida para QUINCENAS.")

    log.info("Staging generado: %d filas", len(df_consolidado))

    ruta = escribir_staging_quincenas(df_consolidado)
    log.info("Staging guardado: %s", ruta)
    return ruta


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description=("Pipeline QUINCENAS — genera staging_quincenas.csv")
    )
    parser.add_argument(
        "--periodo",
        help="Filtrar por año (ej: 2026)",
    )
    parser.add_argument(
        "--archivo",
        help=(
            "Procesar un archivo específico " "(ej: 'QUINCENAS 03-2026.xlsx')"
        ),
    )
    args = parser.parse_args()
    try:
        ejecutar(periodo=args.periodo, nombre=args.archivo)
    except Exception as e:
        log.error("Fallo ejecución QUINCENAS: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    _cli()
