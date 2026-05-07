"""
run_mensuales.py — Punto de entrada del pipeline MENSUALES.

Descubre archivos MM-YYYY.xlsx en input_raw/, transforma con
nuevas_fuentes y persiste staging_mensuales.csv en report_gerencias/.

Uso:
    python -m projects.report_direccion.src.nuevas_fuentes.run_mensuales
    python -m projects.report_direccion.src.nuevas_fuentes.run_mensuales \\
        --periodo 2026
    python -m projects.report_direccion.src.nuevas_fuentes.run_mensuales \\
        --archivo 03-2026.xlsx
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from .transformer_mensuales import transformar_mensuales
from .writer_mensuales import escribir_staging_mensuales
from ._constantes import LOOCKUPS_FILE

# ── Rutas base ───────────────────────────────────────────────────────────────
# parents[4] = richard_ia86_dev/
_ROOT = Path(__file__).parents[4]
_INPUT_RAW = _ROOT / "report_gerencias" / "input_raw" / "mensuales"
_LOOCKUPS = _ROOT / "report_gerencias" / "data" / "lookups" / LOOCKUPS_FILE

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("run_mensuales")

_PATRON_MENSUALES = re.compile(r"^\d{2}-\d{4}\.xlsx$", re.IGNORECASE)


# ── Descubrimiento ───────────────────────────────────────────────────────────


def _descubrir_archivos(
    input_raw: Path,
    periodo: str | None = None,
    nombre: str | None = None,
) -> list[Path]:
    """
    Devuelve archivos MENSUALES disponibles en input_raw/.

    Parameters
    ----------
    input_raw : Path
        Carpeta donde buscar archivos MM-YYYY.xlsx.
    periodo : str | None
        Año a filtrar (ej. "2026"). None = todos.
    nombre : str | None
        Nombre de archivo específico (ej. "03-2026.xlsx").

    Raises
    ------
    FileNotFoundError
        Si input_raw no existe o el nombre específico no se encuentra.
    """
    if not input_raw.exists():
        raise FileNotFoundError(
            f"Carpeta input_raw no encontrada: {input_raw}"
        )
    if nombre:
        ruta = input_raw / nombre
        if not ruta.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
        return [ruta]

    archivos = [
        f for f in input_raw.iterdir() if _PATRON_MENSUALES.match(f.name)
    ]
    if periodo:
        archivos = [f for f in archivos if f"-{periodo}" in f.name]
    return sorted(archivos)


# ── Orquestador ──────────────────────────────────────────────────────────────


def ejecutar(
    periodo: str | None = None,
    nombre: str | None = None,
    input_raw: Path = _INPUT_RAW,
    loockups_path: Path = _LOOCKUPS,
) -> Path:
    """
    Orquesta el pipeline MENSUALES completo.

    1. Descubre archivos MM-YYYY.xlsx en input_raw/.
    2. Transforma y consolida el staging.
    3. Persiste staging_mensuales.csv.

    Returns
    -------
    Path
        Ruta al staging_mensuales.csv generado.
    """
    archivos = _descubrir_archivos(input_raw, periodo, nombre)
    if not archivos:
        filtro = f" (periodo={periodo})" if periodo else ""
        raise FileNotFoundError(
            f"No se encontraron archivos MENSUALES{filtro} en: {input_raw}"
        )

    if not loockups_path.exists():
        raise FileNotFoundError(f"Loockups no encontrado: {loockups_path}")

    log.info("Archivos MENSUALES a procesar: %d", len(archivos))
    for a in archivos:
        log.info("  -> %s", a.name)

    df = transformar_mensuales(archivos, loockups_path)
    log.info("Staging generado: %d filas", len(df))

    ruta = escribir_staging_mensuales(df)
    log.info("Staging guardado: %s", ruta)
    return ruta


# ── CLI ──────────────────────────────────────────────────────────────────────


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline MENSUALES — genera staging_mensuales.csv"
    )
    parser.add_argument(
        "--periodo",
        help="Filtrar por año (ej: 2026)",
    )
    parser.add_argument(
        "--archivo",
        help="Procesar un archivo específico (ej: 03-2026.xlsx)",
    )
    args = parser.parse_args()
    ejecutar(periodo=args.periodo, nombre=args.archivo)


if __name__ == "__main__":
    _cli()
