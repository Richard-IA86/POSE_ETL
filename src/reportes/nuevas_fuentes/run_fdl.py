"""
run_fdl.py — Punto de entrada del pipeline FDL.

Descubre archivos MM-YYYY.xlsx en input_raw/, transforma con
nuevas_fuentes y persiste staging_fdl.csv en report_gerencias/.

Uso:
    python -m projects.report_direccion.src.nuevas_fuentes.run_fdl
    python -m projects.report_direccion.src.nuevas_fuentes.run_fdl \\
        --periodo 2026
    python -m projects.report_direccion.src.nuevas_fuentes.run_fdl \\
        --archivo 03-2026.xlsx
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

from . import escribir_staging_fdl, transformar_fdl
from ._constantes import LOOCKUPS_FILE

# ── Rutas base ───────────────────────────────────────────────────────────────
# parents[0] = nuevas_fuentes/
# parents[1] = src/
# parents[2] = report_direccion/
# parents[3] = projects/
# parents[4] = richard_ia86_dev/
_ROOT = Path(__file__).parents[4]
_INPUT_RAW = _ROOT / "report_gerencias" / "input_raw" / "fdl"
_LOOCKUPS = _ROOT / "report_gerencias" / "data" / "lookups" / LOOCKUPS_FILE

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("run_fdl")

# Patrón de nombres de archivo FDL: MM-YYYY.xlsx
_PATRON_FDL = re.compile(r"^\d{2}-\d{4}\.xlsx$", re.IGNORECASE)


# ── Descubrimiento ───────────────────────────────────────────────────────────


def _descubrir_archivos(
    input_raw: Path,
    periodo: str | None = None,
    nombre: str | None = None,
) -> list[Path]:
    """
    Devuelve archivos FDL disponibles en input_raw/.

    Parameters
    ----------
    input_raw : Path
        Carpeta donde buscar archivos MM-YYYY.xlsx.
    periodo : str | None
        Año a filtrar (ej. "2026"). None = todos los años.
    nombre : str | None
        Nombre de archivo específico (ej. "03-2026.xlsx").

    Returns
    -------
    list[Path]
        Lista ordenada de archivos encontrados.

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

    archivos = [f for f in input_raw.iterdir() if _PATRON_FDL.match(f.name)]
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
    Orquesta el pipeline FDL completo.

    1. Descubre archivos MM-YYYY.xlsx en input_raw/.
    2. Llama a transformar_fdl para consolidar el staging.
    3. Persiste staging_fdl.csv vía escribir_staging_fdl.

    Parameters
    ----------
    periodo : str | None
        Filtrar por año (ej. "2026"). None = todos.
    nombre : str | None
        Procesar un archivo específico (ej. "03-2026.xlsx").
    input_raw : Path
        Carpeta fuente. Inyectable para tests.
    loockups_path : Path
        Ruta a Loockups.xlsx. Inyectable para tests.

    Returns
    -------
    Path
        Ruta al staging_fdl.csv generado.

    Raises
    ------
    FileNotFoundError
        Si no hay archivos FDL o falta Loockups.xlsx.
    ValueError
        Si el staging resultante está vacío.
    """
    archivos = _descubrir_archivos(input_raw, periodo=periodo, nombre=nombre)
    if not archivos:
        filtro = f" (periodo={periodo})" if periodo else ""
        raise FileNotFoundError(
            f"No se encontraron archivos FDL{filtro} en: {input_raw}"
        )

    if not loockups_path.exists():
        raise FileNotFoundError(f"Loockups no encontrado: {loockups_path}")

    log.info("Archivos FDL a procesar: %d", len(archivos))
    for f in archivos:
        log.info("  -> %s", f.name)

    df_staging = transformar_fdl(archivos, loockups_path)
    log.info("Staging generado: %d filas", len(df_staging))

    ruta = escribir_staging_fdl(df_staging)
    log.info("Staging guardado: %s", ruta)
    return ruta


# ── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline FDL — FACTURACION + GG FDL"
    )
    p.add_argument(
        "--periodo",
        help="Filtrar por año (ej: 2026). Default: todos.",
        default=None,
    )
    p.add_argument(
        "--archivo",
        help="Nombre de archivo específico (ej: 03-2026.xlsx).",
        default=None,
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        ruta = ejecutar(periodo=args.periodo, nombre=args.archivo)
        print(f"[OK] {ruta}")
        sys.exit(0)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
