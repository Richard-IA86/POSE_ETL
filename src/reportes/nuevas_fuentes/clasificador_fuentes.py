"""
clasificador_fuentes.py — Pre-ingesta: descubrimiento de archivos.

Lee config/maestro_fuentes.xlsx (solo filas ACTIVO == "SI") y
escanea input_raw/ con el patron de cada fuente para devolver
un mapa fuente → archivos encontrados.

Responsabilidad ÚNICA: resolver qué archivo físico alimenta
cada FUENTE_DESTINO. No lee columnas, no filtra datos, no transforma.

Uso programático
----------------
    from src.reportes.nuevas_fuentes import (
        clasificador_fuentes as cf,
    )
    mapa = cf.clasificar(input_raw=Path("..."), maestro=Path("..."))
    # {"GG_FDL": [Path("01-2026.xlsx")], "MENSUALES": [...], ...}

Uso CLI
-------
    python -m projects.report_direccion.src.nuevas_fuentes.\\
        clasificador_fuentes
"""

from __future__ import annotations

import fnmatch
import logging
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Rutas por defecto ────────────────────────────────────────────────────────
_HERE = Path(__file__).parents[4]  # raiz richard_ia86_dev
_MAESTRO_DEFAULT = _HERE / "config" / "maestro_fuentes.xlsx"
_INPUT_RAW_DEFAULT = (
    Path(__file__).parents[2] / "report_gerencias" / "input_raw"
)

# Token regex: patron es una expresion regular si empieza con \d o \w etc.
_RE_TOKEN = re.compile(r"[\\^$(){}|+?]")


# ── Lectura del maestro ──────────────────────────────────────────────────────


def leer_maestro(maestro: Path) -> list[dict[str, Any]]:
    """
    Lee Config_Fuentes del maestro y devuelve solo filas ACTIVO=SI.

    Returns
    -------
    list[dict]
        Cada dict: {fuente, patron, hojas, notas}
    """
    df = pd.read_excel(maestro, sheet_name="Config_Fuentes", dtype=str)
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[df["ACTIVO"].str.strip().str.upper() == "SI"]
    df = df.dropna(subset=["PATRON_ARCHIVO"])

    fuentes: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        fuentes.append(
            {
                "fuente": str(row["FUENTE_DESTINO"]).strip(),
                "patron": str(row["PATRON_ARCHIVO"]).strip(),
                "hojas": str(row.get("HOJAS_A_LEER", "")).strip(),
                "notas": str(row.get("NOTAS", "")).strip(),
            }
        )
    return fuentes


# ── Matching de archivos ─────────────────────────────────────────────────────


def _es_regex(patron: str) -> bool:
    """True si el patron contiene metacaracteres de regex."""
    return bool(_RE_TOKEN.search(patron))


def _matchea(nombre: str, patron: str) -> bool:
    """
    Comprueba si 'nombre' (solo filename) satisface 'patron'.

    Estrategia:
    - Si patron contiene metacaracteres regex → re.match (case-insensitive)
    - Si no → fnmatch glob (*, ?) (case-insensitive)
    """
    nombre_u = nombre.upper()
    if _es_regex(patron):
        return bool(re.match(patron, nombre, re.IGNORECASE))
    return fnmatch.fnmatch(nombre_u, patron.upper())


# ── API principal ────────────────────────────────────────────────────────────


def clasificar(
    input_raw: Path | None = None,
    maestro: Path | None = None,
) -> dict[str, list[Path]]:
    """
    Escanea input_raw/ y devuelve mapa fuente → archivos.

    Un mismo archivo puede aparecer en múltiples fuentes si
    su nombre satisface varios patrones (ej: 01-2026.xlsx →
    GG_FDL, FACTURACION_FDL y MENSUALES).

    Parameters
    ----------
    input_raw : Path | None
        Carpeta donde buscar archivos. Default: _INPUT_RAW_DEFAULT.
    maestro : Path | None
        Ruta al maestro_fuentes.xlsx. Default: _MAESTRO_DEFAULT.

    Returns
    -------
    dict[str, list[Path]]
        Fuentes con al menos un archivo encontrado.
        Fuentes sin match NO aparecen en el dict.
    """
    raw = input_raw or _INPUT_RAW_DEFAULT
    mst = maestro or _MAESTRO_DEFAULT

    if not raw.exists():
        raise FileNotFoundError(f"input_raw no encontrado: {raw}")
    if not mst.exists():
        raise FileNotFoundError(f"maestro no encontrado: {mst}")

    fuentes = leer_maestro(mst)
    archivos = [
        f for f in raw.iterdir() if f.is_file() and f.suffix.lower() == ".xlsx"
    ]

    logger.info(
        "Clasificador: %d archivos en input_raw, "
        "%d fuentes activas en maestro",
        len(archivos),
        len(fuentes),
    )

    mapa: dict[str, list[Path]] = {}
    for cfg in fuentes:
        matches = [a for a in archivos if _matchea(a.name, cfg["patron"])]
        if matches:
            mapa[cfg["fuente"]] = matches
            logger.info(
                "  %-20s → %d archivo(s): %s",
                cfg["fuente"],
                len(matches),
                [a.name for a in matches],
            )
        else:
            logger.warning(
                "  %-20s → sin archivos (patron: %s)",
                cfg["fuente"],
                cfg["patron"],
            )

    return mapa


# ── CLI ──────────────────────────────────────────────────────────────────────


def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    mapa = clasificar()
    if not mapa:
        print("Sin archivos clasificados en input_raw/")
        return
    print("\nResultado clasificacion:")
    for fuente, archivos in mapa.items():
        for a in archivos:
            print(f"  {fuente:<22} ← {a.name}")


if __name__ == "__main__":
    _main()
