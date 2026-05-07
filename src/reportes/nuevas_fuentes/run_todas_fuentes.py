"""
run_todas_fuentes.py — Orquestador maestro del pipeline.

Flujo:
  1. clasificador_fuentes.clasificar() descubre qué archivos
     hay en input_raw/ y los mapea a FUENTE_DESTINO.
  2. Por cada grupo de fuentes con archivos, dispara el runner
     correspondiente una sola vez.
  3. Reporta staging generados y errores.

Regla de agrupacion runners:
  GG_FDL + FACTURACION_FDL → run_fdl.ejecutar()  (un solo runner)
  MENSUALES                 → run_mensuales.ejecutar()
  QUINCENAS                 → run_quincenas.ejecutar()

Uso:
    python -m projects.report_direccion.src.nuevas_fuentes.run_todas_fuentes
    python -m projects.report_direccion.src.nuevas_fuentes.run_todas_fuentes
        --periodo 2026
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import clasificador_fuentes as cf
from . import run_fdl, run_mensuales, run_quincenas
from ._constantes import LOOCKUPS_FILE

# ── Rutas base ───────────────────────────────────────────────────────────────
_REPORT_DIR = Path(__file__).parents[2]
_INPUT_RAW = _REPORT_DIR / "report_gerencias" / "input_raw"
_MAESTRO = Path(__file__).parents[4] / "config" / "maestro_fuentes.xlsx"

# Loockups: primero common/, fallback raíz
_LOOCKUPS = (
    _INPUT_RAW / "common" / LOOCKUPS_FILE
    if (_INPUT_RAW / "common" / LOOCKUPS_FILE).exists()
    else _INPUT_RAW / LOOCKUPS_FILE
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("run_todas_fuentes")

# ── Mapeo fuente → runner ────────────────────────────────────────────────────
# GG_FDL y FACTURACION_FDL comparten el mismo runner FDL.
# El runner los separa internamente por TIPO_EROGACION.
_FUENTES_FDL = {"GG_FDL", "FACTURACION_FDL"}


# ── Orquestador ──────────────────────────────────────────────────────────────


def ejecutar_todas(
    periodo: str | None = None,
    input_raw: Path = _INPUT_RAW,
    maestro: Path = _MAESTRO,
    loockups_path: Path | None = None,
) -> dict[str, Path | Exception]:
    """
    Clasifica archivos en input_raw/ y ejecuta los runners activos.

    Parameters
    ----------
    periodo : str | None
        Año a filtrar (ej. "2026"). None = todos.
    input_raw : Path
        Carpeta buzón único con los archivos Excel.
    maestro : Path
        Ruta a maestro_fuentes.xlsx.
    loockups_path : Path | None
        Ruta a Loockups.xlsx. None = resolución automática.

    Returns
    -------
    dict[str, Path | Exception]
        Por runner ejecutado: Path al staging generado o
        la excepción capturada si falló.
    """
    loockups = loockups_path or _resolver_loockups(input_raw)

    log.info("=== Orquestador maestro ===")
    log.info("  input_raw : %s", input_raw)
    log.info("  maestro   : %s", maestro)
    log.info("  loockups  : %s", loockups)

    mapa = cf.clasificar(input_raw=input_raw, maestro=maestro)

    if not mapa:
        log.warning("Sin archivos clasificados. Verificar input_raw/.")
        return {}

    resultados: dict[str, Path | Exception] = {}

    # ── FDL (GG_FDL y/o FACTURACION_FDL) ────────────────────────────────────
    if _FUENTES_FDL & set(mapa):
        log.info("--- Runner: FDL (GG_FDL + FACTURACION_FDL) ---")
        try:
            ruta = run_fdl.ejecutar(
                periodo=periodo,
                input_raw=input_raw,
                loockups_path=loockups,
            )
            resultados["FDL"] = ruta
            log.info("FDL OK → %s", ruta.name)
        except Exception as exc:
            resultados["FDL"] = exc
            log.error("FDL ERROR: %s", exc)

    # ── MENSUALES ────────────────────────────────────────────────────────────
    if "MENSUALES" in mapa:
        log.info("--- Runner: MENSUALES ---")
        try:
            ruta = run_mensuales.ejecutar(
                periodo=periodo,
                input_raw=input_raw,
                loockups_path=loockups,
            )
            resultados["MENSUALES"] = ruta
            log.info("MENSUALES OK → %s", ruta.name)
        except Exception as exc:
            resultados["MENSUALES"] = exc
            log.error("MENSUALES ERROR: %s", exc)

    # ── QUINCENAS ────────────────────────────────────────────────────────────
    if "QUINCENAS" in mapa:
        log.info("--- Runner: QUINCENAS ---")
        try:
            ruta = run_quincenas.ejecutar(
                periodo=periodo,
                input_raw=input_raw,
                loockups_path=loockups,
            )
            resultados["QUINCENAS"] = ruta
            log.info("QUINCENAS OK → %s", ruta.name)
        except Exception as exc:
            resultados["QUINCENAS"] = exc
            log.error("QUINCENAS ERROR: %s", exc)

    return resultados


def _resolver_loockups(input_raw: Path) -> Path:
    """Resuelve ruta de Loockups: common/ primero, raíz como fallback."""
    candidatos = [
        input_raw / "common" / LOOCKUPS_FILE,
        input_raw / LOOCKUPS_FILE,
    ]
    for c in candidatos:
        if c.exists():
            return c
    # Devuelve el primero para que el runner genere el error descriptivo
    return candidatos[0]


# ── CLI ──────────────────────────────────────────────────────────────────────


def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestador maestro — ejecuta todos los runners activos"
    )
    parser.add_argument(
        "--periodo",
        metavar="YYYY",
        help="Filtrar archivos por año (ej. 2026)",
    )
    args = parser.parse_args()

    resultados = ejecutar_todas(periodo=args.periodo)

    print("\n" + "=" * 50)
    print("RESUMEN")
    print("=" * 50)
    if not resultados:
        print("  Sin runners ejecutados.")
        return

    ok = {k: v for k, v in resultados.items() if isinstance(v, Path)}
    err = {k: v for k, v in resultados.items() if isinstance(v, Exception)}

    for runner, ruta in ok.items():
        print(f"  OK  {runner:<14} → {ruta.name}")
    for runner, exc in err.items():
        print(f"  ERR {runner:<14} → {exc}")

    if err:
        sys.exit(1)


if __name__ == "__main__":
    _main()
