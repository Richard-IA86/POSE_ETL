"""
bifurcador.py — Orquestador del Bifurcador B52.

Pipeline completo:
  1. Leer  BaseCostosPOSE.xlsx (output/director/)
  2. Calcular hashes SHA-256 por fila
  3. Comparar vs corrida anterior (_hashes_anterior.csv)
  4. Clasificar: NUEVO | SIN_CAMBIO | MODIFICADO
  5. Escribir CSV completo + CSV delta (output/b52/)
  6. Persistir hashes para próxima corrida
  7. Imprimir resumen de corrida

Uso:
  python -m ETL_BaseA2.src.bifurcador.bifurcador
  python -m ETL_BaseA2.src.bifurcador.bifurcador --xlsx otra/ruta.xlsx
  python -m ETL_BaseA2.src.bifurcador.bifurcador --dry-run

Exit code:
  0 — OK (incluyendo primera corrida o sin cambios)
  1 — Error de lectura o schema inválido
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from ETL_BaseA2.src.bifurcador.escritor_csv import ArchivosB52, escribir_csv
from ETL_BaseA2.src.bifurcador.hasher import (
    calcular_hashes,
    cargar_hashes_anterior,
    clasificar_estado,
    guardar_hashes,
    resumen_estados,
)
from ETL_BaseA2.src.bifurcador.lector import (
    leer_base_costos,
    resumen_lectura,
)

SEP = "=" * 72
FUENTE_DEFAULT = "output/director/BaseCostosPOSE.xlsx"
HASH_FILE = "output/b52/_hashes_anterior.csv"


def parsear_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bifurcador B52 — BaseCostosPOSE → output/b52/"
    )
    p.add_argument(
        "--xlsx",
        default=FUENTE_DEFAULT,
        help=f"Ruta al Excel fuente. Default: {FUENTE_DEFAULT}",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Muestra resumen sin escribir archivos" " ni actualizar hashes."
        ),
    )
    return p.parse_args()


def fmt_imp(v: float) -> str:
    """Importe formato ES: punto miles, coma decimal."""
    us = format(v, "16,.2f")
    return us.replace(".", "#").replace(",", ".").replace("#", ",")


def imprimir_resumen(
    resumen_lect: dict[str, object],
    estados: dict[str, int],
    archivos: ArchivosB52 | None,
    dry_run: bool,
    inicio: datetime,
) -> None:
    duracion = (datetime.now() - inicio).total_seconds()

    print(SEP)
    print("  BIFURCADOR B52 — RESUMEN DE CORRIDA")
    print(SEP)

    filas = resumen_lect.get("filas", 0)
    imp = resumen_lect.get("importe_total") or 0.0
    imp_f: float = float(imp) if isinstance(imp, (int, float)) else 0.0
    anio_min = resumen_lect.get("anio_min", "?")
    anio_max = resumen_lect.get("anio_max", "?")

    print("  Fuente          : BaseCostosPOSE.xlsx")
    print(f"  Filas leídas    : {filas:,}")
    print(f"  Importe total   : {fmt_imp(imp_f)}")
    print(f"  Rango años      : {anio_min} — {anio_max}")
    print()
    print("  CONTROL INCREMENTAL:")
    print(f"    NUEVO         : {estados.get('NUEVO', 0):,}")
    print(f"    MODIFICADO    : {estados.get('MODIFICADO', 0):,}")
    print(f"    SIN_CAMBIO    : {estados.get('SIN_CAMBIO', 0):,}")

    if not dry_run and archivos is not None:
        print()
        print("  ARCHIVOS GENERADOS:")
        print(f"    Completo      : {archivos['completo']}")
        print(f"    Delta         : {archivos['delta']}")
        print(f"    Filas delta   : {archivos['filas_delta']:,}")
    else:
        print()
        print("  [DRY-RUN] Sin escritura de archivos.")

    print(f"\n  Duración: {duracion:.1f}s")
    print(SEP)


def run(
    xlsx: str = FUENTE_DEFAULT,
    dry_run: bool = False,
) -> dict[str, object]:
    """
    Punto de entrada reutilizable (también llamable desde tests).
    Retorna resumen de corrida como dict.
    """
    inicio = datetime.now()

    # 1. Leer
    df = leer_base_costos(xlsx)
    resumen_lect = resumen_lectura(df)

    # 2. Calcular hashes
    df = calcular_hashes(df)

    # 3. Cargar hashes anteriores y clasificar
    hashes_ant = cargar_hashes_anterior(HASH_FILE)
    primera_corrida = len(hashes_ant) == 0
    df = clasificar_estado(df, hashes_ant)

    estados = resumen_estados(df)

    # 4. Escribir archivos
    archivos: ArchivosB52 | None = None
    if not dry_run:
        archivos = escribir_csv(df)
        guardar_hashes(df, HASH_FILE)

    imprimir_resumen(resumen_lect, estados, archivos, dry_run, inicio)

    if primera_corrida:
        print(
            "  [INFO] Primera corrida — todos los registros"
            " marcados como NUEVO."
        )

    return {
        "lectura": resumen_lect,
        "estados": estados,
        "archivos": archivos,  # ArchivosB52 | None
        "primera_corrida": primera_corrida,
    }


def main() -> None:
    args = parsear_argumentos()
    try:
        run(xlsx=args.xlsx, dry_run=args.dry_run)
    except FileNotFoundError as exc:
        print(f"\n[ERROR] {exc}")
        sys.exit(1)
    except ValueError as exc:
        print(f"\n[ERROR] Schema inválido: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
