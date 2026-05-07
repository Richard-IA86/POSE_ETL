"""lanzar_demo.py — Lanzador interactivo GRUPO POSE.

Reemplaza demo_presentacion.bat eliminando los quirks del
parser CMD (echo. en bloques if, rutas con .., etc.).

Fuentes disponibles:
  [1] DESPACHOS          — CUENTA POSE MM-YYYY.xlsx
  [2] GG FDL             — MM-YYYY.xlsx
  [3] MENSUALES          — MM-YYYY.xlsx
  [4] QUINCENAS          — QUINCENAS MM-YYYY.xlsx
  [5] TODAS LAS FUENTES  — clasificador maestro buzon unico
  [6] Dashboard          — solo abrir dashboard
  [0] Salir

Uso:
  python projects/report_direccion/lanzar_demo.py
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

# ── Rutas base ────────────────────────────────────────────────
# lanzar_demo.py vive en projects/report_direccion/
_HERE = Path(__file__).resolve().parent
REPO_ROOT = _HERE.parents[1]

INPUT_RAW = _HERE / "report_gerencias" / "input_raw"
STAGING = _HERE / "report_gerencias" / "staging_despachos.csv"
PARQUET = (
    _HERE / "report_gerencias" / "output_director" / "datos_director.parquet"
)
APP = _HERE / "src" / "dashboard" / "app_director.py"

_PY = sys.executable
_SEP = "=" * 60
_SEP2 = "-" * 40
_RE_FECHA = re.compile(r"(\d{2})-(\d{4})")


# ── Utilidades ────────────────────────────────────────────────
def _p(msg: str = "") -> None:
    print(msg)


def _cls() -> None:
    _p("\n" + _SEP)


def _cmd(args: list[str]) -> bool:
    """Ejecuta subproceso (silenciado). True = exito."""
    result = subprocess.run(
        args,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        err = result.stderr if isinstance(result.stderr, str) else ""
        lineas = [ln for ln in err.strip().splitlines() if ln.strip()]
        if lineas:
            _p(f"  {lineas[-1][:70]}")
    return result.returncode == 0


def _verificar_lookups() -> bool:
    lookups = INPUT_RAW / "Loockups.xlsx"
    if not lookups.exists():
        _p("  [ERROR] Falta archivo de configuracion:")
        _p(f"          {lookups}")
        return False
    return True


def _listar_archivos(patron: str) -> list[Path]:
    return sorted(INPUT_RAW.glob(patron))


def _seleccionar_archivo(archivos: list[Path]) -> Path | None:
    for i, f in enumerate(archivos, 1):
        _p(f"  [{i}]  {f.name}")
    _p()
    _p("  [0]  Volver")
    _p()
    sel = input("  Opcion: ").strip()
    if sel == "0":
        return None
    try:
        idx = int(sel) - 1
        if 0 <= idx < len(archivos):
            return archivos[idx]
    except ValueError:
        pass
    return None


def _fecha_clave(p: Path) -> int:
    """Calcula clave YYYY*100+MM desde nombre del archivo."""
    m = _RE_FECHA.search(p.stem)
    if m:
        return int(m.group(2)) * 100 + int(m.group(1))
    return 0


def _mejor_archivo_despachos(
    anio: str | None = None,
) -> Path | None:
    archivos = _listar_archivos("CUENTA POSE*.xlsx")
    if anio:
        archivos = [f for f in archivos if anio in f.name]
    if not archivos:
        return None
    return max(archivos, key=_fecha_clave)


# ── Helpers de pipeline ───────────────────────────────────────
def _run_etl() -> bool:
    _p("  Actualizando datos del Dashboard ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.report_direccion.src.etl_director",
            "--staging",
            str(STAGING),
            "--output",
            str(PARQUET),
        ]
    )
    _p(
        "  \u2714 Datos listos."
        if ok
        else "  \u2718 Error al actualizar datos."
    )
    return ok


def _preguntar_dashboard() -> bool:
    _p()
    _p("  [S]  Abrir Dashboard ahora")
    _p("  [N]  Volver al menu")
    _p()
    return input("  Opcion: ").strip().upper() == "S"


def _procesar_despachos(excel: Path) -> None:
    _p(f"  Archivo  : {excel.name}")
    _p("  [1/2] Ingresando datos a la Base de Datos ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.shared.pipeline_runner",
            "--informe",
            "report_direccion",
            "--archivo",
            str(excel),
        ]
    )
    if not ok:
        _p("  \u2718 Error al procesar.")
        input("  Pulse ENTER para continuar ...")
        return
    _p("  \u2714 Datos cargados.")
    if not _run_etl():
        input("  Pulse ENTER para continuar ...")
        return
    if _preguntar_dashboard():
        _abrir_dashboard()


# ── Submenú DESPACHOS ─────────────────────────────────────────
def _menu_despachos() -> None:
    while True:
        _cls()
        _p("  Fuente: DESPACHOS (CUENTA POSE)")
        _p("  " + _SEP2)
        _p()
        _p("  [T]  Archivo mas reciente (recomendado)")
        _p("  [A]  Filtrar por ano")
        _p("  [N]  Elegir archivo de la lista")
        _p("  [0]  Volver al menu")
        _p()
        opc = input("  Opcion: ").strip().upper()

        if opc == "0":
            return

        if opc == "T":
            if not _verificar_lookups():
                input("  ENTER para continuar...")
                continue
            mejor = _mejor_archivo_despachos()
            if mejor is None:
                _p(f"  No hay archivos en: {INPUT_RAW}")
                input("  ENTER para continuar...")
                continue
            _p(f"  Procesando: {mejor.name}")
            _procesar_despachos(mejor)

        elif opc == "A":
            anio = input("  Ano (ej: 2025): ").strip()
            if not (anio.isdigit() and 2000 <= int(anio) <= 2099):
                _p("  Ano no valido.")
                input("  ENTER para continuar...")
                continue
            if not _verificar_lookups():
                input("  ENTER para continuar...")
                continue
            mejor = _mejor_archivo_despachos(anio)
            if mejor is None:
                _p(f"  Sin archivos para el ano {anio}.")
                input("  ENTER para continuar...")
                continue
            _p(f"  Mas reciente {anio}: {mejor.name}")
            _procesar_despachos(mejor)

        elif opc == "N":
            archivos = _listar_archivos("CUENTA POSE*.xlsx")
            if not archivos:
                _p(f"  No hay archivos en: {INPUT_RAW}")
                input("  ENTER para continuar...")
                continue
            if not _verificar_lookups():
                input("  ENTER para continuar...")
                continue
            _cls()
            _p("  Archivos DESPACHOS disponibles:")
            _p("  " + _SEP2)
            _p()
            sel = _seleccionar_archivo(archivos)
            if sel:
                _procesar_despachos(sel)


# ── Submenú FDL ───────────────────────────────────────────────
def _ejecutar_fdl(extra: list[str]) -> None:
    if not _verificar_lookups():
        input("  ENTER para continuar ...")
        return
    _p("  Procesando archivos FDL ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.shared.pipeline_runner",
            "--informe",
            "report_direccion",
            "--stages-key",
            "STAGES_FDL",
        ]
        + extra
    )
    _p("  \u2714 Completado." if ok else "  \u2718 Error al procesar.")
    input("  ENTER para continuar ...")


def _menu_fdl() -> None:
    while True:
        _cls()
        _p("  Fuente: GG FDL  (Gastos Generales + Facturacion)")
        _p("  " + _SEP2)
        _p()
        _p("  [T]  Todos los periodos")
        _p("  [A]  Filtrar por ano  (ej: 2026)")
        _p("  [F]  Archivo especifico  (ej: 03-2026.xlsx)")
        _p("  [0]  Volver al menu")
        _p()
        opc = input("  Opcion: ").strip().upper()

        if opc == "0":
            return
        if opc == "T":
            _ejecutar_fdl([])
        elif opc == "A":
            anio = input("  Ano (ej: 2026): ").strip()
            if anio:
                _ejecutar_fdl(["--periodo", anio])
        elif opc == "F":
            archivos = _listar_archivos("??-????.xlsx")
            if not archivos:
                _p(f"  No hay archivos FDL en {INPUT_RAW}")
                input("  ENTER para continuar...")
                continue
            _cls()
            _p("  Archivos FDL disponibles:")
            _p()
            sel = _seleccionar_archivo(archivos)
            if sel:
                _ejecutar_fdl(["--fdl-archivo", sel.name])


# ── Submenú MENSUALES ─────────────────────────────────────────
def _ejecutar_mensuales(extra: list[str]) -> None:
    if not _verificar_lookups():
        input("  ENTER para continuar ...")
        return
    _p("  Procesando archivos MENSUALES ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.report_direccion.src.nuevas_fuentes" ".run_mensuales",
        ]
        + extra
    )
    _p("  \u2714 Completado." if ok else "  \u2718 Error al procesar.")
    input("  ENTER para continuar ...")


def _menu_mensuales() -> None:
    while True:
        _cls()
        _p("  Fuente: MENSUALES  (Sueldos y Jornales)")
        _p("  " + _SEP2)
        _p()
        _p("  [T]  Todos los periodos")
        _p("  [A]  Filtrar por ano  (ej: 2026)")
        _p("  [F]  Archivo especifico  (ej: 03-2026.xlsx)")
        _p("  [0]  Volver al menu")
        _p()
        opc = input("  Opcion: ").strip().upper()

        if opc == "0":
            return
        if opc == "T":
            _ejecutar_mensuales([])
        elif opc == "A":
            anio = input("  Ano (ej: 2026): ").strip()
            if anio:
                _ejecutar_mensuales(["--periodo", anio])
        elif opc == "F":
            archivos = _listar_archivos("??-????.xlsx")
            if not archivos:
                _p(f"  No hay archivos MENSUALES en {INPUT_RAW}")
                input("  ENTER para continuar...")
                continue
            _cls()
            _p("  Archivos MENSUALES disponibles:")
            _p()
            sel = _seleccionar_archivo(archivos)
            if sel:
                _ejecutar_mensuales(["--archivo", sel.name])


# ── Submenú QUINCENAS ─────────────────────────────────────────
QUINCENAS_RAW = _HERE / "report_gerencias" / "input_raw"


def _ejecutar_quincenas(extra: list[str]) -> None:
    if not _verificar_lookups():
        input("  ENTER para continuar ...")
        return
    _p("  Procesando archivos QUINCENAS ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.report_direccion.src.nuevas_fuentes" ".run_quincenas",
        ]
        + extra
    )
    _p("  \u2714 Completado." if ok else "  \u2718 Error al procesar.")
    input("  ENTER para continuar ...")


def _menu_quincenas() -> None:
    while True:
        _cls()
        _p("  Fuente: QUINCENAS  (Sueldos quincenales)")
        _p("  " + _SEP2)
        _p()
        _p("  [T]  Todos los periodos")
        _p("  [A]  Filtrar por ano  (ej: 2026)")
        _p("  [F]  Archivo especifico  (ej: QUINCENAS 03-2026.xlsx)")
        _p("  [0]  Volver al menu")
        _p()
        opc = input("  Opcion: ").strip().upper()

        if opc == "0":
            return
        if opc == "T":
            _ejecutar_quincenas([])
        elif opc == "A":
            anio = input("  Ano (ej: 2026): ").strip()
            if anio:
                _ejecutar_quincenas(["--periodo", anio])
        elif opc == "F":
            archivos = sorted(QUINCENAS_RAW.glob("QUINCENAS ??-????.xlsx"))
            if not archivos:
                _p(f"  No hay archivos QUINCENAS en {QUINCENAS_RAW}")
                input("  ENTER para continuar...")
                continue
            _cls()
            _p("  Archivos QUINCENAS disponibles:")
            _p()
            sel = _seleccionar_archivo(archivos)
            if sel:
                _ejecutar_quincenas(["--archivo", sel.name])


# ── Todas las fuentes ───────────────────────────────────────
def _ejecutar_todas_fuentes(extra: list[str]) -> None:
    if not _verificar_lookups():
        input("  ENTER para continuar ...")
        return
    _p("  Clasificando y procesando todas las fuentes ...")
    ok = _cmd(
        [
            _PY,
            "-m",
            "projects.report_direccion.src.nuevas_fuentes"
            ".run_todas_fuentes",
        ]
        + extra
    )
    _p("  \u2714 Completado." if ok else "  \u2718 Error al procesar.")
    input("  ENTER para continuar ...")


def _menu_todas_fuentes() -> None:
    while True:
        _cls()
        _p("  Fuente: TODAS LAS FUENTES  (buzon unico)")
        _p("  " + _SEP2)
        _p()
        _p("  [T]  Todos los periodos")
        _p("  [A]  Filtrar por ano  (ej: 2026)")
        _p("  [0]  Volver al menu")
        _p()
        opc = input("  Opcion: ").strip().upper()

        if opc == "0":
            return
        if opc == "T":
            _ejecutar_todas_fuentes([])
        elif opc == "A":
            anio = input("  Ano (ej: 2026): ").strip()
            if anio:
                _ejecutar_todas_fuentes(["--periodo", anio])


# ── Dashboard ─────────────────────────────────────────────────
def _abrir_dashboard() -> None:
    if not PARQUET.exists():
        _p()
        _p("  [ERROR] El parquet no fue generado todavia.")
        _p("  Ejecute DESPACHOS primero (opcion [1]).")
        input("  ENTER para continuar...")
        return
    _p()
    _p("  Abriendo Dashboard en el navegador...")
    _p("  URL: http://localhost:8501")
    _p()
    # Lanzar Streamlit desacoplado: el director lo cierra desde el navegador.
    # CREATE_NEW_CONSOLE oculta la ventana negra en Windows.
    flags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
    subprocess.Popen(
        [_PY, "-m", "streamlit", "run", str(APP), "--server.port", "8501"],
        cwd=str(REPO_ROOT),
        creationflags=flags,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
    )
    input("  Presione ENTER cuando termine la presentacion...")


# ── Menú principal ────────────────────────────────────────────
def main() -> None:
    while True:
        print("\n" + _SEP)
        print("  Sistema de Gestión de Costos  |  GRUPO POSE")
        print(_SEP)
        print()
        print("  [1]  DESPACHOS     Materiales de obra")
        print("  [2]  GG FDL        Gastos generales y facturación")
        print("  [3]  MENSUALES     Sueldos y jornales")
        print("  [4]  QUINCENAS     Sueldos quincenales")
        print("  [5]  TODAS         Buzon unico (clasifica automatico)")
        print("  [6]  Dashboard     Ver analisis (sin actualizar)")
        print()
        print("  [0]  Salir")
        print()
        opc = input("  Opcion : ").strip()
        print()

        if opc == "1":
            _menu_despachos()
        elif opc == "2":
            _menu_fdl()
        elif opc == "3":
            _menu_mensuales()
        elif opc == "4":
            _menu_quincenas()
        elif opc == "5":
            _menu_todas_fuentes()
        elif opc == "6":
            _abrir_dashboard()
        elif opc == "0":
            print("  Hasta luego.\n")
            break


if __name__ == "__main__":
    main()
