"""
Validacion Ingesta — Opcion 1.

Compara output_normalized vs output_ready_for_pq por archivo.
Metricas: filas e importe (Sigma IMPORTE por archivo).

Genera:
  output/reportes/ingesta_YYYYMMDD_HHMMSS.dat
  (CSV pipe-separated, UTF-8, una fila por archivo procesado)

Uso:
  python -m src.ingesta.validacion_ingesta
  python -m src.ingesta.validacion_ingesta --segmento 2026
  python -m src.ingesta.validacion_ingesta --segmento 2025 2026

Exit code:
  0 — sin diferencias bloqueantes
  1 — hay archivos con PERDIDA de filas o importe divergente
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Any

import pandas as pd

DIR_NORM_IND = "output/output_normalized/individuales"
DIR_NORM_CON = "output/output_normalized/consolidados"
DIR_PQ = "output/output_ready_for_pq"
DIR_REP = "output/reportes"

SEGMENTOS_VALIDOS = [
    "2021_2022_Historico",
    "Modificaciones",
    "2025_Compensaciones",
    "2023_2025_Hist",
    "2025_Ajustes",
    "2025",
    "2026",
]

SEP = "=" * 72
COL_DAT = [
    "SEGMENTO",
    "ARCHIVO",
    "FILAS_NORM",
    "IMPORTE_NORM",
    "FILAS_PQ",
    "IMPORTE_PQ",
    "DELTA_FILAS",
    "DELTA_IMP",
    "ESTADO",
]


def parsear_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validacion ingesta: normalized vs ready_for_pq"
    )
    p.add_argument(
        "--segmento",
        nargs="+",
        metavar="SEG",
        choices=SEGMENTOS_VALIDOS,
        help="Segmento(s). Si se omite, todos.",
    )
    return p.parse_args()


def leer_metricas(ruta: str) -> tuple[int, float]:
    """
    Retorna (filas, suma_importe) de un xlsx normalizado.
    Si la columna IMPORTE no existe, importe = 0.0.
    """
    df = pd.read_excel(ruta, engine="openpyxl")
    filas = len(df)
    cols_up = [c.upper() for c in df.columns]
    if "IMPORTE" in cols_up:
        col = df.columns[cols_up.index("IMPORTE")]
        importe = float(pd.to_numeric(df[col], errors="coerce").sum())
    else:
        importe = 0.0
    return filas, importe


def fmt_imp(v: float, ancho: int = 16) -> str:
    """Importe formato ES: punto miles, coma decimal."""
    us = format(v, f"{ancho},.2f")
    return us.replace(".", "#").replace(",", ".").replace("#", ",")


def determinar_estado(
    filas_norm: int,
    filas_pq: int,
    delta_imp: float,
) -> str:
    """
    Clasifica el resultado de la comparacion:
      OK          — sin diferencias
      AVISO_FILAS — el alineador descarto filas (delta > 0 esperado
                    en consolidados, delta < 0 es sospechoso)
      PERDIDA     — el alineador perdio filas (bloqueante)
      IMP_DIFF    — importe diverge mas de 0.01 ARS (bloqueante)
    """
    if filas_pq == 0 and filas_norm > 0:
        return "PERDIDA_TOTAL"
    if filas_pq < filas_norm:
        return "PERDIDA"
    if abs(delta_imp) > 0.01:
        return "IMP_DIFF"
    return "OK"


def recolectar_pares(
    segmentos: list[str] | None,
) -> list[dict[str, Any]]:
    """
    Devuelve pares (ruta_norm, ruta_pq) por archivo.
    Busca el normalizado en individuales/ primero, luego
    consolidados/. Si no existe en ninguno, ruta_norm = "".
    """
    if not os.path.isdir(DIR_PQ):
        return []

    segs = segmentos if segmentos else sorted(os.listdir(DIR_PQ))
    pares: list[dict[str, Any]] = []

    for seg in segs:
        dir_pq = os.path.join(DIR_PQ, seg)
        if not os.path.isdir(dir_pq):
            continue
        for nombre in sorted(os.listdir(dir_pq)):
            if not nombre.lower().endswith(".xlsx"):
                continue
            ruta_pq = os.path.join(dir_pq, nombre)
            ruta_norm = os.path.join(DIR_NORM_IND, seg, nombre)
            if not os.path.exists(ruta_norm):
                ruta_norm = os.path.join(DIR_NORM_CON, seg, nombre)
            if not os.path.exists(ruta_norm):
                ruta_norm = ""
            pares.append(
                {
                    "seg": seg,
                    "nombre": nombre,
                    "ruta_norm": ruta_norm,
                    "ruta_pq": ruta_pq,
                }
            )
    return pares


def main() -> None:
    args = parsear_argumentos()
    segmentos: list[str] | None = args.segmento
    ts = datetime.now().strftime(SEP[0] and "%Y%m%d_%H%M%S")
    scope = ", ".join(segmentos) if segmentos else "todos los segmentos"

    print()
    print(SEP)
    print("  VALIDACION INGESTA — normalized vs ready_for_pq")
    print(
        f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}" f"  |  scope: {scope}"
    )
    print(SEP)

    pares = recolectar_pares(segmentos)
    if not pares:
        print("  Sin archivos para comparar en output_ready_for_pq.")
        sys.exit(0)

    registros: list[dict[str, Any]] = []
    bloqueantes: list[str] = []
    seg_actual = ""

    for par in pares:
        if par["seg"] != seg_actual:
            seg_actual = par["seg"]
            print(f"\n  [{seg_actual}]")
            print(
                f"     {'Archivo':<38} {'F.Norm':>7}"
                f" {'F.PQ':>7} {'dF':>6}  Estado"
            )
            print("     " + "-" * 66)

        nombre: str = par["nombre"]
        ruta_pq: str = par["ruta_pq"]
        ruta_norm: str = par["ruta_norm"]
        clave = f"{par['seg']}/{nombre}"

        if not ruta_norm:
            print(
                f"     {nombre:<38} {'---':>7}"
                f" {'---':>7} {'---':>6}  sin_norm"
            )
            registros.append(
                {
                    "SEGMENTO": par["seg"],
                    "ARCHIVO": nombre,
                    "FILAS_NORM": "",
                    "IMPORTE_NORM": "",
                    "FILAS_PQ": "",
                    "IMPORTE_PQ": "",
                    "DELTA_FILAS": "",
                    "DELTA_IMP": "",
                    "ESTADO": "SIN_NORM",
                }
            )
            continue

        try:
            fn, in_ = leer_metricas(ruta_norm)
            fp, ip = leer_metricas(ruta_pq)
            df = fp - fn
            di = ip - in_
            estado = determinar_estado(fn, fp, di)

            if estado not in ("OK",):
                bloqueantes.append(f"{clave} [{estado}]")

            signo = "+" if df >= 0 else ""
            print(
                f"     {nombre:<38} {fn:>7,}"
                f" {fp:>7,} {signo}{df:>5,}  {estado}"
            )
            print(
                f"     {'  Σ IMPORTE':<38}"
                f" {fmt_imp(in_)} {fmt_imp(ip)}"
                f"  Δ {fmt_imp(di, 14)}"
            )

            registros.append(
                {
                    "SEGMENTO": par["seg"],
                    "ARCHIVO": nombre,
                    "FILAS_NORM": fn,
                    "IMPORTE_NORM": round(in_, 2),
                    "FILAS_PQ": fp,
                    "IMPORTE_PQ": round(ip, 2),
                    "DELTA_FILAS": df,
                    "DELTA_IMP": round(di, 2),
                    "ESTADO": estado,
                }
            )

        except Exception as exc:
            print(
                f"     {nombre:<38} {'---':>7}"
                f" {'---':>7} {'---':>6}  Error: {exc}"
            )
            bloqueantes.append(f"{clave} [ERROR]")
            registros.append(
                {
                    "SEGMENTO": par["seg"],
                    "ARCHIVO": nombre,
                    "FILAS_NORM": "",
                    "IMPORTE_NORM": "",
                    "FILAS_PQ": "",
                    "IMPORTE_PQ": "",
                    "DELTA_FILAS": "",
                    "DELTA_IMP": "",
                    "ESTADO": f"ERROR: {exc}",
                }
            )

    # ── Guardar .dat ────────────────────────────────────────────
    os.makedirs(DIR_REP, exist_ok=True)
    nombre_dat = f"ingesta_{ts}.dat"
    ruta_dat = os.path.join(DIR_REP, nombre_dat)
    df_out = pd.DataFrame(registros, columns=COL_DAT)
    df_out.to_csv(ruta_dat, sep="|", index=False, encoding="utf-8")

    # ── Resumen ─────────────────────────────────────────────────
    print()
    print(SEP)
    total = len(registros)
    ok = sum(1 for r in registros if r["ESTADO"] == "OK")
    print(
        f"  Total archivos: {total}  |  OK: {ok}"
        f"  |  Con diferencias: {len(bloqueantes)}"
    )
    print(f"  .dat guardado: {ruta_dat}")

    if bloqueantes:
        print()
        print("  DIFERENCIAS DETECTADAS:")
        for b in bloqueantes:
            print(f"    - {b}")
        print(SEP)
        print()
        sys.exit(1)

    print("  Validacion APROBADA — normalized == ready_for_pq")
    print(SEP)
    print()
    sys.exit(0)


if __name__ == "__main__":
    main()
