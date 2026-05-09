"""
Validacion Corridas — Opcion 5.

Lee output/director/BaseCostosPOSE.xlsx, agrupa por ANIO+MES
y compara filas e importe contra la corrida anterior registrada
en output/reportes/corrida_*.dat.

Genera:
  output/reportes/corrida_YYYYMMDD_HHMMSS.dat
  (CSV pipe-separated, UTF-8, una fila por ANIO+MES)

Columnas .dat:
  ANIO | MES | FILAS | IMPORTE | DELTA_FILAS | DELTA_IMP | ESTADO

Uso:
  python -m src.ingesta.validacion_corridas
  python -m src.ingesta.validacion_corridas --xlsx otro.xlsx

Exit code:
  0 — primera corrida o sin diferencias detectadas
  1 — hay meses con cambios en filas o importe
"""

import argparse
import glob
import os
import sys
from datetime import datetime
from typing import Any

import pandas as pd

ARCHIVO_DEFAULT = "output/director/BaseCostosPOSE.xlsx"
DIR_REP = "output/reportes"

COL_DAT = [
    "ANIO",
    "MES",
    "FILAS",
    "IMPORTE",
    "DELTA_FILAS",
    "DELTA_IMP",
    "ESTADO",
]

SEP = "=" * 72


def parsear_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Validacion corridas: BaseCostosPOSE.xlsx" " vs corrida anterior"
        )
    )
    p.add_argument(
        "--xlsx",
        default=ARCHIVO_DEFAULT,
        help=("Ruta al Excel de salida final. " f"Default: {ARCHIVO_DEFAULT}"),
    )
    return p.parse_args()


def fmt_imp(v: float, ancho: int = 16) -> str:
    """Importe formato ES: punto miles, coma decimal."""
    us = format(v, f"{ancho},.2f")
    return us.replace(".", "#").replace(",", ".").replace("#", ",")


def leer_base_costos(ruta: str) -> pd.DataFrame:
    """
    Lee BaseCostosPOSE.xlsx y retorna DataFrame con columnas
    ANIO (int), MES (int), IMPORTE (float).
    Extrae ANIO y MES de la columna FECHA.
    """
    df = pd.read_excel(ruta, engine="openpyxl")
    cols_up = {c.upper(): c for c in df.columns}

    if "FECHA" not in cols_up:
        raise ValueError("Columna FECHA no encontrada en el archivo.")
    if "IMPORTE" not in cols_up:
        raise ValueError("Columna IMPORTE no encontrada en el archivo.")

    col_fecha = cols_up["FECHA"]
    col_imp = cols_up["IMPORTE"]

    df["_fecha_dt"] = pd.to_datetime(
        df[col_fecha], errors="coerce", dayfirst=True
    )
    df["ANIO"] = df["_fecha_dt"].dt.year
    df["MES"] = df["_fecha_dt"].dt.month
    df["_IMP"] = pd.to_numeric(df[col_imp], errors="coerce")

    sin_fecha = df["_fecha_dt"].isna().sum()
    if sin_fecha > 0:
        print(
            f"  [AVISO] {sin_fecha} filas con FECHA no parseable"
            " — excluidas del agrupado."
        )

    return df[["ANIO", "MES", "_IMP"]].dropna(subset=["ANIO", "MES"])


def agrupar_por_mes(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Agrupa por ANIO+MES y calcula filas e importe total.
    Retorna DataFrame ordenado cronologicamente.
    """
    agrupado = (
        df.groupby(["ANIO", "MES"], as_index=False)
        .agg(FILAS=("_IMP", "count"), IMPORTE=("_IMP", "sum"))
        .sort_values(["ANIO", "MES"])
        .reset_index(drop=True)
    )
    agrupado["ANIO"] = agrupado["ANIO"].astype(int)
    agrupado["MES"] = agrupado["MES"].astype(int)
    agrupado["IMPORTE"] = agrupado["IMPORTE"].round(2)
    return agrupado


def buscar_corrida_anterior() -> pd.DataFrame | None:
    """
    Busca el .dat de corrida mas reciente en output/reportes/.
    Retorna DataFrame o None si no existe ninguno.
    """
    patron = os.path.join(DIR_REP, "corrida_*.dat")
    archivos = sorted(glob.glob(patron))
    if not archivos:
        return None
    ultimo = archivos[-1]
    print(f"  Corrida anterior: {os.path.basename(ultimo)}")
    return pd.read_csv(
        ultimo,
        sep="|",
        dtype={"ANIO": int, "MES": int},
        encoding="utf-8",
    )


def comparar(
    actual: pd.DataFrame,
    anterior: pd.DataFrame | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Cruza actual vs anterior por ANIO+MES.
    Retorna (registros_dat, lista_cambios_detectados).
    """
    registros: list[dict[str, Any]] = []
    cambios: list[str] = []

    ant_idx: dict[tuple[int, int], dict[str, Any]] = {}
    if anterior is not None:
        for _, row in anterior.iterrows():
            clave = (int(row["ANIO"]), int(row["MES"]))
            ant_idx[clave] = row.to_dict()

    for _, row in actual.iterrows():
        anio = int(row["ANIO"])
        mes = int(row["MES"])
        filas = int(row["FILAS"])
        importe = float(row["IMPORTE"])

        if anterior is None or (anio, mes) not in ant_idx:
            delta_f: int | str = ""
            delta_i: float | str = ""
            estado = "NUEVA" if anterior is not None else "OK"
        else:
            ant = ant_idx[(anio, mes)]
            delta_f = filas - int(ant["FILAS"])
            delta_i = round(importe - float(ant["IMPORTE"]), 2)
            if delta_f != 0 or abs(float(delta_i)) > 0.01:
                estado = "CAMBIO"
                cambios.append(
                    f"{anio:04d}-{mes:02d} "
                    f"Δfilas={delta_f:+,} "
                    f"Δimp={delta_i:+,.2f}"
                )
            else:
                estado = "OK"

        registros.append(
            {
                "ANIO": anio,
                "MES": mes,
                "FILAS": filas,
                "IMPORTE": importe,
                "DELTA_FILAS": delta_f,
                "DELTA_IMP": delta_i,
                "ESTADO": estado,
            }
        )

    return registros, cambios


def main() -> None:
    args = parsear_argumentos()
    ruta_xlsx: str = args.xlsx
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print()
    print(SEP)
    print("  VALIDACION CORRIDAS — BaseCostosPOSE.xlsx vs anterior")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(SEP)

    if not os.path.exists(ruta_xlsx):
        print(f"  [ERROR] Archivo no encontrado: {ruta_xlsx}")
        sys.exit(1)

    print(f"  Leyendo: {ruta_xlsx}")

    try:
        df_raw = leer_base_costos(ruta_xlsx)
    except ValueError as exc:
        print(f"  [ERROR] {exc}")
        sys.exit(1)

    actual = agrupar_por_mes(df_raw)
    anterior = buscar_corrida_anterior()

    registros, cambios = comparar(actual, anterior)

    # ── Imprimir tabla ──────────────────────────────────────────
    print()
    print(
        f"  {'ANIO':>4} {'MES':>3}  {'Filas':>8}"
        f"  {'Importe':>18}  {'dFilas':>7}  {'dImp':>16}"
        f"  Estado"
    )
    print("  " + "-" * 70)

    for r in registros:
        df_str = (
            f"{r['DELTA_FILAS']:+,}"
            if isinstance(r["DELTA_FILAS"], int)
            else "---"
        )
        di_str = (
            fmt_imp(float(r["DELTA_IMP"]), 16)
            if isinstance(r["DELTA_IMP"], float)
            else "---"
        )
        print(
            f"  {r['ANIO']:>4} {r['MES']:>3}  {r['FILAS']:>8,}"
            f"  {fmt_imp(r['IMPORTE'])}"
            f"  {df_str:>7}  {di_str:>16}"
            f"  {r['ESTADO']}"
        )

    # ── Guardar .dat ────────────────────────────────────────────
    os.makedirs(DIR_REP, exist_ok=True)
    nombre_dat = f"corrida_{ts}.dat"
    ruta_dat = os.path.join(DIR_REP, nombre_dat)
    df_out = pd.DataFrame(registros, columns=COL_DAT)
    df_out.to_csv(ruta_dat, sep="|", index=False, encoding="utf-8")

    # ── Resumen ─────────────────────────────────────────────────
    total_filas = int(actual["FILAS"].sum())
    total_imp = float(actual["IMPORTE"].sum())
    print()
    print(SEP)
    print(
        f"  Total filas: {total_filas:,}"
        f"  |  Σ importe: {fmt_imp(total_imp).strip()}"
    )
    print(f"  .dat guardado: {ruta_dat}")

    if anterior is None:
        print("  Primera corrida registrada — sin comparacion.")
        print(SEP)
        print()
        sys.exit(0)

    if cambios:
        print(f"  CAMBIOS vs corrida anterior ({len(cambios)}):")
        for c in cambios:
            print(f"    - {c}")
        print(SEP)
        print()
        sys.exit(1)

    print("  Sin cambios vs corrida anterior. Corrida APROBADA.")
    print(SEP)
    print()
    sys.exit(0)


if __name__ == "__main__":
    main()
