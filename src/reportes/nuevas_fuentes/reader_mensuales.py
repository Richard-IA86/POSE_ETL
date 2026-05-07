"""
reader_mensuales.py — Lector crudo para la fuente MENSUALES.

Cada archivo MM-YYYY.xlsx contiene una hoja de resumen mensual:
  - En el servidor (original): tabla nombrada 'mensuales' con col
    'obra pronto' separada de la descripción.
  - En copias locales: hoja 'M-26' (ej: '1-26') con col
    'Etiquetas de fila' = "00000004 TALLER NUEVO" (código + desc).

El reader intenta primero la tabla nombrada 'mensuales' y cae
al modo hoja local si no existe.

Referencia: PowerQuery mensuales.pq (fuente autoritativa).
"""

from __future__ import annotations

import re
from pathlib import Path

import openpyxl
import pandas as pd

from .reader_fdl import parse_nombre_archivo


def leer_tabla_mensuales(path: Path) -> pd.DataFrame:
    """
    Lee los datos de MENSUALES del archivo mensual.

    Estrategia de lectura (fallback):
      1. Tabla nombrada 'mensuales' (archivos del servidor).
      2. Hoja 'M-26' derivada del nombre (copias locales).

    Columnas devueltas:
      OBRA_PRONTO, DESCRIPCION_OBRA, TOTAL_COSTO

    Sólo se incluyen filas con OBRA_PRONTO no vacío y
    TOTAL_COSTO numérico > 0.

    Raises
    ------
    ValueError
        Si no se encuentra ni la tabla ni la hoja esperada.
    """
    mes, _ = parse_nombre_archivo(path)

    wb = openpyxl.load_workbook(path, read_only=False)
    try:
        tabla_info = _buscar_tabla_mensuales(wb)
    finally:
        wb.close()

    if tabla_info:
        df = _leer_por_tabla(path, tabla_info)
    else:
        df = _leer_por_hoja_local(path, mes)

    df["TOTAL_COSTO"] = pd.to_numeric(df["TOTAL_COSTO"], errors="coerce")
    df = df[df["TOTAL_COSTO"].notna() & (df["TOTAL_COSTO"] != 0)]
    df = df[df["OBRA_PRONTO"].str.strip() != ""].reset_index(drop=True)
    return df[["OBRA_PRONTO", "DESCRIPCION_OBRA", "TOTAL_COSTO"]]


# ── Estrategia 1: tabla nombrada del servidor ────────────────────────────────


def _buscar_tabla_mensuales(
    wb: openpyxl.Workbook,
) -> dict[str, str] | None:
    """
    Devuelve {'sheet': nombre_hoja, 'ref': rango} si existe la tabla
    nombrada 'mensuales'; None si no.
    """
    for ws in wb.worksheets:
        for tname in ws.tables:
            if tname.lower() == "mensuales":
                return {"sheet": ws.title, "ref": ws.tables[tname].ref}
    return None


def _leer_por_tabla(path: Path, tabla_info: dict[str, str]) -> pd.DataFrame:
    """Lee desde la tabla nombrada 'mensuales' (formato servidor)."""
    df = pd.read_excel(path, sheet_name=tabla_info["sheet"], header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)

    # Columnas del servidor: 'obra pronto', 'TOTAL COSTO'
    df = df.rename(
        columns={
            "obra pronto": "OBRA_PRONTO",
            "TOTAL COSTO": "TOTAL_COSTO",
        }
    )
    if "OBRA_PRONTO" not in df.columns:
        raise ValueError(
            f"Tabla 'mensuales' en '{path.name}' no tiene columna "
            "'obra pronto'."
        )

    df["OBRA_PRONTO"] = (
        pd.to_numeric(df["OBRA_PRONTO"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .str.replace("<NA>", "", regex=False)
        .str.strip()
    )
    # En el servidor la descripción viene en columna separada
    if "DESCRIPCION_OBRA" not in df.columns:
        df["DESCRIPCION_OBRA"] = None

    return df


# ── Estrategia 2: hoja local 'M-26' ─────────────────────────────────────────

# Patrón: 8 dígitos con primer dígito 0 → campo concatenado
# (OBRA_PRONTO & DESCRIPCION_OBRA). Cualquier otra cadena es solo OBRA_PRONTO.
_PATRON_ETIQUETA = re.compile(r"^(0\d{7})\s+(.+)$")


def _leer_por_hoja_local(path: Path, mes: int) -> pd.DataFrame:
    """
    Lee la hoja resumen 'M-26' de las copias locales.

    Reglas columna 'Etiquetas de fila':
      - Empieza con 0 + 7 dígitos (8 en total) seguido de espacio:
          campo concatenado → OBRA_PRONTO='00000004',
          DESCRIPCION_OBRA='TALLER NUEVO'.
      - No empieza con ese patrón (ej. 'SEDE', 'CAC COMPENSAR'):
          valor completo → OBRA_PRONTO, DESCRIPCION_OBRA=''.
      - 'Total general' se excluye siempre.
    """
    sheet_name = f"{mes}-26"
    try:
        df_raw = pd.read_excel(path, sheet_name=sheet_name, header=0)
    except Exception as exc:
        raise ValueError(
            f"No se encontró tabla 'mensuales' ni hoja '{sheet_name}' "
            f"en '{path.name}': {exc}"
        ) from exc

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    df_raw = df_raw.dropna(how="all").reset_index(drop=True)

    if "Etiquetas de fila" not in df_raw.columns:
        raise ValueError(
            f"Hoja '{sheet_name}' en '{path.name}' no tiene columna "
            "'Etiquetas de fila'."
        )
    if "TOTAL COSTO" not in df_raw.columns:
        raise ValueError(
            f"Hoja '{sheet_name}' en '{path.name}' no tiene columna "
            "'TOTAL COSTO'."
        )

    # Excluir fila de totales y filas sin etiqueta (NaN/vacío)
    etiquetas = df_raw["Etiquetas de fila"].astype(str).str.strip()
    df_raw = df_raw[
        etiquetas.notna()
        & (etiquetas != "nan")
        & (etiquetas != "")
        & (etiquetas != "Total general")
    ].reset_index(drop=True)

    obras = []
    descs = []
    for etiqueta in df_raw["Etiquetas de fila"].astype(str):
        m = _PATRON_ETIQUETA.match(etiqueta.strip())
        if m:
            # Concatenado: 8 dígitos (comienza con 0) + descripción
            obras.append(m.group(1))
            descs.append(m.group(2).strip())
        else:
            # Sin descripción embebida: valor completo = OBRA_PRONTO
            obras.append(etiqueta.strip())
            descs.append("")

    return pd.DataFrame(
        {
            "OBRA_PRONTO": obras,
            "DESCRIPCION_OBRA": descs,
            "TOTAL_COSTO": df_raw["TOTAL COSTO"].values,
        }
    )
