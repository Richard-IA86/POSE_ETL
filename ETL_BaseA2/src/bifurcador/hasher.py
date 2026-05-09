"""
hasher.py — Control incremental por hash de fila.

Calcula dos hashes SHA-256 por registro:
  _hash_fila    → identidad del registro (clave de negocio)
  _hash_importe → identidad + valor (detecta cambio de importe)

Clave de negocio:
  OBRA_PRONTO + FECHA + FUENTE + TIPO_COMPROBANTE + NRO_COMPROBANTE

Estado resultante por fila (columna _estado_carga):
  NUEVO    → hash_fila no existe en el registro anterior
  SIN_CAMBIO → hash_importe idéntico → skip en carga
  MODIFICADO → hash_fila conocido pero hash_importe distinto

El registro anterior se guarda en:
  output/b52/_hashes_anterior.csv
  (columnas: _hash_fila | _hash_importe)
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pandas as pd

HASH_FILE_DEFAULT = "output/b52/_hashes_anterior.csv"

COLS_CLAVE: list[str] = [
    "OBRA_PRONTO",
    "FECHA",
    "FUENTE",
    "TIPO_COMPROBANTE",
    "NRO_COMPROBANTE",
]


def _sha256_fila(row: pd.Series) -> str:
    """SHA-256 de la clave de negocio (identidad del registro)."""
    partes = "|".join(
        str(row[c]).strip().upper() if c in row.index else ""
        for c in COLS_CLAVE
    )
    return hashlib.sha256(partes.encode("utf-8")).hexdigest()


def _sha256_importe(hash_fila: str, importe: float) -> str:
    """SHA-256 de la identidad + importe (detecta modificación)."""
    contenido = f"{hash_fila}|{importe:.6f}"
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


def calcular_hashes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columnas _hash_fila y _hash_importe al DataFrame.
    No modifica columnas de negocio existentes.
    """
    df = df.copy()

    # Rellenar ausentes en cols clave para hasheo consistente
    for col in COLS_CLAVE:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str)

    importe_num = pd.to_numeric(
        df.get("IMPORTE", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0.0)

    df["_hash_fila"] = df.apply(_sha256_fila, axis=1)
    df["_hash_importe"] = [
        _sha256_importe(hf, imp)
        for hf, imp in zip(df["_hash_fila"], importe_num)
    ]

    return df


def cargar_hashes_anterior(
    ruta: str = HASH_FILE_DEFAULT,
) -> dict[str, str]:
    """
    Lee el CSV de hashes de la corrida anterior.
    Retorna dict { hash_fila -> hash_importe }.
    Si no existe, retorna dict vacío (primera corrida).
    """
    p = Path(ruta)
    if not p.exists():
        return {}

    try:
        df = pd.read_csv(p, dtype=str)
        if "_hash_fila" not in df.columns or "_hash_importe" not in df.columns:
            return {}
        return dict(zip(df["_hash_fila"], df["_hash_importe"]))
    except Exception:
        return {}


def clasificar_estado(
    df: pd.DataFrame,
    hashes_anterior: dict[str, str],
) -> pd.DataFrame:
    """
    Agrega columna _estado_carga a cada fila:
      NUEVO | SIN_CAMBIO | MODIFICADO

    Requiere que df tenga columnas _hash_fila y _hash_importe.
    """
    df = df.copy()

    estados: list[str] = []
    for _, row in df.iterrows():
        hf = row["_hash_fila"]
        hi = row["_hash_importe"]
        if hf not in hashes_anterior:
            estados.append("NUEVO")
        elif hashes_anterior[hf] == hi:
            estados.append("SIN_CAMBIO")
        else:
            estados.append("MODIFICADO")

    df["_estado_carga"] = estados
    return df


def guardar_hashes(
    df: pd.DataFrame,
    ruta: str = HASH_FILE_DEFAULT,
) -> None:
    """
    Persiste los hashes actuales para la próxima corrida.
    Sobreescribe el CSV anterior.
    """
    os.makedirs(Path(ruta).parent, exist_ok=True)
    df[["_hash_fila", "_hash_importe"]].to_csv(
        ruta, index=False, encoding="utf-8"
    )


def resumen_estados(df: pd.DataFrame) -> dict[str, int]:
    """
    Retorna conteo por _estado_carga.
    """
    if "_estado_carga" not in df.columns:
        return {}
    conteo = df["_estado_carga"].value_counts().to_dict()
    return {
        "NUEVO": conteo.get("NUEVO", 0),
        "SIN_CAMBIO": conteo.get("SIN_CAMBIO", 0),
        "MODIFICADO": conteo.get("MODIFICADO", 0),
    }
