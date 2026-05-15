"""
validar_cobertura_csv.py — Verifica cobertura 100% de columnas clave.

Uso (post-bifurcador):
    cd C:\\Dev\\POSE_ETL
    python scripts/validar_cobertura_csv.py

Detecta automáticamente el CSV completo más reciente en output/b52/.
Verifica que OBRA_PRONTO, GERENCIA y COMPENSABLE estén al 100%.
Sale con exit code 1 si alguna columna tiene vacíos.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
_OUTPUT_DIR = _REPO_ROOT / "output" / "b52"

COLS_VERIFICAR = ["OBRA_PRONTO", "GERENCIA", "COMPENSABLE"]


def _detectar_csv() -> Path:
    candidatos = sorted(
        [
            p
            for p in _OUTPUT_DIR.glob("costos_b52_*.csv")
            if "_delta" not in p.name
            and "_hashes" not in p.name
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidatos:
        print(f"❌ No hay CSV completo en {_OUTPUT_DIR}")
        sys.exit(1)
    return candidatos[0]


def main() -> None:
    csv_path = _detectar_csv()
    print(f"CSV: {csv_path.name}\n")

    df = pd.read_csv(
        csv_path,
        sep="|",
        usecols=COLS_VERIFICAR,
        dtype=str,
        low_memory=False,
    )
    total = len(df)
    errores = 0

    for col in COLS_VERIFICAR:
        vacios = (
            df[col].isna().sum()
            + (df[col].str.strip() == "").sum()
        )
        pct = (total - vacios) / total * 100
        estado = "✅" if vacios == 0 else "❌"
        print(
            f"{estado} {col:15}"
            f" total={total:,}"
            f"  vacios={vacios:,}"
            f"  poblados={total - vacios:,}"
            f"  ({pct:.2f}%)"
        )
        if vacios > 0:
            errores += 1

    print()
    if errores == 0:
        print("✅ Cobertura 100% — todo OK.")
    else:
        print(f"❌ {errores} columna(s) con vacíos. Revisar fuente.")
        sys.exit(1)


if __name__ == "__main__":
    main()
