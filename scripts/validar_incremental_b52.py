#!/usr/bin/env python3
"""
validar_incremental_b52.py
Valida la carga incremental de fact_costos_b52:
Uso:
    python scripts/validar_incremental_b52.py \
        --csv path/delta.csv [--anio 2026]
"""

import argparse
import logging
import os
from pathlib import Path
import pandas as pd
import psycopg2
import hashlib

DB_HOST = os.environ.get("POSE_DB_HOST", "localhost")
DB_PORT = os.environ.get("POSE_DB_PORT", "5432")
DB_NAME = os.environ.get("POSE_DB_NAME", "dwgrupopose_b53_prod")
DB_USER = os.environ.get("POSE_DB_USER", "postgres")
DB_PASS = os.environ.get("POSE_DB_PASS", "")

TABLE = "fact_costos_b52"
PK_COLS = ["periodo", "codigo_obra", "codigo_cuenta"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Valida la carga incremental de fact_costos_b52."
    )
    parser.add_argument(
        "--csv", required=True, help="Ruta al archivo CSV delta"
    )
    parser.add_argument("--anio", type=str, help="Año a validar (opcional)")
    return parser.parse_args()


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def hash_row(row: pd.Series) -> str:
    s = "|".join(str(v) for v in row.values)
    return hashlib.sha256(s.encode()).hexdigest()


def fetch_db_rows(anio: str | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        query = f"SELECT * FROM {TABLE}"
        if anio:
            query += f" WHERE periodo LIKE '{anio}%'"
        df = pd.read_sql(query, conn)
    return df


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logging.error(f"No existe el archivo: {csv_path}")
        return
    df_csv = pd.read_csv(csv_path, dtype=str)
    if df_csv.empty:
        logging.warning("El CSV está vacío. Nada que validar.")
        return
    anio = args.anio
    df_db = fetch_db_rows(anio)
    # Hash por PK
    df_csv["_hash"] = df_csv.apply(hash_row, axis=1)
    df_db["_hash"] = df_db.apply(hash_row, axis=1)
    # Merge por PK
    merged = pd.merge(
        df_csv, df_db, on=PK_COLS, suffixes=("_csv", "_db"), how="left"
    )
    # Diferencias
    diffs = merged[merged["_hash_csv"] != merged["_hash_db"]]
    if not diffs.empty:
        logging.warning(f"Filas con diferencias: {len(diffs)}")
        print(diffs[PK_COLS + ["_hash_csv", "_hash_db"]])
    else:
        logging.info("Todas las filas del delta coinciden en la BD.")
    # Conteo total
    total_db = len(df_db)
    total_csv = len(df_csv)
    logging.info(f"Total filas en BD: {total_db}")
    logging.info(f"Total filas en CSV delta: {total_csv}")
    # Duplicados
    dups = df_db.duplicated(subset=PK_COLS, keep=False)
    if dups.any():
        logging.error("¡Hay duplicados en la BD por PK!")
        print(df_db[dups][PK_COLS])
    else:
        logging.info("No hay duplicados por PK en la BD.")


if __name__ == "__main__":
    main()
