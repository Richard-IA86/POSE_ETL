#!/usr/bin/env python3
"""
cargar_incremental_b52.py
Carga incremental de fact_costos_b52 desde un CSV delta (NUEVO/MODIFICADO).
Uso:
    python scripts/cargar_incremental_b52.py \
        --csv path/delta.csv [--dry-run]
"""

import argparse
import logging
import os
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuración
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
        description="Carga incremental de fact_costos_b52 desde CSV delta."
    )
    parser.add_argument(
        "--csv", required=True, help="Ruta al archivo CSV delta"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo simula la carga, no inserta",
    )
    return parser.parse_args()


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def upsert_rows(df: pd.DataFrame, dry_run: bool = False) -> None:
    cols = list(df.columns)
    values = [tuple(row) for row in df.values]
    set_cols = [c for c in cols if c not in PK_COLS]
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in set_cols)
    sql = (
        f"INSERT INTO {TABLE} ({', '.join(cols)})\n"
        f"VALUES %s\n"
        f"ON CONFLICT ({', '.join(PK_COLS)}) DO UPDATE SET {set_clause};"
    )
    logging.info(f"Filas a upsert: {len(values)}")
    if dry_run:
        logging.info("[DRY RUN] No se insertan datos.")
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
    logging.info("Upsert completado.")


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logging.error(f"No existe el archivo: {csv_path}")
        return
    df = pd.read_csv(csv_path, dtype=str)
    if df.empty:
        logging.warning("El CSV está vacío. Nada que cargar.")
        return
    upsert_rows(df, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
