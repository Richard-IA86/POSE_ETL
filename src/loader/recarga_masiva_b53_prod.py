"""
Recarga masiva de fact_costos_b52 en Hetzner (PostgreSQL).

Estrategia:
  1. DROP TABLE IF EXISTS + CREATE TABLE (schema 17 cols)
  2. INSERT en batches desde el CSV completo del bifurcador
  3. Validación final: COUNT(*) == filas_csv

Uso:
  cd /home/richard/Dev/POSE_ETL
  ETL_ENV=PROD python src/loader/recarga_masiva_b53_prod.py

Variables de entorno (via config/.env o export manual):
  PG_HOST   (default: 10.10.0.1)
  PG_PORT   (default: 5432)
  PG_USER   (default: pose_admin)
  PG_PASS   — obligatoria
  PG_DB_PROD (default: dw_grupopose_b52_prod)
"""

import os
import sys
import pathlib
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ── Rutas ────────────────────────────────────────────────────────────────────
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_ENV_PATH = _REPO_ROOT / "config" / ".env"


def _detectar_csv() -> pathlib.Path:
    """Retorna el CSV completo más reciente en output/b52/."""
    directorio = _REPO_ROOT / "output" / "b52"
    candidatos = sorted(
        [
            p
            for p in directorio.glob("costos_b52_*.csv")
            if "_delta" not in p.name and "_hashes" not in p.name
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidatos:
        print(f"❌ No hay CSV en {directorio}")
        sys.exit(1)
    return candidatos[0]


# ── Parámetros ───────────────────────────────────────────────────────────────
TABLA = "fact_costos_b52"
SCHEMA = "public"
BATCH_SIZE = 50_000
FILAS_ESPERADAS = 473_098

# Columnas que se cargan en la tabla (sin ANIO, MES, _hash_*, _estado_carga)
COLS_DESTINO = [
    "OBRA_PRONTO",
    "DESCRIPCION_OBRA",
    "FECHA",
    "FUENTE",
    "TIPO_COMPROBANTE",
    "NRO_COMPROBANTE",
    "PROVEEDOR",
    "DETALLE",
    "CODIGO_CUENTA",
    "IMPORTE",
    "OBSERVACION",
    "RUBRO_CONTABLE",
    "CUENTA_CONTABLE",
    "COMPENSABLE",
    "GERENCIA",
    "TC",
    "IMPORTE_USD",
]

DDL_CREAR_TABLA = f"""
CREATE TABLE {TABLA} (
    "OBRA_PRONTO"       VARCHAR(20),
    "DESCRIPCION_OBRA"  TEXT,
    "FECHA"             DATE,
    "FUENTE"            VARCHAR(100),
    "TIPO_COMPROBANTE"  VARCHAR(100),
    "NRO_COMPROBANTE"   VARCHAR(100),
    "PROVEEDOR"         TEXT,
    "DETALLE"           TEXT,
    "CODIGO_CUENTA"     VARCHAR(50),
    "IMPORTE"           NUMERIC(18, 2),
    "OBSERVACION"       TEXT,
    "RUBRO_CONTABLE"    VARCHAR(200),
    "CUENTA_CONTABLE"   VARCHAR(200),
    "COMPENSABLE"       VARCHAR(100),
    "GERENCIA"          VARCHAR(100),
    "TC"                NUMERIC(10, 6),
    "IMPORTE_USD"       NUMERIC(18, 2)
);
"""


def _build_engine() -> Any:
    load_dotenv(dotenv_path=_ENV_PATH)
    host = os.getenv("PG_HOST", "10.10.0.1")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "pose_admin")
    password = os.getenv("PG_PASS", "")
    database = os.getenv("PG_DB_PROD", "dw_grupopose_b52_prod")
    if not password:
        print("❌ PG_PASS no definida. Abortando.")
        sys.exit(1)
    url = (
        f"postgresql+psycopg2://{user}:{password}" f"@{host}:{port}/{database}"
    )
    return create_engine(url, pool_pre_ping=True)


def _ping(engine: Any) -> None:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT version()")).fetchone()
        db = engine.url.database
        print(f"✅ Conexión OK — {db}")
        if row:
            print(f"   {row[0][:60]}")


def _drop_and_create(engine: Any) -> None:
    print(f"\n[1/5] DROP + CREATE {TABLA}...")
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{TABLA}";'))
        conn.execute(text(DDL_CREAR_TABLA))
    print(f"      ✅ Tabla {TABLA} recreada (17 cols).")


def _cargar_csv() -> pd.DataFrame:
    csv_path = _detectar_csv()
    print(f"\n[2/5] Leyendo CSV: {csv_path.name}...")
    df = pd.read_csv(
        csv_path,
        sep="|",
        usecols=COLS_DESTINO,
        dtype=str,
        low_memory=False,
    )
    print(f"      Filas leídas : {len(df):,}")
    print(f"      Columnas     : {list(df.columns)}")
    return df


def _convertir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df["IMPORTE"] = pd.to_numeric(df["IMPORTE"], errors="coerce")
    df["TC"] = pd.to_numeric(df["TC"], errors="coerce")
    df["IMPORTE_USD"] = pd.to_numeric(df["IMPORTE_USD"], errors="coerce")
    return df


def _insertar(df: pd.DataFrame, engine: Any) -> int:
    total = len(df)
    insertadas = 0
    n_batches = (total // BATCH_SIZE) + (1 if total % BATCH_SIZE else 0)
    print(
        f"\n[3/5] Insertando {total:,} filas"
        f" en {n_batches} batches de {BATCH_SIZE:,}..."
    )
    for i in range(0, total, BATCH_SIZE):
        batch = df.iloc[i : i + BATCH_SIZE]
        batch.to_sql(
            name=TABLA,
            con=engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
        )
        insertadas += len(batch)
        pct = insertadas / total * 100
        print(
            f"      batch {i // BATCH_SIZE + 1}/{n_batches}"
            f" — {insertadas:,} filas ({pct:.1f}%)"
        )
    return insertadas


def _poblar_gerencia(engine: Any) -> int:
    """UPDATE GERENCIA en fact desde dim_obras_gerencias."""
    print("\n[4/5] Poblando GERENCIA desde dim_obras_gerencias...")
    with engine.begin() as conn:
        r = conn.execute(text("""
                UPDATE fact_costos_b52 f
                SET "GERENCIA" = d.gerencia
                FROM dim_obras_gerencias d
                WHERE f."OBRA_PRONTO" = d.obra_pronto
                  AND d.gerencia IS NOT NULL
                  AND d.gerencia != ''
            """))
        actualizadas = r.rowcount
    print(f"      ✅ {actualizadas:,} filas con GERENCIA poblada.")
    return actualizadas


def _validar(engine: Any, insertadas: int, gerencia: int) -> None:
    print("\n[5/5] Validando COUNT(*) en Hetzner...")
    with engine.connect() as conn:
        row = conn.execute(text(f'SELECT COUNT(*) FROM "{TABLA}"')).fetchone()
        count_db = row[0] if row else 0

    print(f"      Filas CSV        : {insertadas:,}")
    print(f"      COUNT(*) DB      : {count_db:,}")
    print(f"      GERENCIA poblada : {gerencia:,}")
    print(f"      Filas esperadas  : {FILAS_ESPERADAS:,}")

    if count_db == insertadas == FILAS_ESPERADAS:
        print("\n✅ RECARGA COMPLETA — todo cuadra.")
    elif count_db == insertadas:
        print(
            "\n⚠️  COUNT cuadra con CSV pero difiere"
            f" del esperado ({FILAS_ESPERADAS:,})."
            " Verificar manualmente."
        )
    else:
        print(
            f"\n❌ DIVERGENCIA: DB={count_db:,}"
            f" vs CSV={insertadas:,}. Investigar."
        )
        sys.exit(1)


def main() -> None:
    print("=" * 60)
    print("RECARGA MASIVA — fact_costos_b52 (Hetzner PROD)")
    print("=" * 60)

    engine = _build_engine()
    _ping(engine)
    _drop_and_create(engine)
    df = _cargar_csv()
    df = _convertir_tipos(df)
    insertadas = _insertar(df, engine)
    gerencia = _poblar_gerencia(engine)
    _validar(engine, insertadas, gerencia)


if __name__ == "__main__":
    main()
