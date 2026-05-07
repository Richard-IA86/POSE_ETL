"""
test_etl_director.py — Tests unitarios para etl_director.py.

Estrategia: staging sintético en CSV (sin Excel, sin BD, sin red).
Se verifica tipado, columnas derivadas, parquet generado y errores esperados.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from src.reportes.etl_director import (
    generar_parquet,
    _RENAME,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COLS_STAGING = [
    "MES",
    "FECHA*",
    "OBRA PRONTO*",
    "DESCRIPCION OBRA",
    "GERENCIA",
    "DETALLE*",
    "IMPORTE*",
    "TIPO COMPROBANTE*",
    "N° COMPROBANTE*",
    "OBSERVACION*",
    "PROVEEDOR*",
    "RUBRO CONTABLE*",
    "CUENTA CONTABLE*",
    "CODIGO CUENTA*",
    "FUENTE*",
    "COMPENSABLE",
    "TC",
    "IMPORTE USD",
]


def _make_staging_csv(filas: list[dict]) -> str:
    """Genera CSV con separador ';' y encoding UTF-8 (sin BOM)."""
    df = pd.DataFrame(filas, columns=_COLS_STAGING)
    buf = io.StringIO()
    df.to_csv(buf, sep=";", index=False, encoding="utf-8")
    return buf.getvalue()


def _staging_base() -> list[dict]:
    return [
        {
            "MES": "Enero",
            "FECHA*": "2025-01-15",
            "OBRA PRONTO*": "OBR-001",
            "DESCRIPCION OBRA": "Obra Norte",
            "GERENCIA": "GERENCIA A",
            "DETALLE*": "Materiales varios",
            "IMPORTE*": 100000.0,
            "TIPO COMPROBANTE*": "REMITO",
            "N° COMPROBANTE*": "R-001",
            "OBSERVACION*": "",
            "PROVEEDOR*": "PROVEEDOR SA",
            "RUBRO CONTABLE*": "Materiales",
            "CUENTA CONTABLE*": "MATERIALES",
            "CODIGO CUENTA*": "511700002",
            "FUENTE*": "CAC",
            "COMPENSABLE": "N",
            "TC": 1050.5,
            "IMPORTE USD": 95.19,
        },
        {
            "MES": "Febrero",
            "FECHA*": "2025-02-10",
            "OBRA PRONTO*": "OBR-002",
            "DESCRIPCION OBRA": "Obra Sur",
            "GERENCIA": "GERENCIA B",
            "DETALLE*": "Mano de obra",
            "IMPORTE*": 250000.0,
            "TIPO COMPROBANTE*": "REMITO",
            "N° COMPROBANTE*": "R-002",
            "OBSERVACION*": "",
            "PROVEEDOR*": "CONTRATISTA SRL",
            "RUBRO CONTABLE*": "Materiales",
            "CUENTA CONTABLE*": "MATERIALES",
            "CODIGO CUENTA*": "511700002",
            "FUENTE*": "CAC",
            "COMPENSABLE": "S",
            "TC": 1060.0,
            "IMPORTE USD": 235.85,
        },
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def staging_csv(tmp_path: Path) -> Path:
    """CSV de staging sintético con 2 filas válidas."""
    csv_path = tmp_path / "staging_despachos.csv"
    csv_path.write_text(_make_staging_csv(_staging_base()), encoding="utf-8")
    return csv_path


@pytest.fixture()
def parquet_path(tmp_path: Path) -> Path:
    return tmp_path / "output" / "datos_director.parquet"


# ---------------------------------------------------------------------------
# Tests — happy path
# ---------------------------------------------------------------------------


class TestGenerar:
    def test_genera_parquet(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        ruta = generar_parquet(staging_csv, parquet_path)
        assert ruta.exists(), "El parquet debe existir tras la generación."

    def test_filas_conservadas(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path, extra_stagings=[])
        df = pd.read_parquet(parquet_path)
        assert len(df) == 2

    def test_columnas_renombradas(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        nombres_esperados = set(_RENAME.values())
        assert nombres_esperados.issubset(set(df.columns))

    def test_sin_nombres_raw_con_asterisco(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        cols_raw = [c for c in df.columns if "*" in c]
        assert cols_raw == [], f"No deben quedar columnas con '*': {cols_raw}"

    def test_columnas_derivadas(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        for col in ("anio", "mes", "periodo", "mes_nombre_ord"):
            assert col in df.columns, f"Falta columna derivada: {col}"

    def test_periodo_formato_correcto(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        periodos = df["periodo"].unique().tolist()
        for p in periodos:
            assert len(p) == 7, f"Periodo inesperado: {p}"  # YYYY-MM

    def test_importe_numerico(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        assert pd.api.types.is_numeric_dtype(df["importe_ars"])

    def test_total_importe(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path, extra_stagings=[])
        df = pd.read_parquet(parquet_path)
        total = df["importe_ars"].sum()
        assert total == pytest.approx(350_000.0, rel=0.001)

    def test_gerencia_es_categoria(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        assert str(df["gerencia"].dtype) == "category"

    def test_fecha_es_datetime(
        self, staging_csv: Path, parquet_path: Path
    ) -> None:
        generar_parquet(staging_csv, parquet_path)
        df = pd.read_parquet(parquet_path)
        assert pd.api.types.is_datetime64_any_dtype(df["fecha"])

    def test_output_dir_se_crea(
        self, staging_csv: Path, tmp_path: Path
    ) -> None:
        nuevo_dir = tmp_path / "subdir_nuevo" / "datos_director.parquet"
        generar_parquet(staging_csv, nuevo_dir)
        assert nuevo_dir.exists()


# ---------------------------------------------------------------------------
# Tests — errores esperados
# ---------------------------------------------------------------------------


class TestErrores:
    def test_staging_no_existe(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="staging no encontrado"):
            generar_parquet(
                tmp_path / "no_existe.csv",
                tmp_path / "out.parquet",
            )

    def test_staging_vacio(self, tmp_path: Path) -> None:
        csv_vacio = tmp_path / "vacio.csv"
        encabezado = ";".join(_COLS_STAGING)
        csv_vacio.write_text(encabezado + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="vacío"):
            generar_parquet(
                csv_vacio, tmp_path / "out.parquet", extra_stagings=[]
            )

    def test_staging_sin_columnas_requeridas(self, tmp_path: Path) -> None:
        csv_malo = tmp_path / "malo.csv"
        csv_malo.write_text("colA;colB\n1;2\n", encoding="utf-8")
        with pytest.raises(ValueError, match="Columnas faltantes"):
            generar_parquet(
                csv_malo, tmp_path / "out.parquet", extra_stagings=[]
            )
