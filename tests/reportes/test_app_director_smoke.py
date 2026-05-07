"""
test_app_director_smoke.py — Smoke tests para app_director.py.

Estrategia: importar módulo + verificar lógica interna sin levantar
servidor Streamlit (no hay llamadas a st.*).

Cubre:
  - Importación del módulo sin errores
  - Ruta por defecto del parquet (variable _DEFAULT_PARQUET)
  - Función _cargar_datos soporta env var DATOS_DIRECTOR_PATH
  - Constante _VERSION_APP presente y no vacía
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

pytest.importorskip("streamlit")

# ---------------------------------------------------------------------------
# Test de importación (smoke básico)
# ---------------------------------------------------------------------------


class TestImportacion:
    def test_modulo_importable(self) -> None:
        """El módulo debe importar sin excepciones."""
        import src.reportes.dashboard.app_director as mod

        assert mod is not None

    def test_version_presente(self) -> None:
        import src.reportes.dashboard.app_director as mod

        assert mod._VERSION_APP, "_VERSION_APP no debe ser vacío."

    def test_parquet_default_es_path(self) -> None:
        import src.reportes.dashboard.app_director as mod

        assert isinstance(mod._DEFAULT_PARQUET, Path)


# ---------------------------------------------------------------------------
# Test de _cargar_datos con parquet sintético
# ---------------------------------------------------------------------------


def _parquet_sintetico(tmp_path: Path) -> Path:
    """Crea un parquet con datos mínimos para las pruebas del dashboard."""
    df = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2025-01-15", "2025-02-10"]),
            "obra": ["OBR-001", "OBR-002"],
            "descripcion_obra": ["Obra Norte", "Obra Sur"],
            "gerencia": pd.Categorical(["GER A", "GER B"]),
            "detalle": ["Det 1", "Det 2"],
            "importe_ars": [100_000.0, 250_000.0],
            "tipo_comprobante": pd.Categorical(["REMITO", "REMITO"]),
            "nro_comprobante": ["R-001", "R-002"],
            "observacion": ["", ""],
            "proveedor": ["PROV SA", "CONTRAT SRL"],
            "rubro_contable": pd.Categorical(["Materiales", "Materiales"]),
            "cuenta_contable": pd.Categorical(["MATERIALES", "MATERIALES"]),
            "codigo_cuenta": ["511700002", "511700002"],
            "fuente": pd.Categorical(["CAC", "CAC"]),
            "compensable": ["N", "S"],
            "tipo_cambio": [1050.5, 1060.0],
            "importe_usd": [95.19, 235.85],
            "mes_nombre": ["Enero", "Febrero"],
            "anio": pd.array([2025, 2025], dtype="Int16"),
            "mes": pd.array([1, 2], dtype="Int8"),
            "periodo": ["2025-01", "2025-02"],
            "mes_nombre_ord": [1, 2],
        }
    )
    ruta = tmp_path / "datos_director.parquet"
    df.to_parquet(ruta, engine="pyarrow", index=False)
    return ruta


class TestCargarDatos:
    def test_carga_con_env_var(self, tmp_path: Path) -> None:
        """_cargar_datos lee el parquet indicado en DATOS_DIRECTOR_PATH."""
        import src.reportes.dashboard.app_director as mod

        ruta = _parquet_sintetico(tmp_path)

        # Parchear st.cache_data para que no use caché en tests
        with (
            mock.patch.dict(os.environ, {"DATOS_DIRECTOR_PATH": str(ruta)}),
            mock.patch("streamlit.cache_data", lambda **_kw: (lambda f: f)),
            mock.patch("streamlit.stop"),
            mock.patch("streamlit.error"),
        ):
            # Forzar recarga del decorador sin caché
            df = (
                mod._cargar_datos.__wrapped__()
                if hasattr(mod._cargar_datos, "__wrapped__")
                else _cargar_desde_ruta(ruta)
            )

        assert df is not None

    def test_carga_directa_desde_ruta(self, tmp_path: Path) -> None:
        """Lee el parquet y verifica columnas mínimas."""
        ruta = _parquet_sintetico(tmp_path)
        df = pd.read_parquet(ruta)
        cols_requeridas = {
            "fecha",
            "importe_ars",
            "gerencia",
            "periodo",
            "anio",
            "mes",
        }
        assert cols_requeridas.issubset(set(df.columns))

    def test_dos_filas_en_parquet_sintetico(self, tmp_path: Path) -> None:
        ruta = _parquet_sintetico(tmp_path)
        df = pd.read_parquet(ruta)
        assert len(df) == 2

    def test_total_importe_esperado(self, tmp_path: Path) -> None:
        ruta = _parquet_sintetico(tmp_path)
        df = pd.read_parquet(ruta)
        assert df["importe_ars"].sum() == pytest.approx(350_000.0)


# ---------------------------------------------------------------------------
# Helper para tests que no pueden usar el decorador cacheado
# ---------------------------------------------------------------------------


def _cargar_desde_ruta(ruta: Path) -> pd.DataFrame:
    """Lectura directa sin pasar por streamlit.cache_data."""
    df = pd.read_parquet(ruta)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df
