"""
Tests unitarios — contratos de validacion ETL Base A2.

Cubre:
  - validar_schema: columnas faltantes y extras
  - validar_integridad: df vacio, nulos en criticas, importe no num
  - validar_rango_fecha: fuera de rango, segmento desconocido
  - resumen_calidad: metricas de completitud
"""

import pandas as pd
import pytest

from ETL_BaseA2.src.validacion.contratos import (
    COLUMNAS_CRITICAS,
    COLUMNAS_NEGOCIO,
    resumen_calidad,
    validar_integridad,
    validar_rango_fecha,
    validar_schema,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _df_completo() -> pd.DataFrame:
    """DataFrame con todas las columnas canonicas de negocio."""
    data = {col: ["valor"] for col in COLUMNAS_NEGOCIO}
    data["IMPORTE"] = [1000.0]
    data["FECHA"] = ["2025-01-15"]
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# validar_schema
# ---------------------------------------------------------------------------


class TestValidarSchema:
    def test_schema_completo_sin_errores(self):
        df = _df_completo()
        assert validar_schema(df) == []

    def test_columna_faltante_genera_error(self):
        df = _df_completo().drop(columns=["OBRA_PRONTO"])
        errores = validar_schema(df)
        assert any("OBRA_PRONTO" in e for e in errores)

    def test_columna_extra_genera_error(self):
        df = _df_completo()
        df["COLUMNA_RARA"] = "x"
        errores = validar_schema(df)
        assert any("COLUMNA_RARA" in e.upper() for e in errores)

    def test_todas_las_columnas_criticas_presentes(self):
        df = _df_completo()
        presentes = {c.upper() for c in df.columns}
        for col in COLUMNAS_CRITICAS:
            assert col.upper() in presentes


# ---------------------------------------------------------------------------
# validar_integridad
# ---------------------------------------------------------------------------


class TestValidarIntegridad:
    def test_df_valido_sin_errores(self):
        df = _df_completo()
        assert validar_integridad(df) == []

    def test_df_vacio_genera_error(self):
        df = pd.DataFrame(columns=COLUMNAS_NEGOCIO)
        errores = validar_integridad(df)
        assert any("vacio" in e for e in errores)

    def test_nulo_en_obra_pronto_genera_error(self):
        df = _df_completo()
        df["OBRA_PRONTO"] = None
        errores = validar_integridad(df)
        assert any("OBRA_PRONTO" in e for e in errores)

    def test_importe_no_numerico_genera_error(self):
        df = _df_completo()
        df["IMPORTE"] = "no_es_numero"
        errores = validar_integridad(df)
        assert any("IMPORTE" in e for e in errores)

    def test_importe_negativo_permitido(self):
        df = _df_completo()
        df["IMPORTE"] = -50000.0
        assert validar_integridad(df) == []


# ---------------------------------------------------------------------------
# validar_rango_fecha
# ---------------------------------------------------------------------------


class TestValidarRangoFecha:
    def test_fecha_dentro_rango_sin_errores(self):
        df = _df_completo()
        df["FECHA"] = "2026-02-15"
        assert validar_rango_fecha(df, "2026") == []

    def test_fecha_fuera_rango_genera_error(self):
        df = _df_completo()
        df["FECHA"] = "2020-01-01"
        errores = validar_rango_fecha(df, "2026")
        assert any("2026" in e for e in errores)

    def test_segmento_desconocido_sin_errores(self):
        df = _df_completo()
        assert validar_rango_fecha(df, "segmento_x") == []

    def test_sin_columna_fecha_sin_errores(self):
        df = _df_completo().drop(columns=["FECHA"])
        assert validar_rango_fecha(df, "2026") == []


# ---------------------------------------------------------------------------
# resumen_calidad
# ---------------------------------------------------------------------------


class TestResumenCalidad:
    def test_retorna_dict_con_claves_esperadas(self):
        df = _df_completo()
        resumen = resumen_calidad(df)
        assert "filas" in resumen
        assert "columnas" in resumen
        assert "completitud" in resumen
        assert "importe_sum" in resumen
        assert "obras_unicas" in resumen

    def test_filas_correctas(self):
        df = _df_completo()
        assert resumen_calidad(df)["filas"] == 1

    def test_importe_sum_correcto(self):
        df = _df_completo()
        df["IMPORTE"] = 5000.0
        resumen = resumen_calidad(df)
        assert resumen["importe_sum"] == pytest.approx(5000.0)

    def test_obras_unicas_correctas(self):
        df = pd.concat([_df_completo(), _df_completo()])
        df["OBRA_PRONTO"] = ["OBRA-A", "OBRA-B"]
        assert resumen_calidad(df)["obras_unicas"] == 2

    def test_completitud_100_en_df_completo(self):
        df = _df_completo()
        resumen = resumen_calidad(df)
        for col in COLUMNAS_CRITICAS:
            assert resumen["completitud"][col] == 100.0
