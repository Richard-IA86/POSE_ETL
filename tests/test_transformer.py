"""
Tests unitarios — DataTransformer (planif_pose/src/normalizador/transformer.py)

Cubre:
  - reset_stats
  - normalizar_columnas
  - filtrar_por_anio
  - detectar_duplicados
"""

import pandas as pd
import pytest

from transformer import DataTransformer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def transformer():
    return DataTransformer()


def _df_costos():
    """DataFrame mínimo con las columnas clave del normalizador."""
    return pd.DataFrame(
        {
            "_ID_INGESTA": [0, 1, 2, 3],
            "OBRA": ["ObraA", "ObraB", "ObraA", "ObraA"],
            "FECHA": [
                "01/03/2024",
                "15/07/2023",
                "01/03/2024",
                "01/03/2024",
            ],
            "DETALLE": ["Det1", "Det2", "Det1", "Det1"],
            "IMPORTE": [100.0, 200.0, 100.0, 100.0],
        }
    )


# ---------------------------------------------------------------------------
# reset_stats
# ---------------------------------------------------------------------------


class TestResetStats:
    def test_stats_inicial_en_cero(self, transformer):
        stats = transformer.stats
        assert stats["duplicados_origen"] == 0
        assert stats["duplicados_proceso"] == 0
        assert stats["filas_eliminadas"] == 0
        assert stats["filas_sin_fecha"] == 0

    def test_reset_restaura_valores(self, transformer):
        transformer.stats["duplicados_proceso"] = 5
        transformer.stats["filas_eliminadas"] = 3
        transformer.reset_stats()
        assert transformer.stats["duplicados_proceso"] == 0
        assert transformer.stats["filas_eliminadas"] == 0


# ---------------------------------------------------------------------------
# normalizar_columnas
# ---------------------------------------------------------------------------


class TestNormalizarColumnas:
    def test_convierte_a_mayusculas(self, transformer):
        # normalizar_columnas también aplica mapeo de sinónimos:
        # 'importe' y 'fuente' no tienen mapeo → quedan en mayúsculas
        df = pd.DataFrame({"importe": [100.0], "fuente": ["X"]})
        df_out = transformer.normalizar_columnas(df)
        assert "IMPORTE" in df_out.columns
        assert "FUENTE" in df_out.columns

    def test_elimina_asteriscos(self, transformer):
        # DETALLE* → DETALLE (sin mapeo adicional)
        # OBRA* → OBRA → mapeado a OBRA_PRONTO por sinónimos del normalizador
        df = pd.DataFrame({"DETALLE*": ["x"], "OBRA*": ["y"]})
        df_out = transformer.normalizar_columnas(df)
        assert "DETALLE" in df_out.columns
        assert "OBRA_PRONTO" in df_out.columns

    def test_strip_espacios_en_nombres(self, transformer):
        # ' FECHA ' (con espacios) → normaliza a 'FECHA'
        # ' IMPORTE ' → normaliza a 'IMPORTE'
        df = pd.DataFrame({" IMPORTE ": [100.0], "FECHA ": ["01/01/2024"]})
        df_out = transformer.normalizar_columnas(df)
        assert "IMPORTE" in df_out.columns
        assert "FECHA" in df_out.columns

    def test_elimina_columnas_duplicadas_por_asterisco(self, transformer):
        # DETALLE y DETALLE* → ambas quedan como DETALLE → duplicada
        df = pd.DataFrame({"DETALLE": ["a"], "DETALLE*": ["b"]})
        df_out = transformer.normalizar_columnas(df)
        assert df_out.columns.tolist().count("DETALLE") == 1

    def test_df_none_retorna_none(self, transformer):
        result = transformer.normalizar_columnas(None)
        assert result is None

    def test_df_vacio_retorna_vacio(self, transformer):
        df = pd.DataFrame()
        df_out = transformer.normalizar_columnas(df)
        assert df_out.empty


# ---------------------------------------------------------------------------
# filtrar_por_anio
# ---------------------------------------------------------------------------


class TestFiltrarPorAnio:
    def test_filtra_filas_por_anio(self, transformer):
        df = pd.DataFrame(
            {
                "FECHA": [
                    "10/01/2024",
                    "05/06/2023",
                    "20/12/2024",
                    "01/01/2022",
                ]
            }
        )
        df_out = transformer.filtrar_por_anio(df, [2024])
        assert len(df_out) == 2

    def test_multiples_anios(self, transformer):
        df = pd.DataFrame(
            {
                "FECHA": [
                    "10/01/2024",
                    "05/06/2023",
                    "01/01/2022",
                ]
            }
        )
        df_out = transformer.filtrar_por_anio(df, [2023, 2024])
        assert len(df_out) == 2

    def test_df_none_retorna_none(self, transformer):
        result = transformer.filtrar_por_anio(None, [2024])
        assert result is None

    def test_df_vacio_retorna_vacio(self, transformer):
        df = pd.DataFrame({"FECHA": []})
        df_out = transformer.filtrar_por_anio(df, [2024])
        assert df_out.empty

    def test_sin_columna_fecha_retorna_original(self, transformer):
        df = pd.DataFrame({"OBRA": ["A", "B"]})
        df_out = transformer.filtrar_por_anio(df, [2024])
        assert list(df_out["OBRA"]) == ["A", "B"]

    def test_lista_vacia_retorna_original(self, transformer):
        df = pd.DataFrame({"FECHA": ["01/01/2024"]})
        df_out = transformer.filtrar_por_anio(df, [])
        assert len(df_out) == 1

    def test_no_agrega_columna_temporal(self, transformer):
        df = pd.DataFrame({"FECHA": ["01/01/2024"]})
        df_out = transformer.filtrar_por_anio(df, [2024])
        assert "_ANIO_TEMP" not in df_out.columns


# ---------------------------------------------------------------------------
# detectar_duplicados
# ---------------------------------------------------------------------------


class TestDetectarDuplicados:
    def test_sin_id_ingesta_retorna_original(self, transformer):
        df = pd.DataFrame({"OBRA": ["A", "A"]})
        df_out = transformer.detectar_duplicados(df)
        assert len(df_out) == 2

    def test_sin_duplicados_no_elimina_filas(self, transformer):
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0, 1],
                "OBRA": ["ObraA", "ObraB"],
                "FECHA": ["01/01/2024", "01/01/2024"],
                "DETALLE": ["Det1", "Det2"],
                "IMPORTE": [100.0, 200.0],
            }
        )
        df_out = transformer.detectar_duplicados(df)
        assert len(df_out) == 2

    def test_duplicados_de_proceso_eliminados(self, transformer):
        """Duplicados de proceso: ID=0 aparece 2 veces con datos idénticos.
        Se debe eliminar únicamente el duplicado y conservar el primero
        (2 filas total).
        """
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0, 0, 1],
                "OBRA": ["ObraX", "ObraX", "ObraY"],
                "FECHA": ["01/01/2024", "01/01/2024", "02/02/2024"],
                "DETALLE": ["Det", "Det", "DetY"],
                "IMPORTE": [50.0, 50.0, 99.0],
            }
        )
        df_out = transformer.detectar_duplicados(df)
        assert len(df_out) == 2
        assert df_out["_ID_INGESTA"].tolist() == [0, 1]

    def test_duplicados_de_origen_conservados(self, transformer):
        """IDs distintos con mismos datos: duplicados de origen, conservados.

        Se conservan ambas filas (no son duplicados de proceso).
        """
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [1, 2],
                "OBRA": ["ObraY", "ObraY"],
                "FECHA": ["15/03/2024", "15/03/2024"],
                "DETALLE": ["Det", "Det"],
                "IMPORTE": [300.0, 300.0],
            }
        )
        df_out = transformer.detectar_duplicados(df)
        assert len(df_out) == 2

    def test_no_agrega_columna_temporal(self, transformer):
        df = _df_costos()
        df_out = transformer.detectar_duplicados(df)
        assert "_GRUPO_DUP" not in df_out.columns

    def test_stats_actualizados_tras_duplicados_proceso(self, transformer):
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [7, 7],
                "OBRA": ["X", "X"],
                "FECHA": ["01/06/2024", "01/06/2024"],
                "DETALLE": ["D", "D"],
                "IMPORTE": [10.0, 10.0],
            }
        )
        transformer.detectar_duplicados(df)
        assert transformer.stats["duplicados_proceso"] >= 1

    def test_modo_strict_elimina_origen(self, transformer):
        """strict: elimina duplicados aunque sean de origen (IDs distintos)."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [1, 2],
                "OBRA": ["ObraY", "ObraY"],
                "FECHA": ["15/03/2024", "15/03/2024"],
                "DETALLE": ["Det", "Det"],
                "IMPORTE": [300.0, 300.0],
            }
        )
        df_out = transformer.detectar_duplicados(df, modo="strict")
        assert len(df_out) == 1

    def test_modo_soft_conserva_origen(self, transformer):
        """soft: conserva duplicados de origen (IDs distintos)."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [1, 2],
                "OBRA": ["ObraY", "ObraY"],
                "FECHA": ["15/03/2024", "15/03/2024"],
                "DETALLE": ["Det", "Det"],
                "IMPORTE": [300.0, 300.0],
            }
        )
        df_out = transformer.detectar_duplicados(df, modo="soft")
        assert len(df_out) == 2

    def test_modo_invalido_usa_soft(self, transformer):
        """Modo desconocido cae a soft (conserva duplicados de origen)."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [1, 2],
                "OBRA": ["Z", "Z"],
                "FECHA": ["01/01/2024", "01/01/2024"],
                "DETALLE": ["X", "X"],
                "IMPORTE": [5.0, 5.0],
            }
        )
        df_out = transformer.detectar_duplicados(df, modo="ultra")
        assert len(df_out) == 2


# ---------------------------------------------------------------------------
# normalizar_columnas — calidad IMPORTE: Sin Dato vs Costo Cero
# ---------------------------------------------------------------------------


class TestCalidadImporte:
    def test_nulo_contabilizado_como_sin_dato(self, transformer):
        """IMPORTE nulo → importe_sin_dato += 1."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0],
                "OBRA": ["A"],
                "FECHA": ["01/01/2024"],
                "IMPORTE": [None],
            }
        )
        transformer.normalizar_columnas(df)
        assert transformer.stats["importe_sin_dato"] == 1

    def test_cero_explicito_contabilizado_como_costo_cero(self, transformer):
        """IMPORTE=0 en origen → importe_costo_cero += 1."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0],
                "OBRA": ["A"],
                "FECHA": ["01/01/2024"],
                "IMPORTE": [0.0],
            }
        )
        transformer.normalizar_columnas(df)
        assert transformer.stats["importe_costo_cero"] >= 1

    def test_nulo_rellenado_con_cero(self, transformer):
        """IMPORTE nulo se rellena con 0.0 tras normalizar."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0],
                "OBRA": ["A"],
                "FECHA": ["01/01/2024"],
                "IMPORTE": [None],
            }
        )
        df_out = transformer.normalizar_columnas(df)
        assert df_out["IMPORTE"].iloc[0] == 0.0

    def test_valor_positivo_no_afecta_stats_calidad(self, transformer):
        """IMPORTE positivo no incrementa ninguno de los contadores."""
        df = pd.DataFrame(
            {
                "_ID_INGESTA": [0],
                "OBRA": ["A"],
                "FECHA": ["01/01/2024"],
                "IMPORTE": [500.0],
            }
        )
        transformer.normalizar_columnas(df)
        assert transformer.stats["importe_sin_dato"] == 0
        assert transformer.stats["importe_costo_cero"] == 0

    def test_reset_limpia_stats_calidad(self, transformer):
        """reset_stats() limpia importe_sin_dato e importe_costo_cero."""
        transformer.stats["importe_sin_dato"] = 5
        transformer.stats["importe_costo_cero"] = 3
        transformer.reset_stats()
        assert transformer.stats["importe_sin_dato"] == 0
        assert transformer.stats["importe_costo_cero"] == 0
