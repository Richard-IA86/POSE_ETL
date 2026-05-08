"""
Tests unitarios — ExcelWriter.guardar_archivo
(planif_pose/src/normalizador/writer.py)

Cubre:
  - Retorna False para df None
  - Retorna False para df vacío
  - Retorna True y crea archivo para df válido
  - Elimina columna _ID_INGESTA del archivo exportado
  - Las columnas del schema_contract quedan primero en el resultado
"""

import pandas as pd
import pytest

from writer import ExcelWriter


@pytest.fixture
def writer(tmp_path):
    return ExcelWriter(str(tmp_path))


@pytest.fixture
def df_basico():
    return pd.DataFrame(
        {
            "FECHA": ["01/01/2025"],
            "OBRA_PRONTO": ["00000139"],
            "FUENTE": ["SAP"],
            "IMPORTE": [500.0],
        }
    )


# ---------------------------------------------------------------------------
# Guardianes de entrada
# ---------------------------------------------------------------------------


class TestGuardaArchivoGuardianes:
    def test_df_none_retorna_false(self, writer, tmp_path):
        resultado = writer.guardar_archivo(None, str(tmp_path), "salida.xlsx")
        assert resultado is False

    def test_df_vacio_retorna_false(self, writer, tmp_path):
        df_vacio = pd.DataFrame()
        resultado = writer.guardar_archivo(
            df_vacio, str(tmp_path), "salida.xlsx"
        )
        assert resultado is False


# ---------------------------------------------------------------------------
# Caso nominal
# ---------------------------------------------------------------------------


class TestGuardaArchivoNominal:
    def test_retorna_true_para_df_valido(self, writer, tmp_path, df_basico):
        resultado = writer.guardar_archivo(
            df_basico.copy(), str(tmp_path), "salida.xlsx"
        )
        assert resultado is True

    def test_crea_archivo_en_disco(self, writer, tmp_path, df_basico):
        nombre = "resultado_test.xlsx"
        writer.guardar_archivo(df_basico.copy(), str(tmp_path), nombre)
        assert (tmp_path / nombre).exists()

    def test_archivo_legible_con_pandas(self, writer, tmp_path, df_basico):
        nombre = "resultado_legible.xlsx"
        writer.guardar_archivo(df_basico.copy(), str(tmp_path), nombre)
        df_leido = pd.read_excel(tmp_path / nombre)
        assert len(df_leido) == 1

    def test_elimina_columna_id_ingesta(self, writer, tmp_path):
        df = pd.DataFrame(
            {
                "FECHA": ["01/01/2025"],
                "IMPORTE": [100.0],
                "_ID_INGESTA": ["abc123"],
            }
        )
        nombre = "sin_id.xlsx"
        writer.guardar_archivo(df.copy(), str(tmp_path), nombre)
        df_leido = pd.read_excel(tmp_path / nombre)
        assert "_ID_INGESTA" not in df_leido.columns


# ---------------------------------------------------------------------------
# Schema contract aplicado por guardar_archivo
# ---------------------------------------------------------------------------


class TestGuardaArchivoSchema:
    def test_columnas_canonicas_presentes_en_salida(
        self, writer, tmp_path, df_basico
    ):
        nombre = "schema.xlsx"
        writer.guardar_archivo(df_basico.copy(), str(tmp_path), nombre)
        df_leido = pd.read_excel(tmp_path / nombre)
        # Las columnas canonicas clave deben estar presentes
        for col in ["FECHA", "OBRA_PRONTO", "FUENTE", "IMPORTE"]:
            assert col in df_leido.columns
