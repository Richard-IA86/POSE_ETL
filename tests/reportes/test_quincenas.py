"""
test_quincenas.py — Tests unitarios del pipeline QUINCENAS.

Cubre:
  - leer_hojas_quincenas: retorno de tupla (df, info_hojas) y caso sin
    hojas
  - _normalizar_texto: upper, strip, colapso de espacios
  - transformar_quincenas: columnas COLS_STAGING, ramas sueldos/cargas,
    filtro de ceros, Q2 fecha día 15, fallo Opción B
  - procesar_lote_quincenas: lote vacío y lote con un archivo
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.reportes.nuevas_fuentes._constantes import (
    COLS_STAGING,
)
from src.reportes.nuevas_fuentes.reader_quincenas import (
    leer_hojas_quincenas,
)
from src.reportes.nuevas_fuentes.transformer_quincenas import (  # noqa: E501
    _normalizar_texto,
    transformar_quincenas,
)
from src.reportes.nuevas_fuentes.run_quincenas import (  # noqa: E501
    _descubrir_archivos,
    procesar_lote_quincenas,
)
from src.reportes.nuevas_fuentes.writer_quincenas import (
    escribir_staging_quincenas,
)

# ── Fixtures ─────────────────────────────────────────────────────────

_LOOCKUPS_FAKE = Path("/fake/Loockups.xlsx")


def _df_crudo() -> pd.DataFrame:
    """Crudo mínimo con dos empleados, quincena 1 y 2 en la misma
    obra."""
    return pd.DataFrame(
        {
            "LEGAJO Nº": [101, 102],
            "APELLIDO": ["GARCIA", "PEREZ"],
            "NOMBRE": ["JUAN", "ANA"],
            "CATEGORIA": ["A1", "B2"],
            "TAREA": ["ALBANIL", "CAPATAZ"],
            "OBRA": ["Planta Norte", "Planta Norte"],
            "FECHA": pd.to_datetime(["2026-01-01", "2026-01-01"]),
            "QUINCENA": [1, 2],
            "SUBTOTAL SUELDO": [50000.0, 80000.0],
            "SUBTOTAL IMPUESTOS": [10000.0, 0.0],
        }
    )


def _df_lookups() -> pd.DataFrame:
    """Lookup simulado — retornado por _leer_obras_gerencias."""
    return pd.DataFrame(
        {
            "OBRA_PRONTO": ["00000001"],
            "DESCRIPCION_OBRA": ["Planta Norte"],
            "GERENCIA": ["GER A"],
            "COMPENSABLE": ["S"],
        }
    )


# ── leer_hojas_quincenas ─────────────────────────────────────────────


class TestLeerHojasQuincenas:
    def test_retorna_tupla(self) -> None:
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".reader_quincenas.pd.ExcelFile"
            ) as mock_excel_file,
            patch(
                "src.reportes.nuevas_fuentes" ".reader_quincenas.pd.read_excel"
            ) as mock_read,
        ):
            mock_xls = MagicMock()
            mock_xls.sheet_names = [
                "1ER QUINCENA ENE",
                "2DA QUINCENA ENE",
            ]
            mock_excel_file.return_value = mock_xls
            mock_read.return_value = pd.DataFrame({"A": [1], "B": [2]})
            resultado = leer_hojas_quincenas(Path("01-2026.xlsx"))
            assert isinstance(resultado, tuple)
            assert len(resultado) == 2

    def test_sin_hojas_quincenas_devuelve_vacio(self) -> None:
        with patch(
            "src.reportes.nuevas_fuentes" ".reader_quincenas.pd.ExcelFile"
        ) as mock_excel_file:
            mock_xls = MagicMock()
            mock_xls.sheet_names = [
                "TICKET - AUMENTOS",
                "BANCOS HABILITADOS",
            ]
            mock_excel_file.return_value = mock_xls
            df, info = leer_hojas_quincenas(Path("01-2026.xlsx"))
            assert df.empty
            assert info == ""

    def test_info_contiene_nombre_hoja(self) -> None:
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".reader_quincenas.pd.ExcelFile"
            ) as mock_excel_file,
            patch(
                "src.reportes.nuevas_fuentes" ".reader_quincenas.pd.read_excel"
            ) as mock_read,
        ):
            mock_xls = MagicMock()
            mock_xls.sheet_names = ["1ER QUINCENA ENE"]
            mock_excel_file.return_value = mock_xls
            mock_read.return_value = pd.DataFrame({"A": [1]})
            _df, info = leer_hojas_quincenas(Path("01-2026.xlsx"))
            assert "1ER QUINCENA ENE" in info


# ── _normalizar_texto ────────────────────────────────────────────────


class TestNormalizarTexto:
    def test_convierte_a_mayusculas(self) -> None:
        assert _normalizar_texto("planta norte") == "PLANTA NORTE"

    def test_colapsa_espacios(self) -> None:
        assert _normalizar_texto("PLANTA   NORTE") == "PLANTA NORTE"

    def test_none_devuelve_vacio(self) -> None:
        assert _normalizar_texto(None) == ""

    def test_elimina_saltos_de_linea(self) -> None:
        assert _normalizar_texto("OBRA\nNORTE") == "OBRA NORTE"


# ── transformar_quincenas ────────────────────────────────────────────


class TestTransformarQuincenas:
    def _run(self, df_crudo: pd.DataFrame) -> pd.DataFrame:
        with patch(
            "src.reportes.nuevas_fuentes"
            ".transformer_quincenas._leer_obras_gerencias",
            return_value=_df_lookups(),
        ):
            return transformar_quincenas(df_crudo, _LOOCKUPS_FAKE)

    def test_columnas_staging_exactas(self) -> None:
        df = self._run(_df_crudo())
        assert list(df.columns) == COLS_STAGING

    def test_genera_dos_ramas_por_empleado(self) -> None:
        # 2 empleados × 2 ramas (sueldos + cargas) = 4 filas
        # pero empleado 2 tiene SUBTOTAL IMPUESTOS=0 → filtrado
        # → sueldos: 2 filas, cargas: 1 fila = 3 filas
        df = self._run(_df_crudo())
        assert len(df) == 3

    def test_fuente_quincenas(self) -> None:
        df = self._run(_df_crudo())
        assert (df["FUENTE*"] == "QUINCENAS").all()

    def test_proveedor_pose(self) -> None:
        df = self._run(_df_crudo())
        assert (df["PROVEEDOR*"] == "POSE").all()

    def test_q2_fecha_dia_15(self) -> None:
        df = self._run(_df_crudo())
        # El empleado con QUINCENA=2 (PEREZ) tiene fecha ajustada al 15
        fechas_q2 = [f for f in df["FECHA*"] if f and f.endswith("-15")]
        assert len(fechas_q2) >= 1

    def test_importe_sueldos_negado(self) -> None:
        df = self._run(_df_crudo())
        sueldos = df[df["CUENTA CONTABLE*"] == "SUELDOS Y JORNALES"]
        assert (sueldos["IMPORTE*"] < 0).all()

    def test_importe_cargas_negado(self) -> None:
        df = self._run(_df_crudo())
        cargas = df[
            df["CUENTA CONTABLE*"] == "CONTRIBUCIONES Y CARGAS SOCIALES"
        ]
        assert (cargas["IMPORTE*"] < 0).all()

    def test_fila_importe_cero_filtrada(self) -> None:
        # PEREZ tiene SUBTOTAL IMPUESTOS=0 → su rama CARGAS no aparece
        df = self._run(_df_crudo())
        cargas = df[
            df["CUENTA CONTABLE*"] == "CONTRIBUCIONES Y CARGAS SOCIALES"
        ]
        # Solo GARCIA (IMPUESTOS=10000) debe estar en cargas
        assert len(cargas) == 1

    def test_fallo_opcion_b_obra_sin_match(self) -> None:
        df_sin_match = _df_crudo().copy()
        df_sin_match["OBRA"] = "Obra Inexistente XYZ"
        with pytest.raises(ValueError, match="Fallo B"):
            self._run(df_sin_match)

    def test_tc_e_importe_usd_none(self) -> None:
        df = self._run(_df_crudo())
        assert df["TC"].isna().all()
        assert df["IMPORTE USD"].isna().all()

    def test_corresponde_y_comentario_son_none(self) -> None:
        df = self._run(_df_crudo())
        assert df["CORRESPONDE*"].isna().all()
        assert df["COMENTARIO*"].isna().all()

    def test_detalle_sin_espacio_entre_apellido_nombre(
        self,
    ) -> None:
        df = self._run(_df_crudo())
        # GARCIA + JUAN → "GARCIAJUAN" (sin separador como en PQ)
        assert df["DETALLE*"].str.contains("GARCIAJUAN").any()

    def test_depuracion_descarta_fila_sin_legajo(self) -> None:
        df_con_nan = _df_crudo().copy()
        df_con_nan.loc[0, "LEGAJO Nº"] = float("nan")
        # Solo PEREZ queda: SUELDO=80000 (1 fila),
        # IMPUESTOS=0 filtrado. Total=1.
        df = self._run(df_con_nan)
        assert len(df) == 1

    def test_mes_en_espanol(self) -> None:
        df = self._run(_df_crudo())
        assert (df["MES"] == "Enero").all()

    def test_columnas_minimas_faltantes_levanta_error(self) -> None:
        df_roto = _df_crudo().drop(columns=["OBRA"])
        with patch(
            "src.reportes.nuevas_fuentes"
            ".transformer_quincenas._leer_obras_gerencias",
            return_value=_df_lookups(),
        ):
            with pytest.raises(ValueError, match="columnas"):
                transformar_quincenas(df_roto, _LOOCKUPS_FAKE)

    def test_df_vacio_devuelve_staging_vacio(self) -> None:
        with patch(
            "src.reportes.nuevas_fuentes"
            ".transformer_quincenas._leer_obras_gerencias",
            return_value=_df_lookups(),
        ):
            df = transformar_quincenas(pd.DataFrame(), _LOOCKUPS_FAKE)
            assert df.empty
            assert list(df.columns) == COLS_STAGING


# ── procesar_lote_quincenas ──────────────────────────────────────────


class TestProcesarLoteQuincenas:
    def test_lote_vacio_devuelve_df_vacio(self) -> None:
        df = procesar_lote_quincenas([], _LOOCKUPS_FAKE)
        assert df.empty

    def test_lote_un_archivo(self) -> None:
        archivo_fake = Path("/fake/01-2026.xlsx")
        df_esperado = pd.DataFrame({col: ["x"] for col in COLS_STAGING})
        with (
            patch(
                "src.reportes.nuevas_fuentes"
                ".run_quincenas.leer_hojas_quincenas",
                return_value=(_df_crudo(), "1ER QUINCENA ENE"),
            ),
            patch(
                "src.reportes.nuevas_fuentes"
                ".run_quincenas.transformar_quincenas",
                return_value=df_esperado,
            ),
        ):
            df = procesar_lote_quincenas([archivo_fake], _LOOCKUPS_FAKE)
            assert not df.empty
            assert list(df.columns) == COLS_STAGING


class TestDescubrirArchivosQuincenas:
    def test_descubre_solo_patron_quincenas(self, tmp_path: Path) -> None:
        (tmp_path / "QUINCENAS 01-2026.xlsx").touch()
        (tmp_path / "01-2026.xlsx").touch()
        result = _descubrir_archivos(tmp_path)
        assert len(result) == 1
        assert result[0].name == "QUINCENAS 01-2026.xlsx"

    def test_filtra_por_periodo(self, tmp_path: Path) -> None:
        (tmp_path / "QUINCENAS 01-2026.xlsx").touch()
        (tmp_path / "QUINCENAS 01-2025.xlsx").touch()
        result = _descubrir_archivos(tmp_path, periodo="2026")
        assert len(result) == 1
        assert "2026" in result[0].name

    def test_descubrir_desde_input_raw_raiz(
        self,
        tmp_path: Path,
    ) -> None:
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        (input_raw / "QUINCENAS 01-2026.xlsx").touch()

        result = _descubrir_archivos(input_raw)
        assert len(result) == 1
        assert result[0].name == "QUINCENAS 01-2026.xlsx"

    def test_archivo_manual_con_nombre_invalido_falla(
        self,
        tmp_path: Path,
    ) -> None:
        (tmp_path / "01-2026.xlsx").touch()
        with pytest.raises(ValueError, match="Formato esperado"):
            _descubrir_archivos(tmp_path, nombre="01-2026.xlsx")


class TestCsvPendientesMapeo:
    def test_pisa_csv_vacio_si_no_hay_pendientes(self, tmp_path: Path) -> None:
        common = tmp_path / "common"
        csv_path = common / "Obras_Pendientes_Mapeo.csv"

        with (
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._COMMON_DIR",
                common,
            ),
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._PENDIENTES_MAPEO_CSV",
                csv_path,
            ),
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._leer_obras_gerencias",
                return_value=_df_lookups(),
            ),
        ):
            transformar_quincenas(_df_crudo(), _LOOCKUPS_FAKE)

        assert csv_path.exists()
        df = pd.read_csv(csv_path, sep=";")
        assert list(df.columns) == [
            "descripcion",
            "nombre_archivo",
            "origen",
        ]
        assert df.empty

    def test_pisa_csv_con_pendientes_y_lanza_error(
        self,
        tmp_path: Path,
    ) -> None:
        common = tmp_path / "common"
        csv_path = common / "Obras_Pendientes_Mapeo.csv"
        df_sin_match = _df_crudo().copy()
        df_sin_match["OBRA"] = "Obra Inexistente XYZ"

        with (
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._COMMON_DIR",
                common,
            ),
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._PENDIENTES_MAPEO_CSV",
                csv_path,
            ),
            patch(
                "src.reportes.nuevas_fuentes"
                ".transformer_quincenas._leer_obras_gerencias",
                return_value=_df_lookups(),
            ),
        ):
            with pytest.raises(ValueError, match="Fallo B"):
                transformar_quincenas(df_sin_match, _LOOCKUPS_FAKE)

        assert csv_path.exists()
        df = pd.read_csv(csv_path, sep=";")
        assert "descripcion" in df.columns
        assert "nombre_archivo" in df.columns
        assert "origen" in df.columns
        assert df["origen"].eq("QUINCENAS").all()
        assert len(df) >= 1


# ── EscritorQuincenas ────────────────────────────────────────────────


class TestEscribirStagingQuincenas:
    """Contrato del CSV de staging: sep=';', encoding=utf-8-sig,
    round-trip sin pérdida."""

    def _df_minimal(self) -> pd.DataFrame:
        return pd.DataFrame({col: ["x"] for col in COLS_STAGING})

    def test_archivo_creado(self, tmp_path: Path) -> None:
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            ruta = escribir_staging_quincenas(self._df_minimal())
        assert ruta == destino
        assert destino.exists()

    def test_separador_es_punto_y_coma(self, tmp_path: Path) -> None:
        """El CSV debe poder leerse con sep=';' sin error."""
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            escribir_staging_quincenas(self._df_minimal())
        df = pd.read_csv(destino, sep=";", encoding="utf-8-sig")
        assert len(df) == 1
        assert list(df.columns) == COLS_STAGING

    def test_separador_coma_falla(self, tmp_path: Path) -> None:
        """Leer con sep=',' (default) debe producir columnas != COLS.
        Documenta que el separador NO es coma."""
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            escribir_staging_quincenas(self._df_minimal())
        df_mal = pd.read_csv(destino, sep=",", encoding="utf-8-sig")
        assert list(df_mal.columns) != COLS_STAGING

    def test_encoding_utf8_sig(self, tmp_path: Path) -> None:
        """El CSV debe comenzar con BOM utf-8-sig."""
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            escribir_staging_quincenas(self._df_minimal())
        raw = destino.read_bytes()
        assert (
            raw[:3] == b"\xef\xbb\xbf"
        ), "El CSV debe comenzar con BOM utf-8-sig"

    def test_round_trip_no_pierde_filas(self, tmp_path: Path) -> None:
        """Escribir N filas y releer debe devolver exactamente N."""
        df_orig = pd.concat([self._df_minimal()] * 5, ignore_index=True)
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            escribir_staging_quincenas(df_orig)
        df_back = pd.read_csv(destino, sep=";", encoding="utf-8-sig")
        assert len(df_back) == len(df_orig)

    def test_df_vacio_lanza_error(self, tmp_path: Path) -> None:
        df_vacio = pd.DataFrame(columns=COLS_STAGING)
        destino = tmp_path / "staging_quincenas.csv"
        with (
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_CSV",
                destino,
            ),
            patch(
                "src.reportes.nuevas_fuentes" ".writer_quincenas._STAGING_DIR",
                tmp_path,
            ),
        ):
            with pytest.raises(ValueError, match="vacío"):
                escribir_staging_quincenas(df_vacio)
