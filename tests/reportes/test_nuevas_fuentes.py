"""
test_nuevas_fuentes.py — Tests unitarios del módulo nuevas_fuentes.

Cubre:
  - parse_nombre_archivo: nombres válidos e inválidos
  - _normalizar_obra: padding de código numérico y limpieza de comillas
  - _enriquecer (FACTURACION FDL): IMPORTE_USD, constantes, lookup hit/miss
  - _enriquecer (GG FDL): PROVEEDOR=POSE, sin TC, fecha cierre de mes
  - transformar_fdl: integración con mock de pd.read_excel
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.reportes.nuevas_fuentes.run_fdl import (
    _descubrir_archivos,
    ejecutar,
)

from src.reportes.nuevas_fuentes._constantes import (
    COLS_STAGING,
    FACTURACION_FDL_CUENTAS,
)
from src.reportes.nuevas_fuentes.reader_fdl import (
    parse_nombre_archivo,
)
from src.reportes.nuevas_fuentes.reader_mensuales import (
    _PATRON_ETIQUETA,
    _leer_por_hoja_local,
)
from src.reportes.nuevas_fuentes.run_mensuales import (
    _descubrir_archivos as _descubrir_mensuales,
    ejecutar as ejecutar_mensuales,
)
from src.reportes.nuevas_fuentes.transformer_fdl import (
    _enriquecer,
    _normalizar_obra,
    transformar_fdl,
)
from src.reportes.nuevas_fuentes import (  # noqa: E501
    transformar_mensuales,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────


def _df_obras() -> pd.DataFrame:
    """Lookup de obras pequeño para pruebas."""
    return pd.DataFrame(
        {
            "OBRA_PRONTO": ["00000001", "00000002"],
            "DESCRIPCION_OBRA": ["Planta Norte", "Planta Sur"],
            "GERENCIA": ["GER A", "GER B"],
            "COMPENSABLE": ["S", "N"],
        }
    )


def _df_fact_crudo() -> pd.DataFrame:
    """Simula rows FACTURACION FDL de la tabla gg_fdl."""
    return pd.DataFrame(
        {
            "TIPO_EROGACION": ["VENTA DEPTO", "VENTA LOTE"],
            "FECHA": pd.to_datetime(["2026-01-15", "2026-01-20"]),
            "OBRA_PRONTO": ["00000001", "00000099"],
            "SALIDA3": [0.0, 31800.0],
            "ENT2": [52500.0, 0.0],
        }
    )


def _df_gg_crudo() -> pd.DataFrame:
    """Simula rows GG FDL de la tabla gg_fdl."""
    return pd.DataFrame(
        {
            "TIPO_EROGACION": ["OBRA", "OBRA"],
            "FECHA": [
                pd.Timestamp(2026, 1, 31),
                pd.Timestamp(2026, 1, 31),
            ],
            "OBRA_PRONTO": ["00000002", "00000099"],
            "SALIDA3": [150000.0, 75000.0],
            "ENT2": [0.0, 0.0],
        }
    )


def _df_gg_fdl_crudo() -> pd.DataFrame:
    """Simula la tabla completa gg_fdl con ambas fuentes."""
    return pd.DataFrame(
        {
            "TIPO_EROGACION": [
                "VENTA DEPTO",
                "VENTA LOTE",
                "OBRA",
                "OBRA",
            ],
            "FECHA": pd.to_datetime(
                [
                    "2026-01-15",
                    "2026-01-20",
                    "2026-01-31",
                    "2026-01-31",
                ]
            ),
            "OBRA_PRONTO": [
                "00000001",
                "00000099",
                "00000002",
                "00000099",
            ],
            "SALIDA3": [0.0, 31800.0, 150000.0, 75000.0],
            "ENT2": [52500.0, 0.0, 0.0, 0.0],
        }
    )


# ── parse_nombre_archivo ─────────────────────────────────────────────────────


class TestParseNombreArchivo:
    def test_valido_enero(self) -> None:
        mes, anio = parse_nombre_archivo(Path("01-2026.xlsx"))
        assert mes == 1
        assert anio == 2026

    def test_valido_diciembre(self) -> None:
        mes, anio = parse_nombre_archivo(Path("12-2025.xlsx"))
        assert mes == 12
        assert anio == 2025

    def test_invalido_formato(self) -> None:
        with pytest.raises(ValueError, match="Formato esperado"):
            parse_nombre_archivo(Path("enero-2026.xlsx"))

    def test_invalido_extension(self) -> None:
        with pytest.raises(ValueError):
            parse_nombre_archivo(Path("01-2026.xls"))

    def test_invalido_sin_guion(self) -> None:
        with pytest.raises(ValueError):
            parse_nombre_archivo(Path("012026.xlsx"))


# ── _normalizar_obra ─────────────────────────────────────────────────────────


class TestNormalizarObra:
    def test_rellena_ceros_numerica(self) -> None:
        df = pd.DataFrame({"OBRA_PRONTO": ["1", "234"]})
        out = _normalizar_obra(df)
        assert out.iloc[0]["OBRA_PRONTO"] == "00000001"
        assert out.iloc[1]["OBRA_PRONTO"] == "00000234"

    def test_elimina_comilla_inicial(self) -> None:
        df = pd.DataFrame({"OBRA_PRONTO": ["'00000001"]})
        out = _normalizar_obra(df)
        assert out.iloc[0]["OBRA_PRONTO"] == "00000001"

    def test_no_alfa_sin_cambio(self) -> None:
        df = pd.DataFrame({"OBRA_PRONTO": ["OBR-001"]})
        out = _normalizar_obra(df)
        assert out.iloc[0]["OBRA_PRONTO"] == "OBR-001"


# ── _enriquecer (FACTURACION FDL) ────────────────────────────────────────────


class TestEnriquecerFacturacion:
    def test_columnas_staging_completas(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        assert list(df.columns) == COLS_STAGING

    def test_lookup_hit_gerencia(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000001"].iloc[0]
        assert fila["GERENCIA"] == "GER A"
        assert fila["COMPENSABLE"] == "S"

    def test_lookup_miss_gerencia_sin_obra_asignada(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000099"].iloc[0]
        # Obra sin alta → gerencia fija "SIN OBRA ASIGNADA"
        assert fila["GERENCIA"] == "SIN OBRA ASIGNADA"
        assert pd.isna(fila["COMPENSABLE"])

    def test_importe_ent2_menos_salida3(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000001"].iloc[0]
        # ENT2=52500, SALIDA3=0 → 52500 - 0 = 52500
        assert float(fila["IMPORTE*"]) == pytest.approx(52500.0)

    def test_importe_negativo(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000099"].iloc[0]
        # ENT2=0, SALIDA3=31800 → 0 - 31800 = -31800
        assert float(fila["IMPORTE*"]) < 0

    def test_tc_e_importe_usd_none(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        assert df["TC"].isna().all()
        assert df["IMPORTE USD"].isna().all()

    def test_cuentas_venta_depto(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000001"].iloc[0]
        cuentas = FACTURACION_FDL_CUENTAS["VENTA DEPTO"]
        assert fila["RUBRO CONTABLE*"] == cuentas["RUBRO_CONTABLE"]
        assert fila["CUENTA CONTABLE*"] == cuentas["CUENTA_CONTABLE"]
        assert fila["CODIGO CUENTA*"] == cuentas["CODIGO_CUENTA"]

    def test_cuentas_venta_lote(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000099"].iloc[0]
        cuentas = FACTURACION_FDL_CUENTAS["VENTA LOTE"]
        assert fila["RUBRO CONTABLE*"] == cuentas["RUBRO_CONTABLE"]
        assert fila["CUENTA CONTABLE*"] == cuentas["CUENTA_CONTABLE"]

    def test_fuente_correcta(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        assert (df["FUENTE*"] == "FACTURACION FDL").all()

    def test_mes_en_espanol(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        assert (df["MES"] == "Enero").all()

    def test_detalle_dinamico_depto(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000001"].iloc[0]
        assert "Cobranza Deptos." in str(fila["DETALLE*"])

    def test_proveedor_pose(self) -> None:
        df = _enriquecer(_df_fact_crudo(), _df_obras())
        assert (df["PROVEEDOR*"] == "POSE").all()


# ── _enriquecer (GG FDL) ─────────────────────────────────────────────────────


class TestEnriquecerGG:
    def test_proveedor_pose(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert (df["PROVEEDOR*"] == "POSE").all()

    def test_sin_tc_ni_importe_usd(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert df["TC"].isna().all()
        assert df["IMPORTE USD"].isna().all()

    def test_fecha_ultimo_dia_mes(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert (df["FECHA*"] == "2026-01-31").all()

    def test_lookup_hit_desc_obra(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000002"].iloc[0]
        assert fila["DESCRIPCION OBRA"] == "Planta Sur"
        assert fila["GERENCIA"] == "GER B"

    def test_lookup_miss_desc_obra_sin_alta(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000099"].iloc[0]
        # Obra sin alta → descripcion = obra, gerencia = "SIN OBRA ASIGNADA"
        assert fila["DESCRIPCION OBRA"] == "00000099"
        assert fila["GERENCIA"] == "SIN OBRA ASIGNADA"

    def test_cuentas_gg_correctas(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert (df["RUBRO CONTABLE*"] == "Gastos Generales").all()
        assert (df["CUENTA CONTABLE*"] == "GASTOS GENERALES").all()
        assert (df["CODIGO CUENTA*"] == 511121300).all()

    def test_fuente_correcta(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert (df["FUENTE*"] == "GG FDL").all()

    def test_importe_negado(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        fila = df[df["OBRA PRONTO*"] == "00000002"].iloc[0]
        # SALIDA3=150000 → IMPORTE = -150000
        assert float(fila["IMPORTE*"]) == pytest.approx(-150000.0)

    def test_detalle_gastos_generales(self) -> None:
        df = _enriquecer(_df_gg_crudo(), _df_obras())
        assert "Gastos Generales" in str(df["DETALLE*"].iloc[0])


# ── transformar_fdl (integración — mocks a nivel de función) ─────────────────

_BASE_PATH = "src.reportes.nuevas_fuentes.transformer_fdl."


class TestTransformarFdl:
    def test_staging_tiene_cols_correctas(self) -> None:
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _BASE_PATH + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _BASE_PATH + "leer_tabla_gg_fdl",
                return_value=_df_gg_fdl_crudo(),
            ),
        ):
            result = transformar_fdl([archivo], Path("Loockups.xlsx"))

        assert list(result.columns) == COLS_STAGING

    def test_staging_sin_archivos_vacio(self) -> None:
        with patch(
            _BASE_PATH + "_leer_obras_gerencias",
            return_value=_df_obras(),
        ):
            result = transformar_fdl([], Path("Loockups.xlsx"))

        assert result.empty
        assert list(result.columns) == COLS_STAGING

    def test_loockups_no_encontrado(self) -> None:
        with patch.object(Path, "exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Loockups"):
                transformar_fdl([], Path("no_existe.xlsx"))

    def test_fuentes_concatenadas(self) -> None:
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _BASE_PATH + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _BASE_PATH + "leer_tabla_gg_fdl",
                return_value=_df_gg_fdl_crudo(),
            ),
        ):
            result = transformar_fdl([archivo], Path("Loockups.xlsx"))

        fuentes = set(result["FUENTE*"].unique())
        assert "FACTURACION FDL" in fuentes
        assert "GG FDL" in fuentes

    def test_staging_tiene_n_filas_correctas(self) -> None:
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _BASE_PATH + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _BASE_PATH + "leer_tabla_gg_fdl",
                return_value=_df_gg_fdl_crudo(),  # 4 filas: 2 VENTA + 2 OBRA
            ),
        ):
            result = transformar_fdl([archivo], Path("Loockups.xlsx"))

        assert len(result) == 4


# ── _enriquecer (GG FDL — CENTRO_COSTO → OBRA_PRONTO) ────────────────────────


def _cc_obra_map() -> dict[str, str]:
    """Mapeo reducido CC → obra para pruebas."""
    return {
        "SEDE": "00000302",
        "ACTIVOS": "00000310",
        "PEHUEN": "00000451",
    }


def _df_gg_obra_vacia() -> pd.DataFrame:
    """Fila OBRA sin OBRA_PRONTO pero con CENTRO_COSTO=SEDE."""
    return pd.DataFrame(
        {
            "TIPO_EROGACION": ["OBRA"],
            "FECHA": [pd.Timestamp(2026, 1, 31)],
            "OBRA_PRONTO": [""],
            "CENTRO_COSTO": ["SEDE"],
            "SALIDA3": [90000.0],
            "ENT2": [0.0],
        }
    )


class TestEnriquecerGGCentroCosto:
    """Verifica que _enriquecer resuelva OBRA_PRONTO via CENTRO_COSTO."""

    def test_obra_vacia_resuelta_por_centro_costo(self) -> None:
        df = _enriquecer(_df_gg_obra_vacia(), _df_obras(), _cc_obra_map())
        assert df.iloc[0]["OBRA PRONTO*"] == "00000302"

    def test_obra_vacia_sin_cc_obra_gerencia_sin_asignada(
        self,
    ) -> None:
        df = _enriquecer(_df_gg_obra_vacia(), _df_obras(), {})
        assert df.iloc[0]["GERENCIA"] == "SIN OBRA ASIGNADA"

    def test_obra_vacia_cc_desconocido_gerencia_sin_asignada(
        self,
    ) -> None:
        df = _enriquecer(
            _df_gg_obra_vacia(),
            _df_obras(),
            {"OFICINA": "00000999"},
        )
        assert df.iloc[0]["GERENCIA"] == "SIN OBRA ASIGNADA"

    def test_importe_negado_con_resolucion(self) -> None:
        df = _enriquecer(_df_gg_obra_vacia(), _df_obras(), _cc_obra_map())
        # SALIDA3=90000 → IMPORTE = -90000
        assert float(df.iloc[0]["IMPORTE*"]) == pytest.approx(-90000.0)


# ── TestRunFdl ───────────────────────────────────────────────────────────────

_RUN_FDL = "src.reportes.nuevas_fuentes.run_fdl."


class TestRunFdl:
    """Tests del módulo run_fdl: descubrimiento de archivos y orquestador."""

    def test_descubrir_todos(self, tmp_path: Path) -> None:
        """Encuentra solo archivos MM-YYYY.xlsx, ignora el resto."""
        (tmp_path / "01-2026.xlsx").touch()
        (tmp_path / "02-2026.xlsx").touch()
        (tmp_path / "otro.xlsx").touch()
        result = _descubrir_archivos(tmp_path)
        assert len(result) == 2

    def test_descubrir_ordenados(self, tmp_path: Path) -> None:
        """Los archivos se devuelven ordenados alfabéticamente."""
        (tmp_path / "03-2026.xlsx").touch()
        (tmp_path / "01-2026.xlsx").touch()
        result = _descubrir_archivos(tmp_path)
        assert result[0].name == "01-2026.xlsx"
        assert result[1].name == "03-2026.xlsx"

    def test_descubrir_por_periodo(self, tmp_path: Path) -> None:
        """Filtra correctamente por año."""
        (tmp_path / "01-2026.xlsx").touch()
        (tmp_path / "01-2025.xlsx").touch()
        result = _descubrir_archivos(tmp_path, periodo="2026")
        assert len(result) == 1
        assert "2026" in result[0].name

    def test_descubrir_por_nombre(self, tmp_path: Path) -> None:
        """Devuelve el archivo exacto si se pasa nombre."""
        (tmp_path / "01-2026.xlsx").touch()
        result = _descubrir_archivos(tmp_path, nombre="01-2026.xlsx")
        assert result[0].name == "01-2026.xlsx"

    def test_descubrir_nombre_no_existe(self, tmp_path: Path) -> None:
        """Lanza FileNotFoundError si el archivo no existe."""
        with pytest.raises(FileNotFoundError, match="Archivo no encontrado"):
            _descubrir_archivos(tmp_path, nombre="99-2099.xlsx")

    def test_ejecutar_staging_generado(self, tmp_path: Path) -> None:
        """ejecutar llama a transformar_fdl y escribir_staging_fdl."""
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        (input_raw / "01-2026.xlsx").touch()
        loockups = input_raw / "Loockups.xlsx"
        loockups.touch()
        staging_out = tmp_path / "staging_fdl.csv"

        with (
            patch(
                _RUN_FDL + "transformar_fdl",
                return_value=pd.DataFrame({"col": [1]}),
            ) as m_tr,
            patch(
                _RUN_FDL + "escribir_staging_fdl",
                return_value=staging_out,
            ) as m_wr,
        ):
            ruta = ejecutar(input_raw=input_raw, loockups_path=loockups)

        assert ruta == staging_out
        m_tr.assert_called_once()
        m_wr.assert_called_once()

    def test_ejecutar_sin_archivos_falla(self, tmp_path: Path) -> None:
        """Lanza FileNotFoundError si no hay archivos FDL."""
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        loockups = input_raw / "Loockups.xlsx"
        loockups.touch()
        with pytest.raises(FileNotFoundError, match="No se encontraron"):
            ejecutar(input_raw=input_raw, loockups_path=loockups)

    def test_ejecutar_sin_loockups_falla(self, tmp_path: Path) -> None:
        """Lanza FileNotFoundError si Loockups.xlsx no existe."""
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        (input_raw / "01-2026.xlsx").touch()
        with pytest.raises(FileNotFoundError, match="Loockups no encontrado"):
            ejecutar(
                input_raw=input_raw,
                loockups_path=tmp_path / "no_existe.xlsx",
            )


# ── TestReaderMensuales ──────────────────────────────────────────────────────


_RUN_MENSUALES = "src.reportes.nuevas_fuentes.run_mensuales."
_TRANS_MENSUALES = "src.reportes.nuevas_fuentes.transformer_mensuales."


class TestPatronEtiqueta:
    """Verifica la regex que separa código de descripción."""

    def test_etiqueta_valida(self) -> None:
        m = _PATRON_ETIQUETA.match("00000004 TALLER NUEVO")
        assert m is not None
        assert m.group(1) == "00000004"
        assert m.group(2) == "TALLER NUEVO"

    def test_etiqueta_espacios_internos(self) -> None:
        m = _PATRON_ETIQUETA.match("00000019 ADIF LIC 28 NORTE")
        assert m is not None
        assert m.group(1) == "00000019"
        assert m.group(2) == "ADIF LIC 28 NORTE"

    def test_etiqueta_sin_codigo(self) -> None:
        """Etiqueta sin 8 dígitos iniciales → no matchea."""
        assert _PATRON_ETIQUETA.match("TALLER NUEVO") is None

    def test_etiqueta_codigo_corto(self) -> None:
        """Código con menos de 8 dígitos → no matchea."""
        assert _PATRON_ETIQUETA.match("0004 TALLER") is None

    def test_etiqueta_solo_codigo(self) -> None:
        """Solo el código sin descripción → no matchea
        (requiere al menos un char después del espacio)."""
        assert _PATRON_ETIQUETA.match("00000004 ") is None


class TestLeerPorHojaLocal:
    """Tests para _leer_por_hoja_local (lectura de copias locales)."""

    def test_columna_ausente_etiquetas(self, tmp_path: Path) -> None:
        """Sin columna 'Etiquetas de fila' → ValueError claro."""
        import openpyxl

        xlsx = tmp_path / "01-2026.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "1-26"
        ws.append(["OTRA COLUMNA", "TOTAL COSTO"])
        wb.save(xlsx)

        with pytest.raises(ValueError, match="Etiquetas de fila"):
            _leer_por_hoja_local(xlsx, mes=1)

    def test_hoja_no_existe(self, tmp_path: Path) -> None:
        """Hoja 'M-26' inexistente → ValueError con nombre de hoja."""
        import openpyxl

        xlsx = tmp_path / "01-2026.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        wb.save(xlsx)

        with pytest.raises(ValueError, match="1-26"):
            _leer_por_hoja_local(xlsx, mes=1)


class TestTransformarMensuales:
    """Tests de integración para transformar_mensuales (con mocks)."""

    def _df_mensuales_crudo(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "OBRA_PRONTO": ["00000001", "00000099"],
                "DESCRIPCION_OBRA": ["Planta Norte", "Desconocida"],
                "TOTAL_COSTO": [50000.0, 20000.0],
            }
        )

    def test_importe_negativo(self, tmp_path: Path) -> None:
        """IMPORTE* = TOTAL_COSTO * -1."""
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        assert (df["IMPORTE*"] < 0).all()
        assert df.loc[df["OBRA PRONTO*"] == "00000001", "IMPORTE*"].iloc[
            0
        ] == pytest.approx(-50000.0)

    def test_constantes_mensuales(self, tmp_path: Path) -> None:
        """FUENTE*, CUENTA*, RUBRO* y PROVEEDOR* son fijos."""
        archivo = Path("02-2026.xlsx")
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        assert (df["FUENTE*"] == "MENSUALES").all()
        assert (df["PROVEEDOR*"] == "POSE").all()
        assert (
            df["RUBRO CONTABLE*"] == "Sueldos, Jornales y Cargas Sociales"
        ).all()
        assert (df["CUENTA CONTABLE*"] == "SUELDOS Y JORNALES").all()
        assert (df["CODIGO CUENTA*"] == 511200002).all()

    def test_fecha_fin_de_mes(self) -> None:
        """FECHA* = último día del mes del nombre de archivo."""
        archivo = Path("02-2026.xlsx")  # febrero
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        assert (df["FECHA*"] == "2026-02-28").all()

    def test_lookup_gerencia_resuelve(self) -> None:
        """Para obra conocida: GERENCIA y COMPENSABLE se inyectan."""
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        fila = df[df["OBRA PRONTO*"] == "00000001"].iloc[0]
        assert fila["GERENCIA"] == "GER A"
        assert fila["COMPENSABLE"] == "S"

    def test_lookup_miss_usa_desc_local(self) -> None:
        """Para obra desconocida: DESCRIPCION OBRA viene de la col local."""
        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        fila = df[df["OBRA PRONTO*"] == "00000099"].iloc[0]
        assert fila["DESCRIPCION OBRA"] == "Desconocida"

    def test_cols_staging_completas(self) -> None:
        """El staging tiene exactamente COLS_STAGING."""
        from src.reportes.nuevas_fuentes._constantes import (
            COLS_STAGING,
        )

        archivo = Path("01-2026.xlsx")
        with (
            patch(
                _TRANS_MENSUALES + "_leer_obras_gerencias",
                return_value=_df_obras(),
            ),
            patch(
                _TRANS_MENSUALES + "leer_tabla_mensuales",
                return_value=self._df_mensuales_crudo(),
            ),
        ):
            df = transformar_mensuales([archivo], Path("Loockups.xlsx"))

        assert list(df.columns) == COLS_STAGING

    def test_staging_vacio_sin_archivos(self) -> None:
        """Sin archivos → DataFrame vacío con COLS_STAGING."""
        from src.reportes.nuevas_fuentes._constantes import (
            COLS_STAGING,
        )

        with patch(
            _TRANS_MENSUALES + "_leer_obras_gerencias",
            return_value=_df_obras(),
        ):
            df = transformar_mensuales([], Path("Loockups.xlsx"))

        assert df.empty
        assert list(df.columns) == COLS_STAGING


class TestRunMensuales:
    """Tests del orquestador run_mensuales."""

    def test_descubrir_todos(self, tmp_path: Path) -> None:
        (tmp_path / "01-2026.xlsx").touch()
        (tmp_path / "02-2026.xlsx").touch()
        (tmp_path / "otro.xlsx").touch()
        result = _descubrir_mensuales(tmp_path)
        assert len(result) == 2

    def test_descubrir_por_periodo(self, tmp_path: Path) -> None:
        (tmp_path / "01-2026.xlsx").touch()
        (tmp_path / "01-2025.xlsx").touch()
        result = _descubrir_mensuales(tmp_path, periodo="2026")
        assert len(result) == 1
        assert "2026" in result[0].name

    def test_ejecutar_sin_archivos_falla(self, tmp_path: Path) -> None:
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        loockups = input_raw / "Loockups.xlsx"
        loockups.touch()
        with pytest.raises(
            FileNotFoundError, match="No se encontraron archivos MENSUALES"
        ):
            ejecutar_mensuales(input_raw=input_raw, loockups_path=loockups)

    def test_ejecutar_sin_loockups_falla(self, tmp_path: Path) -> None:
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        (input_raw / "01-2026.xlsx").touch()
        with pytest.raises(FileNotFoundError, match="Loockups no encontrado"):
            ejecutar_mensuales(
                input_raw=input_raw,
                loockups_path=tmp_path / "no_existe.xlsx",
            )

    def test_ejecutar_staging_generado(self, tmp_path: Path) -> None:
        """ejecutar llama a transformar_mensuales y escribir_staging."""
        input_raw = tmp_path / "input_raw"
        input_raw.mkdir()
        (input_raw / "01-2026.xlsx").touch()
        loockups = input_raw / "Loockups.xlsx"
        loockups.touch()
        staging_out = tmp_path / "staging_mensuales.csv"

        with (
            patch(
                _RUN_MENSUALES + "transformar_mensuales",
                return_value=pd.DataFrame({"col": [1]}),
            ) as m_tr,
            patch(
                _RUN_MENSUALES + "escribir_staging_mensuales",
                return_value=staging_out,
            ) as m_wr,
        ):
            ruta = ejecutar_mensuales(
                input_raw=input_raw, loockups_path=loockups
            )

        assert ruta == staging_out
        m_tr.assert_called_once()
        m_wr.assert_called_once()
