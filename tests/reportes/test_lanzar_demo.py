"""Tests unitarios para lanzar_demo.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import src.reportes.lanzar_demo as dem


# ── _listar_archivos ──────────────────────────────────────────
class TestListarArchivos:
    def test_vacio(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        assert dem._listar_archivos("CUENTA POSE*.xlsx") == []

    def test_con_archivos(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "CUENTA POSE 01-2026.xlsx").touch()
        (tmp_path / "CUENTA POSE 02-2026.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        result = dem._listar_archivos("CUENTA POSE*.xlsx")
        assert len(result) == 2

    def test_resultado_es_ordenado(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "b.xlsx").touch()
        (tmp_path / "a.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        result = dem._listar_archivos("*.xlsx")
        assert result[0].name == "a.xlsx"


# ── _seleccionar_archivo ──────────────────────────────────────
class TestSeleccionarArchivo:
    def _archivos(self, tmp_path: Path) -> list[Path]:
        a = tmp_path / "a.xlsx"
        b = tmp_path / "b.xlsx"
        a.touch()
        b.touch()
        return [a, b]

    def test_cero_devuelve_none(self, tmp_path: Path) -> None:
        archivos = self._archivos(tmp_path)
        with patch("builtins.input", return_value="0"):
            assert dem._seleccionar_archivo(archivos) is None

    def test_indice_valido(self, tmp_path: Path) -> None:
        archivos = self._archivos(tmp_path)
        with patch("builtins.input", return_value="1"):
            assert dem._seleccionar_archivo(archivos) == archivos[0]

    def test_segundo_elemento(self, tmp_path: Path) -> None:
        archivos = self._archivos(tmp_path)
        with patch("builtins.input", return_value="2"):
            assert dem._seleccionar_archivo(archivos) == archivos[1]

    def test_fuera_de_rango(self, tmp_path: Path) -> None:
        archivos = self._archivos(tmp_path)
        with patch("builtins.input", return_value="99"):
            assert dem._seleccionar_archivo(archivos) is None

    def test_no_numerico(self, tmp_path: Path) -> None:
        archivos = self._archivos(tmp_path)
        with patch("builtins.input", return_value="abc"):
            assert dem._seleccionar_archivo(archivos) is None


# ── _fecha_clave ──────────────────────────────────────────────
class TestFechaClave:
    def _p(self, nombre: str) -> Path:
        return Path(nombre)

    def test_fecha_valida(self) -> None:
        assert dem._fecha_clave(self._p("CUENTA POSE 03-2026")) == 202603

    def test_fecha_diciembre(self) -> None:
        assert dem._fecha_clave(self._p("CUENTA POSE 12-2024")) == 202412

    def test_sin_fecha_retorna_cero(self) -> None:
        assert dem._fecha_clave(self._p("sin_fecha")) == 0


# ── _mejor_archivo_despachos ──────────────────────────────────
class TestMejorArchivoDespachos:
    def test_vacio(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        assert dem._mejor_archivo_despachos() is None

    def test_retorna_mas_reciente(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "CUENTA POSE 01-2025.xlsx").touch()
        (tmp_path / "CUENTA POSE 12-2024.xlsx").touch()
        (tmp_path / "CUENTA POSE 03-2026.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        result = dem._mejor_archivo_despachos()
        assert result is not None
        assert "03-2026" in result.name

    def test_filtro_anio(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "CUENTA POSE 01-2025.xlsx").touch()
        (tmp_path / "CUENTA POSE 03-2026.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        result = dem._mejor_archivo_despachos(anio="2025")
        assert result is not None
        assert "2025" in result.name

    def test_filtro_anio_sin_resultados(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "CUENTA POSE 01-2025.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        assert dem._mejor_archivo_despachos(anio="2099") is None

    def test_mismo_anio_elige_mes_mayor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "CUENTA POSE 01-2026.xlsx").touch()
        (tmp_path / "CUENTA POSE 11-2026.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        result = dem._mejor_archivo_despachos()
        assert result is not None
        assert "11-2026" in result.name


# ── _verificar_lookups ────────────────────────────────────────
class TestVerificarLookups:
    def test_sin_archivo(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        assert dem._verificar_lookups() is False

    def test_con_archivo(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "Loockups.xlsx").touch()
        monkeypatch.setattr(dem, "INPUT_RAW", tmp_path)
        assert dem._verificar_lookups() is True


# ── _cmd ──────────────────────────────────────────────────────
class TestCmd:
    def test_retorna_true_en_exitcode_0(self) -> None:
        mock_result = MagicMock(returncode=0)
        with patch("subprocess.run", return_value=mock_result) as m:
            assert dem._cmd(["python", "--version"]) is True
            m.assert_called_once()

    def test_retorna_false_en_exitcode_1(self) -> None:
        mock_result = MagicMock(returncode=1)
        with patch("subprocess.run", return_value=mock_result):
            assert dem._cmd(["python", "-c", "raise Exception()"]) is False

    def test_usa_repo_root_como_cwd(self) -> None:
        mock_result = MagicMock(returncode=0)
        with patch("subprocess.run", return_value=mock_result) as m:
            dem._cmd(["python", "--version"])
            _, kwargs = m.call_args
            assert kwargs.get("cwd") == str(dem.REPO_ROOT)


# ── Rutas base ────────────────────────────────────────────────
class TestRutasBase:
    def test_repo_root_es_absoluta(self) -> None:
        assert dem.REPO_ROOT.is_absolute()

    def test_input_raw_bajo_report_gerencias(self) -> None:
        assert "report_gerencias" in str(dem.INPUT_RAW)

    def test_staging_es_csv(self) -> None:
        assert dem.STAGING.suffix == ".csv"

    def test_parquet_es_parquet(self) -> None:
        assert dem.PARQUET.suffix == ".parquet"

    def test_app_es_app_director(self) -> None:
        assert dem.APP.suffix == ".py"
        assert "app_director" in dem.APP.name


# ── main() ────────────────────────────────────────────────────
class TestMain:
    def test_sale_con_opcion_0(self) -> None:
        with patch("builtins.input", return_value="0"):
            dem.main()

    def test_opciones_invalidas_no_rompen(self) -> None:
        respuestas = iter(["X", "Z", "9", "0"])
        with patch("builtins.input", side_effect=respuestas):
            dem.main()
