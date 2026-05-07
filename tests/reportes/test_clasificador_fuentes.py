"""
test_clasificador_fuentes.py — Tests del módulo clasificador_fuentes
y del orquestador run_todas_fuentes.

Cubre clasificador_fuentes:
  - _es_regex: detecta patron regex vs glob
  - _matchea: regex case-insensitive, glob case-insensitive
  - leer_maestro: filtra ACTIVO=SI, descarta NaN patron
  - clasificar: mapeo correcto, archivo multi-fuente,
    sin matches, solo xlsx, FileNotFoundError

Cubre run_todas_fuentes:
  - _resolver_loockups: common/ > raiz > fallback
  - ejecutar_todas: disparo por runner, captura de errores,
    todos los runners activos, loockups explícito
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.reportes.nuevas_fuentes import (
    clasificador_fuentes as cf,
)
from src.reportes.nuevas_fuentes import (
    run_todas_fuentes as rtf,
)
from src.reportes.nuevas_fuentes._constantes import (
    LOOCKUPS_FILE,
)

# ── Módulo base para patches de run_todas_fuentes ────────────────────────────
_BASE = "src.reportes.nuevas_fuentes.run_todas_fuentes"

# ── Helpers ──────────────────────────────────────────────────────────────────


def _df_maestro_mock() -> pd.DataFrame:
    """DataFrame con estructura de Config_Fuentes para mocks."""
    return pd.DataFrame(
        {
            "FUENTE_DESTINO": [
                "GG_FDL",
                "FACTURACION_FDL",
                "MENSUALES",
                "QUINCENAS",
                "INACTIVA",
            ],
            "PATRON_ARCHIVO": [
                r"\d{2}-\d{4}\.xlsx",
                r"\d{2}-\d{4}\.xlsx",
                r"\d{2}-\d{4}\.xlsx",
                "QUINCENA*.xlsx",
                "BRIC*.xlsx",
            ],
            "HOJAS_A_LEER": [
                "{mes}",
                "{mes}",
                "{tabla:mensuales}",
                "*QUINCENA*",
                "",
            ],
            "ACTIVO": ["SI", "SI", "SI", "SI", "NO"],
            "NOTAS": ["", "", "", "", ""],
        }
    )


def _fuentes_mock() -> list[dict]:
    """Lista de fuentes activas para patch de leer_maestro."""
    return [
        {
            "fuente": "GG_FDL",
            "patron": r"\d{2}-\d{4}\.xlsx",
            "hojas": "{mes}",
            "notas": "",
        },
        {
            "fuente": "FACTURACION_FDL",
            "patron": r"\d{2}-\d{4}\.xlsx",
            "hojas": "{mes}",
            "notas": "",
        },
        {
            "fuente": "MENSUALES",
            "patron": r"\d{2}-\d{4}\.xlsx",
            "hojas": "{tabla:mensuales}",
            "notas": "",
        },
        {
            "fuente": "QUINCENAS",
            "patron": "QUINCENA*.xlsx",
            "hojas": "*QUINCENA*",
            "notas": "",
        },
    ]


# ═══════════════════════════════════════════════════════════════════
# clasificador_fuentes._es_regex
# ═══════════════════════════════════════════════════════════════════


class TestEsRegex:
    def test_backslash_d_es_regex(self) -> None:
        assert cf._es_regex(r"\d{2}-\d{4}\.xlsx") is True

    def test_circunflejo_es_regex(self) -> None:
        assert cf._es_regex(r"^\d+\.xlsx") is True

    def test_dolar_es_regex(self) -> None:
        assert cf._es_regex(r"archivo$") is True

    def test_parentesis_es_regex(self) -> None:
        assert cf._es_regex(r"(foo|bar)\.xlsx") is True

    def test_glob_asterisco_no_es_regex(self) -> None:
        assert cf._es_regex("QUINCENA*.xlsx") is False

    def test_patron_literal_no_es_regex(self) -> None:
        assert cf._es_regex("archivo.xlsx") is False

    def test_interrogacion_es_regex(self) -> None:
        # '?' esta en _RE_TOKEN → se trata como metacar regex
        assert cf._es_regex("archivo?.xlsx") is True


# ═══════════════════════════════════════════════════════════════════
# clasificador_fuentes._matchea
# ═══════════════════════════════════════════════════════════════════


class TestMatchea:
    def test_regex_match_fdl(self) -> None:
        assert cf._matchea("01-2026.xlsx", r"\d{2}-\d{4}\.xlsx") is True

    def test_regex_no_match_quincenas(self) -> None:
        assert (
            cf._matchea(
                "QUINCENAS 01-2026.xlsx",
                r"\d{2}-\d{4}\.xlsx",
            )
            is False
        )

    def test_regex_case_insensitive(self) -> None:
        assert cf._matchea("01-2026.XLSX", r"\d{2}-\d{4}\.xlsx") is True

    def test_glob_match_quincenas(self) -> None:
        assert cf._matchea("QUINCENAS 01-2026.xlsx", "QUINCENA*.xlsx") is True

    def test_glob_no_match_fdl(self) -> None:
        assert cf._matchea("01-2026.xlsx", "QUINCENA*.xlsx") is False

    def test_glob_case_insensitive(self) -> None:
        assert cf._matchea("quincenas 01-2026.xlsx", "QUINCENA*.xlsx") is True

    def test_regex_multi_digitos_no_matchea_tres(self) -> None:
        """111-2026.xlsx no debe matchear \\d{2}-\\d{4}."""
        assert cf._matchea("111-2026.xlsx", r"\d{2}-\d{4}\.xlsx") is False


# ═══════════════════════════════════════════════════════════════════
# clasificador_fuentes.leer_maestro
# ═══════════════════════════════════════════════════════════════════

_MOD_CF = "src.reportes.nuevas_fuentes.clasificador_fuentes"


class TestLeerMaestro:
    def test_filtra_solo_activo_si(self, tmp_path: Path) -> None:
        maestro = tmp_path / "m.xlsx"
        with patch(
            f"{_MOD_CF}.pd.read_excel",
            return_value=_df_maestro_mock(),
        ):
            resultado = cf.leer_maestro(maestro)
        fuentes = [r["fuente"] for r in resultado]
        assert "GG_FDL" in fuentes
        assert "FACTURACION_FDL" in fuentes
        assert "MENSUALES" in fuentes
        assert "QUINCENAS" in fuentes
        assert "INACTIVA" not in fuentes

    def test_cantidad_correcta(self, tmp_path: Path) -> None:
        maestro = tmp_path / "m.xlsx"
        with patch(
            f"{_MOD_CF}.pd.read_excel",
            return_value=_df_maestro_mock(),
        ):
            resultado = cf.leer_maestro(maestro)
        assert len(resultado) == 4

    def test_estructura_dict_correcta(self, tmp_path: Path) -> None:
        maestro = tmp_path / "m.xlsx"
        with patch(
            f"{_MOD_CF}.pd.read_excel",
            return_value=_df_maestro_mock(),
        ):
            resultado = cf.leer_maestro(maestro)
        campos = {"fuente", "patron", "hojas", "notas"}
        assert all(set(r.keys()) >= campos for r in resultado)

    def test_descarta_patron_nan(self, tmp_path: Path) -> None:
        """Filas ACTIVO=SI pero PATRON_ARCHIVO NaN se descartan."""
        df = _df_maestro_mock()
        # Pone NaN en FACTURACION_FDL
        df.loc[df["FUENTE_DESTINO"] == "FACTURACION_FDL", "PATRON_ARCHIVO"] = (
            float("nan")
        )
        maestro = tmp_path / "m.xlsx"
        with patch(
            f"{_MOD_CF}.pd.read_excel",
            return_value=df,
        ):
            resultado = cf.leer_maestro(maestro)
        fuentes = [r["fuente"] for r in resultado]
        assert "FACTURACION_FDL" not in fuentes
        assert "GG_FDL" in fuentes


# ═══════════════════════════════════════════════════════════════════
# clasificador_fuentes.clasificar
# ═══════════════════════════════════════════════════════════════════


class TestClasificar:
    def test_input_raw_no_existe_lanza_error(self, tmp_path: Path) -> None:
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with pytest.raises(FileNotFoundError, match="input_raw"):
            cf.clasificar(
                input_raw=tmp_path / "no_existe",
                maestro=maestro,
            )

    def test_maestro_no_existe_lanza_error(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        with pytest.raises(FileNotFoundError, match="maestro"):
            cf.clasificar(
                input_raw=raw,
                maestro=tmp_path / "no.xlsx",
            )

    def test_mapea_fdl_dos_archivos(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "01-2026.xlsx").touch()
        (raw / "02-2026.xlsx").touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        assert "GG_FDL" in mapa
        assert len(mapa["GG_FDL"]) == 2

    def test_archivo_fdl_mapea_tres_fuentes(self, tmp_path: Path) -> None:
        """01-2026.xlsx debe aparecer en GG_FDL, FACTURACION_FDL
        y MENSUALES simultáneamente."""
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "01-2026.xlsx").touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        assert "GG_FDL" in mapa
        assert "FACTURACION_FDL" in mapa
        assert "MENSUALES" in mapa
        assert "QUINCENAS" not in mapa

    def test_quincenas_no_matchea_patron_fdl(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "QUINCENAS 01-2026.xlsx").touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        assert "QUINCENAS" in mapa
        assert "GG_FDL" not in mapa
        assert "FACTURACION_FDL" not in mapa
        assert "MENSUALES" not in mapa

    def test_sin_archivos_xlsx_retorna_vacio(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "notas.txt").touch()
        (raw / "datos.csv").touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        assert mapa == {}

    def test_ignora_archivos_no_xlsx(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "01-2026.xlsx").touch()
        (raw / "informe.pdf").touch()
        (raw / "datos.csv").touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        for archivos in mapa.values():
            assert all(a.suffix.lower() == ".xlsx" for a in archivos)

    def test_retorna_paths_dentro_de_input_raw(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        xlsx = raw / "01-2026.xlsx"
        xlsx.touch()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch.object(cf, "leer_maestro", return_value=_fuentes_mock()):
            mapa = cf.clasificar(input_raw=raw, maestro=maestro)
        for archivos in mapa.values():
            for a in archivos:
                assert a.parent == raw


# ═══════════════════════════════════════════════════════════════════
# run_todas_fuentes._resolver_loockups
# ═══════════════════════════════════════════════════════════════════


class TestResolverLoockups:
    def test_usa_common_si_existe(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        common = raw / "common"
        common.mkdir(parents=True)
        (common / LOOCKUPS_FILE).touch()
        resultado = rtf._resolver_loockups(raw)
        assert resultado == common / LOOCKUPS_FILE

    def test_fallback_a_raiz_si_no_hay_common(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / LOOCKUPS_FILE).touch()
        resultado = rtf._resolver_loockups(raw)
        assert resultado == raw / LOOCKUPS_FILE

    def test_common_tiene_prioridad_sobre_raiz(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        common = raw / "common"
        common.mkdir(parents=True)
        (common / LOOCKUPS_FILE).touch()
        (raw / LOOCKUPS_FILE).touch()
        resultado = rtf._resolver_loockups(raw)
        assert resultado == common / LOOCKUPS_FILE

    def test_ninguno_existe_devuelve_common(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        resultado = rtf._resolver_loockups(raw)
        assert "common" in str(resultado)
        assert resultado.name == LOOCKUPS_FILE


# ═══════════════════════════════════════════════════════════════════
# run_todas_fuentes.ejecutar_todas
# ═══════════════════════════════════════════════════════════════════


class TestEjecutarTodas:
    def test_retorna_vacio_sin_archivos_clasificados(
        self, tmp_path: Path
    ) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        maestro = tmp_path / "m.xlsx"
        maestro.touch()
        with patch(f"{_BASE}.cf.clasificar", return_value={}):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=maestro,
                loockups_path=tmp_path / "L.xlsx",
            )
        assert resultado == {}

    def test_runner_fdl_si_gg_fdl_en_mapa(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        staging = tmp_path / "staging_fdl.csv"
        staging.touch()
        mapa = {"GG_FDL": [raw / "01-2026.xlsx"]}
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_fdl.ejecutar", return_value=staging
            ) as mock_fdl,
        ):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        mock_fdl.assert_called_once()
        assert resultado["FDL"] == staging

    def test_runner_fdl_si_facturacion_en_mapa(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        staging = tmp_path / "staging_fdl.csv"
        staging.touch()
        mapa = {"FACTURACION_FDL": [raw / "01-2026.xlsx"]}
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_fdl.ejecutar", return_value=staging
            ) as mock_fdl,
        ):
            rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        mock_fdl.assert_called_once()

    def test_fdl_runner_llamado_una_sola_vez_con_ambas_fuentes(
        self, tmp_path: Path
    ) -> None:
        """GG_FDL + FACTURACION_FDL → un solo run_fdl.ejecutar()."""
        raw = tmp_path / "raw"
        raw.mkdir()
        staging = tmp_path / "staging_fdl.csv"
        staging.touch()
        mapa = {
            "GG_FDL": [raw / "01-2026.xlsx"],
            "FACTURACION_FDL": [raw / "01-2026.xlsx"],
        }
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_fdl.ejecutar", return_value=staging
            ) as mock_fdl,
        ):
            rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        assert mock_fdl.call_count == 1

    def test_runner_mensuales(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        staging = tmp_path / "staging_mensuales.csv"
        staging.touch()
        mapa = {"MENSUALES": [raw / "01-2026.xlsx"]}
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_mensuales.ejecutar",
                return_value=staging,
            ) as mock_men,
        ):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        mock_men.assert_called_once()
        assert resultado["MENSUALES"] == staging

    def test_runner_quincenas(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        staging = tmp_path / "staging_quincenas.csv"
        staging.touch()
        mapa = {"QUINCENAS": [raw / "Q01-2026.xlsx"]}
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_quincenas.ejecutar",
                return_value=staging,
            ) as mock_quin,
        ):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        mock_quin.assert_called_once()
        assert resultado["QUINCENAS"] == staging

    def test_error_runner_capturado_no_propaga(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        mapa = {"GG_FDL": [raw / "01-2026.xlsx"]}
        exc = RuntimeError("archivo corrupto")
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(f"{_BASE}.run_fdl.ejecutar", side_effect=exc),
        ):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        assert isinstance(resultado["FDL"], RuntimeError)
        assert "corrupto" in str(resultado["FDL"])

    def test_todos_runners_activos(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        st_fdl = tmp_path / "staging_fdl.csv"
        st_men = tmp_path / "staging_mensuales.csv"
        st_quin = tmp_path / "staging_quincenas.csv"
        mapa = {
            "GG_FDL": [raw / "01-2026.xlsx"],
            "MENSUALES": [raw / "01-2026.xlsx"],
            "QUINCENAS": [raw / "Q01-2026.xlsx"],
        }
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(f"{_BASE}.run_fdl.ejecutar", return_value=st_fdl),
            patch(f"{_BASE}.run_mensuales.ejecutar", return_value=st_men),
            patch(
                f"{_BASE}.run_quincenas.ejecutar",
                return_value=st_quin,
            ),
        ):
            resultado = rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=tmp_path / "L.xlsx",
            )
        assert set(resultado.keys()) == {"FDL", "MENSUALES", "QUINCENAS"}
        assert resultado["FDL"] == st_fdl
        assert resultado["MENSUALES"] == st_men
        assert resultado["QUINCENAS"] == st_quin

    def test_loockups_explicito_no_llama_resolver(
        self, tmp_path: Path
    ) -> None:
        raw = tmp_path / "raw"
        raw.mkdir()
        loockups = tmp_path / "MiLoockups.xlsx"
        staging = tmp_path / "staging_fdl.csv"
        mapa = {"GG_FDL": [raw / "01-2026.xlsx"]}
        with (
            patch(f"{_BASE}.cf.clasificar", return_value=mapa),
            patch(
                f"{_BASE}.run_fdl.ejecutar", return_value=staging
            ) as mock_fdl,
            patch(f"{_BASE}._resolver_loockups") as mock_resolver,
        ):
            rtf.ejecutar_todas(
                input_raw=raw,
                maestro=tmp_path / "m.xlsx",
                loockups_path=loockups,
            )
        mock_resolver.assert_not_called()
        _args, kwargs = mock_fdl.call_args
        assert kwargs["loockups_path"] == loockups
