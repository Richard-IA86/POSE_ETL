"""
Tests unitarios — Bifurcador B52.

Cubre: hasher.py, lector.py, escritor_csv.py, bifurcador.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from ETL_BaseA2.src.bifurcador.hasher import (
    calcular_hashes,
    cargar_hashes_anterior,
    clasificar_estado,
    guardar_hashes,
    resumen_estados,
)
from ETL_BaseA2.src.bifurcador.escritor_csv import escribir_csv
from ETL_BaseA2.src.bifurcador.lector import resumen_lectura

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _df_base() -> pd.DataFrame:
    data: dict[str, list[Any]] = {
        "OBRA_PRONTO": ["OBR001", "OBR002", "OBR003"],
        "DESCRIPCION_OBRA": ["Obra A", "Obra B", "Obra C"],
        "FECHA": ["01/03/2025", "15/04/2025", "20/05/2025"],
        "FUENTE": ["COMP", "COMP", "COMP"],
        "TIPO_COMPROBANTE": ["FAC", "FAC", "REC"],
        "NRO_COMPROBANTE": ["0001", "0002", "0003"],
        "PROVEEDOR": ["Prov X", "Prov Y", "Prov Z"],
        "DETALLE": ["Det A", "Det B", "Det C"],
        "CODIGO_CUENTA": ["410", "420", "430"],
        "IMPORTE": [1000.0, 2500.5, 800.0],
        "OBSERVACION": ["", "", ""],
        "RUBRO_CONTABLE": ["MANO_OBRA", "MANO_OBRA", "MATERIAL"],
        "CUENTA_CONTABLE": ["4101", "4201", "4301"],
        "ANIO": [2025, 2025, 2025],
        "MES": [3, 4, 5],
    }
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# hasher.py
# ---------------------------------------------------------------------------


class TestCalcularHashes:
    def test_agrega_columnas_hash(self) -> None:
        df = calcular_hashes(_df_base())
        assert "_hash_fila" in df.columns
        assert "_hash_importe" in df.columns

    def test_hash_fila_es_string_64_chars(self) -> None:
        df = calcular_hashes(_df_base())
        for h in df["_hash_fila"]:
            assert isinstance(h, str)
            assert len(h) == 64

    def test_hash_importe_distinto_del_hash_fila(self) -> None:
        df = calcular_hashes(_df_base())
        for hf, hi in zip(df["_hash_fila"], df["_hash_importe"]):
            assert hf != hi

    def test_mismo_registro_mismo_hash(self) -> None:
        df1 = calcular_hashes(_df_base())
        df2 = calcular_hashes(_df_base())
        assert list(df1["_hash_fila"]) == list(df2["_hash_fila"])

    def test_importe_diferente_cambia_hash_importe(self) -> None:
        df = _df_base().copy()
        df2 = df.copy()
        df2.loc[0, "IMPORTE"] = 9999.99
        h1 = calcular_hashes(df)
        h2 = calcular_hashes(df2)
        # hash_fila igual (misma clave)
        assert h1.loc[0, "_hash_fila"] == h2.loc[0, "_hash_fila"]
        # hash_importe distinto
        assert h1.loc[0, "_hash_importe"] != h2.loc[0, "_hash_importe"]

    def test_columna_ausente_no_rompe(self) -> None:
        df = _df_base().drop(columns=["TIPO_COMPROBANTE"])
        df_h = calcular_hashes(df)
        assert "_hash_fila" in df_h.columns


class TestClasificarEstado:
    def test_primera_corrida_todo_nuevo(self) -> None:
        df = calcular_hashes(_df_base())
        df = clasificar_estado(df, {})
        assert (df["_estado_carga"] == "NUEVO").all()

    def test_segunda_corrida_sin_cambio(self) -> None:
        df = calcular_hashes(_df_base())
        hashes = dict(zip(df["_hash_fila"], df["_hash_importe"]))
        df = clasificar_estado(df, hashes)
        assert (df["_estado_carga"] == "SIN_CAMBIO").all()

    def test_importe_modificado(self) -> None:
        df_original = calcular_hashes(_df_base())
        hashes = dict(
            zip(df_original["_hash_fila"], df_original["_hash_importe"])
        )
        df_mod = _df_base().copy()
        df_mod.loc[0, "IMPORTE"] = 9999.99
        df_mod = calcular_hashes(df_mod)
        df_mod = clasificar_estado(df_mod, hashes)
        assert df_mod.loc[0, "_estado_carga"] == "MODIFICADO"
        assert df_mod.loc[1, "_estado_carga"] == "SIN_CAMBIO"

    def test_resumen_estados(self) -> None:
        df = calcular_hashes(_df_base())
        df = clasificar_estado(df, {})
        resumen = resumen_estados(df)
        assert resumen["NUEVO"] == 3
        assert resumen["SIN_CAMBIO"] == 0
        assert resumen["MODIFICADO"] == 0


class TestPersistirHashes:
    def test_guardar_y_cargar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ruta = os.path.join(tmpdir, "hashes.csv")
            df = calcular_hashes(_df_base())
            guardar_hashes(df, ruta)
            recuperado = cargar_hashes_anterior(ruta)
            assert len(recuperado) == 3
            for hf in df["_hash_fila"]:
                assert hf in recuperado

    def test_carga_archivo_inexistente(self) -> None:
        resultado = cargar_hashes_anterior("/tmp/no_existe_jamás.csv")
        assert resultado == {}


# ---------------------------------------------------------------------------
# escritor_csv.py
# ---------------------------------------------------------------------------


class TestEscritorCsv:
    def test_genera_dos_archivos(self) -> None:
        df = calcular_hashes(_df_base())
        df = clasificar_estado(df, {})
        with tempfile.TemporaryDirectory() as tmpdir:
            resultado = escribir_csv(df, directorio=tmpdir, ts="test")
            assert Path(resultado["completo"]).exists()
            assert Path(resultado["delta"]).exists()

    def test_delta_solo_nuevo_y_modificado(self) -> None:
        df_orig = calcular_hashes(_df_base())
        hashes = dict(zip(df_orig["_hash_fila"], df_orig["_hash_importe"]))
        df_mod = _df_base().copy()
        df_mod.loc[0, "IMPORTE"] = 9999.99
        df_mod = calcular_hashes(df_mod)
        df_mod = clasificar_estado(df_mod, hashes)
        with tempfile.TemporaryDirectory() as tmpdir:
            resultado = escribir_csv(df_mod, directorio=tmpdir, ts="t2")
            assert int(resultado["filas_delta"]) == 1
            df_delta = pd.read_csv(resultado["delta"], sep="|")
            assert (df_delta["_estado_carga"] != "SIN_CAMBIO").all()

    def test_completo_tiene_todas_las_filas(self) -> None:
        df = calcular_hashes(_df_base())
        df = clasificar_estado(df, {})
        with tempfile.TemporaryDirectory() as tmpdir:
            resultado = escribir_csv(df, directorio=tmpdir, ts="t3")
            assert int(resultado["filas_total"]) == 3


# ---------------------------------------------------------------------------
# lector.py (solo resumen_lectura — leer_base_costos requiere Excel real)
# ---------------------------------------------------------------------------


class TestResumenLectura:
    def test_resumen_basico(self) -> None:
        df = _df_base()
        r = resumen_lectura(df)
        assert r["filas"] == 3
        assert r["importe_total"] == pytest.approx(4300.5)
        assert r["anio_min"] == 2025
        assert r["anio_max"] == 2025
