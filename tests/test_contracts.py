"""
Tests unitarios — Contratos del pipeline POSE_ETL.
Verifica que los dataclasses de contrato sean instanciables
y cumplan los invariantes esperados.
"""
from pathlib import Path
from src.pipeline.contracts.validador_contract import ValidadorOutput
from src.pipeline.contracts.cargador_contract import CargadorOutput
from src.pipeline.contracts.inspector_contract import InspectorOutput
from src.pipeline.contracts.constructor_contract import ConstructorOutput


class TestValidadorOutput:
    def test_ok_sin_errores(self):
        v = ValidadorOutput(validacion_ok=True)
        assert v.validacion_ok is True
        assert v.errores == []
        assert v.advertencias == []
        assert v.registros_validados == 0

    def test_fallo_con_errores(self):
        v = ValidadorOutput(
            validacion_ok=False,
            errores=["Col IMPORTE ausente"],
            registros_validados=5,
        )
        assert v.validacion_ok is False
        assert len(v.errores) == 1
        assert v.registros_validados == 5

    def test_advertencias_independientes_de_validacion(self):
        v = ValidadorOutput(
            validacion_ok=True,
            advertencias=["Hay NaN en OBSERVACION"],
        )
        assert v.validacion_ok is True
        assert len(v.advertencias) == 1


class TestCargadorOutput:
    def test_ok(self):
        c = CargadorOutput(
            run_id="run-001",
            registros_cargados=100,
            estado="OK",
        )
        assert c.estado == "OK"
        assert c.registros_cargados == 100
        assert c.mensaje == ""

    def test_error_con_mensaje(self):
        c = CargadorOutput(
            run_id="run-002",
            registros_cargados=0,
            estado="ERROR",
            mensaje="Timeout conexión PG",
        )
        assert c.estado == "ERROR"
        assert "Timeout" in c.mensaje


class TestInspectorOutput:
    def test_calidad_ok(self):
        i = InspectorOutput(
            excel_path=Path("fuentes/compensaciones/test.xlsx"),
            hash_sha256="abc123",
            headers_mapeados={"OBRA": "OBRA_PRONTO"},
            calidad_ok=True,
        )
        assert i.calidad_ok is True
        assert i.errores == []
        assert i.ya_procesado is False

    def test_calidad_ko_con_errores(self):
        i = InspectorOutput(
            excel_path=Path("fuentes/compensaciones/test.xlsx"),
            hash_sha256="def456",
            headers_mapeados={},
            calidad_ok=False,
            errores=["Columna IMPORTE ausente"],
        )
        assert i.calidad_ok is False
        assert len(i.errores) == 1

    def test_ya_procesado_flag(self):
        i = InspectorOutput(
            excel_path=Path("fuentes/compensaciones/test.xlsx"),
            hash_sha256="abc123",
            headers_mapeados={"OBRA": "OBRA_PRONTO"},
            calidad_ok=True,
            ya_procesado=True,
            periodo="2026-03",
        )
        assert i.ya_procesado is True
        assert i.periodo == "2026-03"


class TestConstructorOutput:
    def test_sin_errores(self):
        c = ConstructorOutput(
            staging_path=Path("output/b52/staging.parquet"),
            registros_ok=200,
            registros_descartados=3,
        )
        assert c.registros_ok == 200
        assert c.errores == []

    def test_con_advertencias(self):
        c = ConstructorOutput(
            staging_path=Path("output/b52/staging.parquet"),
            registros_ok=0,
            registros_descartados=10,
            advertencias=["IMPORTE cero en 10 filas"],
        )
        assert len(c.advertencias) == 1
