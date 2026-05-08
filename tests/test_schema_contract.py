"""
Tests unitarios — schema_contract
(planif_pose/src/normalizador/schema_contract.py)

Cubre:
  - COLUMNAS_CANONICAS definidas y no vacías
  - aplicar_schema_contract: columnas faltantes se añaden
  - aplicar_schema_contract: columnas extra se descartan
  - aplicar_schema_contract: orden canónico respetado
  - aplicar_schema_contract: _ID_INGESTA se preserva
  - aplicar_schema_contract: df None devuelve None
  - aplicar_schema_contract: df vacío devuelve vacío
"""

import pandas as pd

from schema_contract import (
    COLUMNAS_CANONICAS,
    aplicar_schema_contract,
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------


class TestColumnasCanonnicas:
    def test_lista_no_vacia(self):
        assert len(COLUMNAS_CANONICAS) > 0

    def test_columnas_criticas_presentes(self):
        criticas = {"OBRA_PRONTO", "FECHA", "FUENTE", "IMPORTE"}
        assert criticas.issubset(set(COLUMNAS_CANONICAS))

    def test_auditoria_al_final(self):
        idx_auditoria = [
            i for i, c in enumerate(COLUMNAS_CANONICAS) if c.startswith("_")
        ]
        idx_negocio = [
            i
            for i, c in enumerate(COLUMNAS_CANONICAS)
            if not c.startswith("_")
        ]
        if idx_auditoria and idx_negocio:
            assert min(idx_auditoria) > max(idx_negocio)


# ---------------------------------------------------------------------------
# aplicar_schema_contract
# ---------------------------------------------------------------------------


def _df_completo():
    """DataFrame con todas las columnas canónicas más _ID_INGESTA."""
    data = {c: [None] for c in COLUMNAS_CANONICAS}
    data["_ID_INGESTA"] = [99]
    return pd.DataFrame(data)


class TestAplicarSchemaContract:
    def test_df_none_retorna_none(self):
        result, informe = aplicar_schema_contract(None)
        assert result is None

    def test_df_vacio_retorna_vacio(self):
        df = pd.DataFrame()
        result, informe = aplicar_schema_contract(df)
        assert result.empty

    def test_columnas_faltantes_se_agregan(self):
        # Solo OBRA_PRONTO e IMPORTE → faltan las demás canónicas
        df = pd.DataFrame({"OBRA_PRONTO": ["A"], "IMPORTE": [100.0]})
        result, informe = aplicar_schema_contract(df)
        for col in COLUMNAS_CANONICAS:
            assert col in result.columns
        assert len(informe["columnas_faltantes_agregadas"]) > 0

    def test_columnas_extra_se_descartan(self):
        df = _df_completo()
        df["COLUMNA_EXTRA"] = "basura"
        result, informe = aplicar_schema_contract(df)
        assert "COLUMNA_EXTRA" not in result.columns
        assert "COLUMNA_EXTRA" in informe["columnas_extra_descartadas"]

    def test_orden_canonico_respetado(self):
        # DataFrame con columnas en orden invertido
        df = pd.DataFrame({c: [None] for c in reversed(COLUMNAS_CANONICAS)})
        result, _ = aplicar_schema_contract(df)
        cols_result = [c for c in result.columns if c in COLUMNAS_CANONICAS]
        assert cols_result == COLUMNAS_CANONICAS

    def test_id_ingesta_se_preserva(self):
        df = _df_completo()
        result, _ = aplicar_schema_contract(df)
        assert "_ID_INGESTA" in result.columns

    def test_informe_sin_cambios_cuando_df_completo(self):
        df = _df_completo()
        _, informe = aplicar_schema_contract(df)
        assert informe["columnas_faltantes_agregadas"] == []
        assert informe["columnas_extra_descartadas"] == []

    def test_rellenar_faltantes_false_no_agrega(self):
        df = pd.DataFrame({"OBRA_PRONTO": ["A"]})
        result, informe = aplicar_schema_contract(df, rellenar_faltantes=False)
        # Las faltantes están en el informe pero NO en el DataFrame
        assert len(informe["columnas_faltantes_agregadas"]) > 0
        for col in informe["columnas_faltantes_agregadas"]:
            assert col not in result.columns
