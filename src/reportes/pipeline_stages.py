"""
pipeline_stages.py — Etapas del pipeline para el informe report_direccion.

pipeline_runner importa este módulo dinámicamente:
    from src.reportes.pipeline_stages import STAGES

Cada Stage implementa run(context) → dict con claves:
    ok     : bool
    error  : str  (solo si ok=False)
    output : <ContractOutput>  (uno de los dataclasses de shared/contracts)
"""

from pathlib import Path

import pandas as pd

from src.pipeline.contracts import (
    InspectorOutput,
    ConstructorOutput,
    ValidadorOutput,
    CargadorOutput,
)
from src.reportes.nuevas_fuentes._constantes import (
    COLS_STAGING as FDL_COLS_STAGING,
    COLS_STAGING as MENSUALES_COLS_STAGING,
    COLS_STAGING as QUINCENAS_COLS_STAGING,
)
from src.reportes.ingesta.reader import leer_excel_crudo
from src.reportes.ingesta.transformer import transformar
from src.reportes.ingesta.writer import escribir_staging
from src.reportes.loader.bd_loader import cargar_a_bd
from src.reportes.loader.bd_loader_despachos import (
    cargar_validados,
    cargar_rechazados,
)

# Ruta base del sub-proyecto (report_gerencias)
_REPORT_GERENCIAS = Path(__file__).parents[1] / "report_gerencias"


# ---------------------------------------------------------------------------
# Etapa 1 — Inspector de Datos
# ---------------------------------------------------------------------------
class InspectorStage:
    """
    Calcula hash, mapea encabezados y valida calidad mínima del Excel crudo.
    Resultado: InspectorOutput con calidad_ok=True/False.
    """

    def run(self, context: dict) -> dict:
        try:
            output: InspectorOutput = leer_excel_crudo(
                excel_path=context["excel_path"],
                hash_archivo=context["hash_archivo"],
            )
            if not output.calidad_ok:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa 2 — Constructor ETL
# ---------------------------------------------------------------------------
class ConstructorStage:
    """
    Aplica transformaciones y enriquecimiento usando InspectorOutput.
    NUNCA accede al Excel crudo directamente — solo usa headers_mapeados.
    Resultado: ConstructorOutput con ruta al archivo staging.
    """

    def run(self, context: dict) -> dict:
        inspector_output: InspectorOutput | None = context.get(
            "output_inspectorstage"
        )
        if inspector_output is None:
            return {
                "ok": False,
                "error": "InspectorStage no produjo output",
                "output": None,
            }
        try:
            df_staging, df_pendientes = transformar(inspector_output)
            output: ConstructorOutput = escribir_staging(
                df_staging=df_staging,
                df_pendientes=df_pendientes,
                informe=context["informe"],
                run_id=context["run_id"],
            )
            if output.errores:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa 3 — Validador Staging
# ---------------------------------------------------------------------------
class ValidadorStage:
    """
    Valida el archivo staging antes de cualquier operación sobre la BD.
    Solo lectura — no modifica nada.
    Resultado: ValidadorOutput con validacion_ok=True/False.
    """

    def run(self, context: dict) -> dict:
        constructor_output: ConstructorOutput | None = context.get(
            "output_constructorstage"
        )
        if constructor_output is None:
            return {
                "ok": False,
                "error": "ConstructorStage no produjo output",
                "output": None,
            }
        try:
            from src.reportes.loader.validador import (
                validar_staging,
            )

            output: ValidadorOutput = validar_staging(
                constructor_output.staging_path
            )
            if not output.validacion_ok:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa 4 — Cargador BD (genérico report_direccion — tabla de agregados)
# ---------------------------------------------------------------------------
class CargadorStage:
    """
    Ejecuta DELETE + INSERT en transacción con rollback automático.
    No contiene lógica de negocio — solo mecánica de carga.
    Resultado: CargadorOutput con estado OK/ERROR/ROLLBACK.
    """

    def run(self, context: dict) -> dict:
        constructor_output: ConstructorOutput | None = context.get(
            "output_constructorstage"
        )
        if constructor_output is None:
            return {
                "ok": False,
                "error": "ConstructorStage no produjo output",
                "output": None,
            }
        try:
            output: CargadorOutput = cargar_a_bd(
                staging_path=constructor_output.staging_path,
                run_id=context["run_id"],
                informe=context["informe"],
            )
            if output.estado != "OK":
                return {"ok": False, "error": output.mensaje, "output": output}
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa 5 — Cargador Despachos Validados
#   Carga el staging limpio → PRODUCCION.despachos_validados
#   (DELETE por periodo + INSERT — idempotente)
# ---------------------------------------------------------------------------
class CargadorDespachosValidadosStage:
    """
    Carga staging_despachos.csv a PRODUCCION.despachos_validados.
    Requiere que ValidadorStage y ConstructorStage hayan completado OK.
    """

    def run(self, context: dict) -> dict:
        constructor_output: ConstructorOutput | None = context.get(
            "output_constructorstage"
        )
        if constructor_output is None:
            return {
                "ok": False,
                "error": "ConstructorStage no produjo output",
                "output": None,
            }

        staging_csv = constructor_output.staging_path
        if not staging_csv.exists():
            return {
                "ok": False,
                "error": f"No se encontró staging en {staging_csv}",
                "output": None,
            }

        try:
            resultado = cargar_validados(
                staging_csv=staging_csv,
                run_id=context["run_id"],
                hash_archivo=context["hash_archivo"],
            )
            if not resultado["ok"]:
                return {
                    "ok": False,
                    "error": resultado["error"],
                    "output": resultado,
                }
            return {"ok": True, "output": resultado}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa 6 — Cargador Despachos Rechazados
#   Carga pendientes_carga.csv → PRODUCCION.despachos_rechazados
#   (acumulativo — no borra historial previo)
# ---------------------------------------------------------------------------
class CargadorDespachosRechazadosStage:
    """
    Carga pendientes_carga.csv a PRODUCCION.despachos_rechazados.
    Si no hay pendientes, pasa silenciosamente (ok=True, 0 registros).
    El Dashboard Streamlit resuelve cada fila mediante resolver_rechazado().
    """

    def run(self, context: dict) -> dict:
        pendientes_csv = _REPORT_GERENCIAS / "pendientes_carga.csv"

        try:
            resultado = cargar_rechazados(
                pendientes_csv=pendientes_csv,
                run_id=context["run_id"],
                hash_archivo=context["hash_archivo"],
                motivo_rechazo="Duplicado detectado por inspector — pendiente de revisión",  # noqa: E501
            )
            if not resultado["ok"]:
                return {
                    "ok": False,
                    "error": resultado["error"],
                    "output": resultado,
                }
            return {"ok": True, "output": resultado}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Registro de etapas — pipeline_runner importa STAGES
# ---------------------------------------------------------------------------
STAGES = [
    InspectorStage,
    ConstructorStage,
    ValidadorStage,
    CargadorDespachosValidadosStage,
    CargadorDespachosRechazadosStage,
]


# ---------------------------------------------------------------------------
# Utilidad interna — Validación staging FDL
# ---------------------------------------------------------------------------
def _validar_staging_fdl(staging_path: Path) -> ValidadorOutput:
    """
    Valida staging_fdl.csv: existencia, columnas y nulos
    en columnas obligatorias (las que terminan en '*').
    """
    if not staging_path.exists():
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"Staging FDL no encontrado: {staging_path}"],
        )

    try:
        df = pd.read_csv(
            staging_path,
            sep=";",
            dtype=str,
            encoding="utf-8-sig",
        )
    except Exception as exc:
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"No se pudo leer staging FDL: {exc}"],
        )

    if df.empty:
        return ValidadorOutput(
            validacion_ok=False,
            errores=["El staging FDL está vacío."],
        )

    errores: list[str] = []
    advertencias: list[str] = []

    # Columnas presentes
    for col in FDL_COLS_STAGING:
        if col not in df.columns:
            errores.append(f"Columna ausente en staging FDL: '{col}'")

    if errores:
        return ValidadorOutput(validacion_ok=False, errores=errores)

    # Nulos en columnas obligatorias (terminan en '*')
    cols_req = [c for c in FDL_COLS_STAGING if c.endswith("*")]
    for col in cols_req:
        nulos = df[col].isna().sum()
        if nulos > 0:
            errores.append(f"Columna '{col}' tiene {nulos} valor(es) nulo(s).")

    # IMPORTE* numérico
    if "IMPORTE*" in df.columns:
        no_num = pd.to_numeric(df["IMPORTE*"], errors="coerce").isna().sum()
        if no_num > 0:
            advertencias.append(
                f"'IMPORTE*' tiene {no_num} valor(es) no numérico(s)."
            )

    return ValidadorOutput(
        validacion_ok=len(errores) == 0,
        errores=errores,
        advertencias=advertencias,
        registros_validados=len(df),
    )


# ---------------------------------------------------------------------------
# Etapa A — Transformador FDL
#   Descubre archivos MM-YYYY.xlsx → transforma → staging_fdl.csv
# ---------------------------------------------------------------------------
class TransformadorFDLStage:
    """
    Orquesta el pipeline FDL: descubrimiento → transformación →
    persistencia en staging_fdl.csv.
    Resultado: ConstructorOutput con staging_path.
    """

    def run(self, context: dict) -> dict:
        from src.reportes.nuevas_fuentes.run_fdl import (
            ejecutar,
        )

        try:
            staging_path = ejecutar(
                periodo=context.get("fdl_periodo"),
                nombre=context.get("fdl_archivo"),
            )
            df = pd.read_csv(staging_path, sep=";", encoding="utf-8-sig")
            output = ConstructorOutput(
                staging_path=staging_path,
                registros_ok=len(df),
                registros_descartados=0,
            )
            return {"ok": True, "output": output}
        except (FileNotFoundError, ValueError) as exc:
            return {"ok": False, "error": str(exc), "output": None}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa B — Validador FDL
#   Valida staging_fdl.csv antes de cualquier operación sobre BD
# ---------------------------------------------------------------------------
class ValidadorFDLStage:
    """
    Valida el staging FDL: columnas obligatorias presentes,
    sin nulos en campos requeridos, IMPORTE* numérico.
    Solo lectura — no modifica nada.
    """

    def run(self, context: dict) -> dict:
        trans_out: ConstructorOutput | None = context.get(
            "output_transformadorfdlstage"
        )
        if trans_out is None:
            return {
                "ok": False,
                "error": "TransformadorFDLStage no produjo output",
                "output": None,
            }
        try:
            output = _validar_staging_fdl(trans_out.staging_path)
            if not output.validacion_ok:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapas FDL — pipeline_runner importa STAGES_FDL
# ---------------------------------------------------------------------------
STAGES_FDL = [
    TransformadorFDLStage,
    ValidadorFDLStage,
]


# ---------------------------------------------------------------------------
# Utilidad interna — Validación staging MENSUALES
# ---------------------------------------------------------------------------
def _validar_staging_mensuales(staging_path: Path) -> ValidadorOutput:
    """
    Valida staging_mensuales.csv: existencia, columnas, nulos en
    columnas obligatorias e IMPORTE* numérico.
    """
    if not staging_path.exists():
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"Staging MENSUALES no encontrado: {staging_path}"],
        )

    try:
        df = pd.read_csv(
            staging_path,
            sep=";",
            dtype=str,
            encoding="utf-8-sig",
        )
    except Exception as exc:
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"No se pudo leer staging MENSUALES: {exc}"],
        )

    if df.empty:
        return ValidadorOutput(
            validacion_ok=False,
            errores=["El staging MENSUALES está vacío."],
        )

    errores: list[str] = []
    advertencias: list[str] = []

    for col in MENSUALES_COLS_STAGING:
        if col not in df.columns:
            errores.append(f"Columna ausente en staging MENSUALES: '{col}'")

    if errores:
        return ValidadorOutput(validacion_ok=False, errores=errores)

    cols_req = [c for c in MENSUALES_COLS_STAGING if c.endswith("*")]
    for col in cols_req:
        nulos = df[col].isna().sum()
        if nulos > 0:
            errores.append(f"Columna '{col}' tiene {nulos} valor(es) nulo(s).")

    if "IMPORTE*" in df.columns:
        no_num = pd.to_numeric(df["IMPORTE*"], errors="coerce").isna().sum()
        if no_num > 0:
            advertencias.append(
                f"'IMPORTE*' tiene {no_num} valor(es) no numérico(s)."
            )

    return ValidadorOutput(
        validacion_ok=len(errores) == 0,
        errores=errores,
        advertencias=advertencias,
        registros_validados=len(df),
    )


# ---------------------------------------------------------------------------
# Etapa C — Transformador MENSUALES
# ---------------------------------------------------------------------------
class TransformadorMENSUALESStage:
    """
    Orquesta el pipeline MENSUALES: descubrimiento → transformación →
    persistencia en staging_mensuales.csv.
    Resultado: ConstructorOutput con staging_path.
    """

    def run(self, context: dict) -> dict:
        from src.reportes.nuevas_fuentes.run_mensuales import (  # noqa: E501
            ejecutar,
        )

        try:
            staging_path = ejecutar(
                periodo=context.get("fdl_periodo"),
                nombre=context.get("fdl_archivo"),
            )
            df = pd.read_csv(staging_path, sep=";", encoding="utf-8-sig")
            output = ConstructorOutput(
                staging_path=staging_path,
                registros_ok=len(df),
                registros_descartados=0,
            )
            return {"ok": True, "output": output}
        except (FileNotFoundError, ValueError) as exc:
            return {"ok": False, "error": str(exc), "output": None}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa D — Validador MENSUALES
# ---------------------------------------------------------------------------
class ValidadorMENSUALESStage:
    """
    Valida el staging MENSUALES: columnas obligatorias presentes,
    sin nulos en campos requeridos, IMPORTE* numérico.
    Solo lectura — no modifica nada.
    """

    def run(self, context: dict) -> dict:
        trans_out: ConstructorOutput | None = context.get(
            "output_transformadormensualesstage"
        )
        if trans_out is None:
            return {
                "ok": False,
                "error": "TransformadorMENSUALESStage no produjo output",
                "output": None,
            }
        try:
            output = _validar_staging_mensuales(trans_out.staging_path)
            if not output.validacion_ok:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapas MENSUALES — pipeline_runner importa STAGES_MENSUALES
# ---------------------------------------------------------------------------
STAGES_MENSUALES = [
    TransformadorMENSUALESStage,
    ValidadorMENSUALESStage,
]


# ---------------------------------------------------------------------------
# Utilidad interna — Validación staging QUINCENAS
# ---------------------------------------------------------------------------
def _validar_staging_quincenas(staging_path: Path) -> ValidadorOutput:
    """
    Valida staging_quincenas.csv: existencia, columnas, nulos en
    columnas obligatorias e IMPORTE* numérico.
    """
    if not staging_path.exists():
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"Staging QUINCENAS no encontrado: {staging_path}"],
        )

    try:
        df = pd.read_csv(
            staging_path,
            sep=";",
            dtype=str,
            encoding="utf-8-sig",
        )
    except Exception as exc:
        return ValidadorOutput(
            validacion_ok=False,
            errores=[f"No se pudo leer staging QUINCENAS: {exc}"],
        )

    if df.empty:
        return ValidadorOutput(
            validacion_ok=False,
            errores=["El staging QUINCENAS está vacío."],
        )

    errores: list[str] = []
    advertencias: list[str] = []

    for col in QUINCENAS_COLS_STAGING:
        if col not in df.columns:
            errores.append(f"Columna ausente en staging QUINCENAS: '{col}'")

    if errores:
        return ValidadorOutput(validacion_ok=False, errores=errores)

    cols_req = [c for c in QUINCENAS_COLS_STAGING if c.endswith("*")]
    for col in cols_req:
        nulos = df[col].isna().sum()
        if nulos > 0:
            errores.append(f"Columna '{col}' tiene {nulos} valor(es) nulo(s).")

    if "IMPORTE*" in df.columns:
        no_num = pd.to_numeric(df["IMPORTE*"], errors="coerce").isna().sum()
        if no_num > 0:
            advertencias.append(
                f"'IMPORTE*' tiene {no_num} valor(es) no numérico(s)."
            )

    return ValidadorOutput(
        validacion_ok=len(errores) == 0,
        errores=errores,
        advertencias=advertencias,
        registros_validados=len(df),
    )


# ---------------------------------------------------------------------------
# Etapa E — Transformador QUINCENAS
# ---------------------------------------------------------------------------
class TransformadorQUINCENASStage:
    """
    Orquesta el pipeline QUINCENAS: descubrimiento → transformación
    → persistencia en staging_quincenas.csv.
    Resultado: ConstructorOutput con staging_path.
    """

    def run(self, context: dict) -> dict:
        from src.reportes.nuevas_fuentes.run_quincenas import (  # noqa: E501
            ejecutar,
        )

        try:
            staging_path = ejecutar(
                periodo=context.get("quincenas_periodo"),
                nombre=context.get("quincenas_archivo"),
            )
            df = pd.read_csv(staging_path, sep=";", encoding="utf-8-sig")
            output = ConstructorOutput(
                staging_path=staging_path,
                registros_ok=len(df),
                registros_descartados=0,
            )
            return {"ok": True, "output": output}
        except (FileNotFoundError, ValueError) as exc:
            return {"ok": False, "error": str(exc), "output": None}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapa F — Validador QUINCENAS
# ---------------------------------------------------------------------------
class ValidadorQUINCENASStage:
    """
    Valida el staging QUINCENAS: columnas obligatorias presentes,
    sin nulos en campos requeridos, IMPORTE* numérico.
    Solo lectura — no modifica nada.
    """

    def run(self, context: dict) -> dict:
        trans_out: ConstructorOutput | None = context.get(
            "output_transformadorquincenasstage"
        )
        if trans_out is None:
            return {
                "ok": False,
                "error": "TransformadorQUINCENASStage no produjo output",
                "output": None,
            }
        try:
            output = _validar_staging_quincenas(trans_out.staging_path)
            if not output.validacion_ok:
                return {
                    "ok": False,
                    "error": "; ".join(output.errores),
                    "output": output,
                }
            return {"ok": True, "output": output}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "output": None}


# ---------------------------------------------------------------------------
# Etapas QUINCENAS — pipeline_runner importa STAGES_QUINCENAS
# ---------------------------------------------------------------------------
STAGES_QUINCENAS = [
    TransformadorQUINCENASStage,
    ValidadorQUINCENASStage,
]
