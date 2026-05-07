"""
pipeline_runner.py — Orquestador del pipeline ETL

Controla la secuencia de etapas, estado y logging de auditoría.
Ningún agente decide si el pipeline continúa: esa responsabilidad
es exclusiva de este módulo.

Uso:
    python -m projects.shared.pipeline_runner \
        --informe report_direccion --archivo ruta/al/excel.xlsx
"""

import argparse
import hashlib
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("pipeline_runner")


# ---------------------------------------------------------------------------
# Registro de auditoría
# (stub — se conecta a SQL Express cuando esté disponible)
# ---------------------------------------------------------------------------
def log_auditoria(
    run_id: str,
    informe: str,
    archivo: str,
    hash_archivo: str,
    etapa: str,
    estado: str,
    mensaje: str = "",
    ts_inicio: datetime | None = None,
    ts_fin: datetime | None = None,
) -> None:
    """
    Registra el estado de una etapa en AUDITORIA.pipeline_runs.
    Cuando SQL Express esté disponible, reemplazar el log local por INSERT SQL.
    """
    log.info(
        "[AUDITORÍA] run_id=%s | informe=%s | etapa=%s | estado=%s | %s",
        run_id[:8],
        informe,
        etapa,
        estado,
        mensaje,
    )
    # TODO: Reemplazar por INSERT a AUDITORIA.pipeline_runs
    # cuando SQL Express esté activo
    # conn.execute(
    #     """INSERT INTO AUDITORIA.pipeline_runs
    #        (run_id, informe, archivo, hash_archivo,
    #         etapa, estado, mensaje, ts_inicio, ts_fin)
    #        VALUES (?,?,?,?,?,?,?,?,?)""",
    #     run_id, informe, archivo, hash_archivo,
    #     etapa, estado, mensaje,
    #     (ts_inicio or datetime.now()).isoformat(),
    #     (ts_fin or datetime.now()).isoformat(),
    # )


def calcular_hash(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            sha.update(bloque)
    return sha.hexdigest()


def archivo_ya_procesado(hash_sha256: str) -> bool:
    """
    Verifica en AUDITORIA.archivos_procesados si el hash ya
    existe con estado PROCESADO.
    Stub: siempre retorna False hasta que SQL Express esté activo.
    """
    # TODO: Implementar consulta real cuando SQL Express esté disponible
    # result = conn.execute(
    #     "SELECT estado FROM AUDITORIA.archivos_procesados"
    #     " WHERE hash_sha256 = ?",
    #     hash_sha256,
    # ).fetchone()
    # return result is not None and result[0] == 'PROCESADO'
    return False


# ---------------------------------------------------------------------------
# Importación dinámica de etapas por informe
# ---------------------------------------------------------------------------
def _importar_etapas(informe: str, stages_key: str = "STAGES") -> list:
    """
    Importa dinámicamente las etapas del informe solicitado.
    stages_key selecciona qué lista de etapas usar
    (ej. "STAGES" o "STAGES_FDL").
    """
    mod_path = f"projects.{informe}.src.pipeline_stages"
    try:
        mod = __import__(mod_path, fromlist=[stages_key])
    except ImportError:
        raise ImportError(
            f"No se encontró {mod_path}. "
            "Crea ese módulo con STAGES y/o STAGES_FDL."
        )
    if not hasattr(mod, stages_key):
        raise AttributeError(f"{mod_path} no expone '{stages_key}'.")
    return getattr(mod, stages_key)


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------
def run_pipeline(
    informe: str,
    excel_path: Path | None = None,
    stages_key: str = "STAGES",
    fdl_periodo: str | None = None,
    fdl_archivo: str | None = None,
) -> bool:
    """
    Ejecuta el pipeline completo para un informe dado.

    Para pipelines basados en Excel (DESPACHOS): pasar excel_path.
    Para pipelines FDL: pasar stages_key='STAGES_FDL' y,
    opcionalmente, fdl_periodo y/o fdl_archivo.

    Retorna True si todo el pipeline completó exitosamente.
    """
    run_id = str(uuid4())
    if excel_path is not None:
        hash_archivo = calcular_hash(excel_path)
        archivo_str = str(excel_path)
    else:
        hash_archivo = "(fdl-pipeline)"
        archivo_str = "(fdl-pipeline)"

    log.info("=" * 60)
    log.info("PIPELINE INICIADO")
    log.info("  run_id   : %s", run_id)
    log.info("  informe  : %s", informe)
    log.info("  archivo  : %s", archivo_str)
    log.info("  hash     : %s...", hash_archivo[:16])
    log.info("  stages   : %s", stages_key)
    log.info("=" * 60)

    # Verificar idempotencia antes de arrancar
    if excel_path is not None and archivo_ya_procesado(hash_archivo):
        log.warning(
            "Archivo ya fue procesado (hash idéntico). Se omite el pipeline."
        )
        log.warning(
            "Para reprocesar, eliminar el registro en"
            " AUDITORIA.archivos_procesados."
        )
        return True

    stages = _importar_etapas(informe, stages_key)
    context = {
        "run_id": run_id,
        "informe": informe,
        "excel_path": excel_path,
        "hash_archivo": hash_archivo,
        "fdl_periodo": fdl_periodo,
        "fdl_archivo": fdl_archivo,
    }

    for stage_cls in stages:
        stage = stage_cls()
        etapa_nombre = stage_cls.__name__

        ts_inicio = datetime.now()
        log_auditoria(
            run_id,
            informe,
            archivo_str,
            hash_archivo,
            etapa_nombre,
            "INICIADO",
            ts_inicio=ts_inicio,
        )
        log.info("--- Etapa: %s → INICIANDO", etapa_nombre)

        try:
            result = stage.run(context)
        except Exception as exc:
            ts_fin = datetime.now()
            log.error("Error no controlado en etapa %s: %s", etapa_nombre, exc)
            log_auditoria(
                run_id,
                informe,
                archivo_str,
                hash_archivo,
                etapa_nombre,
                "ERROR",
                mensaje=str(exc),
                ts_inicio=ts_inicio,
                ts_fin=ts_fin,
            )
            log.error("PIPELINE ABORTADO en etapa %s", etapa_nombre)
            return False

        ts_fin = datetime.now()

        if not result.get("ok", True):
            error_msg = result.get("error", "Error desconocido")
            log_auditoria(
                run_id,
                informe,
                archivo_str,
                hash_archivo,
                etapa_nombre,
                "ABORTADO",
                mensaje=error_msg,
                ts_inicio=ts_inicio,
                ts_fin=ts_fin,
            )
            log.error("--- Etapa: %s → ABORTADO: %s", etapa_nombre, error_msg)
            log.error("PIPELINE ABORTADO en etapa %s", etapa_nombre)
            return False

        log_auditoria(
            run_id,
            informe,
            archivo_str,
            hash_archivo,
            etapa_nombre,
            "OK",
            ts_inicio=ts_inicio,
            ts_fin=ts_fin,
        )
        log.info("--- Etapa: %s → OK", etapa_nombre)
        # Pasar el resultado al contexto para la siguiente etapa
        context[f"output_{etapa_nombre.lower()}"] = result.get("output")

    log.info("=" * 60)
    log.info("PIPELINE COMPLETADO EXITOSAMENTE — run_id: %s", run_id)
    log.info("=" * 60)
    return True


# ---------------------------------------------------------------------------
# Entry point CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Orquestador del pipeline ETL"
    )
    parser.add_argument(
        "--informe",
        required=True,
        help="Nombre del informe (ej: report_direccion)",
    )
    parser.add_argument(
        "--archivo",
        default=None,
        help="Ruta al archivo Excel de entrada (DESPACHOS).",
    )
    parser.add_argument(
        "--stages-key",
        default="STAGES",
        dest="stages_key",
        help="Etapas a ejecutar: STAGES (default) | STAGES_FDL.",
    )
    parser.add_argument(
        "--periodo",
        default=None,
        dest="fdl_periodo",
        help="Filtrar por año FDL (ej: 2026). Solo para STAGES_FDL.",
    )
    parser.add_argument(
        "--fdl-archivo",
        default=None,
        dest="fdl_archivo",
        help="Archivo FDL específico (ej: 03-2026.xlsx).",
    )
    args = parser.parse_args()

    if args.stages_key == "STAGES" and args.archivo is None:
        log.error("--archivo es requerido para stages_key='STAGES'.")
        sys.exit(1)

    excel: Path | None = None
    if args.archivo is not None:
        excel = Path(args.archivo)
        if not excel.exists():
            log.error("El archivo no existe: %s", excel)
            sys.exit(1)

    exito = run_pipeline(
        informe=args.informe,
        excel_path=excel,
        stages_key=args.stages_key,
        fdl_periodo=args.fdl_periodo,
        fdl_archivo=args.fdl_archivo,
    )
    sys.exit(0 if exito else 1)
