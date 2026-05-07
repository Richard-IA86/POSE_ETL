"""
Política de retención de archivos.

Elimina archivos con fecha de modificación anterior al umbral
configurado en los directorios de reportes y logs del proyecto.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def limpiar_archivos_antiguos(directorio: Path, dias: int) -> int:
    """
    Elimina archivos con mtime anterior a `dias` días.

    Retorna el número de archivos eliminados.
    """
    if not directorio.exists():
        return 0
    umbral = datetime.now() - timedelta(days=dias)
    eliminados = 0
    for archivo in directorio.iterdir():
        if not archivo.is_file():
            continue
        mtime = datetime.fromtimestamp(archivo.stat().st_mtime)
        if mtime < umbral:
            archivo.unlink()
            logger.info(
                "Retención: eliminado %s (modificado %s)",
                archivo.name,
                mtime.strftime("%Y-%m-%d"),
            )
            eliminados += 1
    if eliminados:
        logger.info(
            "Retención '%s': %d archivo(s) eliminado(s)",
            directorio.name,
            eliminados,
        )
    return eliminados
