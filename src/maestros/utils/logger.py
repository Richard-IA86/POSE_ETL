"""
src/utils/logger.py
────────────────────
Configura Loguru para escribir en consola y en archivo rotativo.
Importar antes de cualquier otro módulo del proyecto.
"""

import sys
from loguru import logger
from config.settings import LOG_LEVEL, LOG_FILE


def configurar_logger() -> None:
    logger.remove()  # eliminar handler default

    # Consola: colores, nivel configurable
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        format=(
            "<green>{time:HH:mm:ss}</green>"
            " | <level>{level: <8}</level>"
            " | {message}"
        ),
        colorize=True,
    )

    # Archivo: rotación diaria, retención 30 días, compresión automática
    logger.add(
        LOG_FILE,
        level="DEBUG",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss}"
            " | {level: <8}"
            " | {name}:{line}"
            " | {message}"
        ),
        rotation="00:00",  # rota a medianoche
        retention="30 days",
        compression="zip",
        encoding="utf-8",
    )
