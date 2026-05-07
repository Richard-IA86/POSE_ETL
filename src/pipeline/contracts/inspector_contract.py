from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class InspectorOutput:
    """
    Contrato de salida del Inspector de Datos.
    El Constructor ETL SOLO arranca si calidad_ok == True.
    El Constructor usa headers_mapeados, nunca lee el Excel crudo directamente.
    """

    excel_path: Path
    hash_sha256: str  # huella del contenido (no del nombre)
    headers_mapeados: dict[str, str]  # columna_excel → columna_bd
    calidad_ok: bool
    errores: list[str] = field(default_factory=list)
    advertencias: list[str] = field(default_factory=list)
    ya_procesado: bool = False  # True si el hash ya existe en AUDITORIA
    periodo: str = ""  # ej: "2025-Q3", "2026-03"
