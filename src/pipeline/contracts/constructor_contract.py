from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ConstructorOutput:
    """
    Contrato de salida del Constructor ETL.
    El Validador Staging recibe staging_path para verificar antes de la carga.
    """

    staging_path: Path
    registros_ok: int
    registros_descartados: int
    errores: list[str] = field(default_factory=list)
    advertencias: list[str] = field(default_factory=list)
