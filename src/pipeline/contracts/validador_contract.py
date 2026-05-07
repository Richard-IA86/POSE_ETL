from dataclasses import dataclass, field


@dataclass
class ValidadorOutput:
    """
    Contrato de salida del Validador Staging.
    El Validador informa al Orquestador; el Orquestador decide si abortar.
    Si validacion_ok == False, el pipeline_runner NO llama al Cargador BD.
    """

    validacion_ok: bool
    errores: list[str] = field(default_factory=list)
    advertencias: list[str] = field(default_factory=list)
    registros_validados: int = 0
