from dataclasses import dataclass


@dataclass
class CargadorOutput:
    """
    Contrato de salida del Cargador BD.
    Confirma resultado de la transacción ejecutada.
    """

    run_id: str
    registros_cargados: int
    estado: str  # 'OK' | 'ERROR' | 'ROLLBACK'
    mensaje: str = ""
