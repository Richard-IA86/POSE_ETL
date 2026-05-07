"""
Configuración centralizada del pipeline ETL.
Lee variables de entorno (.env) y expone parámetros de conexión,
rutas de archivos y ajustes generales del proceso.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "output" / "reportes"
REPORTES_DIR = BASE_DIR / "output" / "reportes"

# Cargar variables desde archivo .env (si existe)
load_dotenv(BASE_DIR / ".env")

# Crear directorios si no existen
DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ConfigDB:
    """Parámetros de conexión a la base de datos origen."""

    host: str = field(
        default_factory=lambda: os.getenv("DB_HOST", "localhost")
    )
    puerto: int = field(
        default_factory=lambda: int(os.getenv("DB_PORT", "5432"))
    )
    nombre: str = field(
        default_factory=lambda: os.getenv("DB_NAME", "gestion_comp")
    )
    usuario: str = field(
        default_factory=lambda: os.getenv("DB_USER", "etl_user")
    )
    contrasena: str = field(
        default_factory=lambda: os.getenv("DB_PASSWORD", "")
    )

    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.usuario}:{self.contrasena}"
            f"@{self.host}:{self.puerto}/{self.nombre}"
        )


@dataclass
class ConfigReportes:
    """Configuración para la descarga y generación de reportes."""

    directorio_salida: Path = field(default_factory=lambda: REPORTES_DIR)
    formato: str = field(
        default_factory=lambda: os.getenv("REPORTE_FORMATO", "xlsx")
    )
    api_url: str = field(
        default_factory=lambda: os.getenv(
            "API_REPORTES_URL", "http://localhost:8080/api"
        )
    )
    api_token: str = field(
        default_factory=lambda: os.getenv("API_REPORTES_TOKEN", "")
    )


@dataclass
class ConfigETL:
    """Parámetros generales del pipeline ETL."""

    db: ConfigDB = field(default_factory=ConfigDB)
    reportes: ConfigReportes = field(default_factory=ConfigReportes)
    directorio_datos: Path = field(default_factory=lambda: DATA_DIR)
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("ETL_BATCH_SIZE", "1000"))
    )
    fecha_proceso: str = field(
        default_factory=lambda: os.getenv("ETL_FECHA_PROCESO", "")
    )
    encoding: str = "utf-8"
    separador_csv: str = ";"
