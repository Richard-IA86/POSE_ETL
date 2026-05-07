"""
_conexion.py — Carga la cadena de conexión desde conexion.json.

Busca en el orden:
1. projects/shared/db/conexion.json  (archivo real, en .gitignore)
2. Variable de entorno POSE_DB_CONN  (para CI/CD o Docker)

Si ninguno está disponible, lanza EnvironmentError con instrucciones claras.
"""

import json
import os
from pathlib import Path

# parents[3] = projects/  →  projects/shared/db/conexion.json
_CONEXION_JSON = Path(__file__).parents[3] / "shared" / "db" / "conexion.json"


def get_connection_string() -> str:
    """
    Retorna la cadena de conexión ODBC para pyodbc.

    Returns
    -------
    str
        Cadena tipo: DRIVER=...;SERVER=...;DATABASE=...;...
    """
    # Opción 1: archivo local (ignorado por .gitignore)
    # Usa el campo 'cadena_conexion' directamente si existe; de lo contrario
    # construye desde los campos individuales para compatibilidad.
    if _CONEXION_JSON.exists():
        with _CONEXION_JSON.open(encoding="utf-8") as f:
            cfg = json.load(f)
        if "cadena_conexion" in cfg:
            return cfg["cadena_conexion"]
        # Fallback: construir desde campos individuales
        servidor = cfg.get("servidor") or cfg.get("server", "")
        base = cfg.get("base_datos") or cfg.get("database", "")
        driver = cfg.get("driver", "ODBC Driver 18 for SQL Server")
        trusted = (
            "yes" if cfg.get("autenticacion", "windows") == "windows" else "no"
        )
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={servidor};"
            f"DATABASE={base};"
            f"Trusted_Connection={trusted};"
            f"TrustServerCertificate=yes;"
        )

    # Opción 2: variable de entorno
    conn_env = os.getenv("POSE_DB_CONN")
    if conn_env:
        return conn_env

    raise EnvironmentError(
        f"No se encontró cadena de conexión.\n"
        f"  Opción A: Crear '{_CONEXION_JSON}' basándose en conexion.template.json\n"  # noqa: E501
        f"  Opción B: Definir la variable de entorno POSE_DB_CONN"
    )
