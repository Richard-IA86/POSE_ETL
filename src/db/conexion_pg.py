import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar las variables del .env
load_dotenv(dotenv_path="config/.env")


class ConexionPG:
    _instancia = None

    def __new__(cls):
        # Implementación simple de Singleton para no crear 100 conexiones
        if cls._instancia is None:
            cls._instancia = super(ConexionPG, cls).__new__(cls)
            cls._instancia.engine = cls._crear_engine()
        return cls._instancia

    @staticmethod
    def _crear_engine():
        env_activo = os.getenv("ETL_ENV", "DEV")

        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASS")

        # Selección dinámica de la base de datos
        if env_activo == "PROD":
            database = os.getenv("PG_DB_PROD")
        else:
            database = os.getenv("PG_DB_DEV")

        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

        # Pool_pre_ping revisa la conexión para asegurar que la VPN está levantada
        return create_engine(url, pool_pre_ping=True, pool_size=5)

    def tester_conexion(self):
        """Pinga la BD e imprime el estado activo y versión de PostgreSQL."""
        env_activo = os.getenv("ETL_ENV", "DEV")
        try:
            with self.engine.connect() as conn:
                resultado = conn.execute(text("SELECT version();")).fetchone()
                print(f"✅ Conexión EXITOSA - Entorno: {env_activo}")
                print(f"📦 BASE SELECCIONADA: {self.engine.url.database}")
                print(f"📦 INFO SERVIDOR: {resultado[0]}")
        except Exception as e:
            print(f"❌ ERROR DE CONEXIÓN en Entorno: {env_activo}.")
            print(
                f"📦 INTENTO EN BD: {self.engine.url.database if hasattr(self, 'engine') else 'Desconocida'}"
            )
            print(str(e))


if __name__ == "__main__":
    db = ConexionPG()
    db.tester_conexion()
