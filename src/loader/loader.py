import pandas as pd
from typing import List
from src.db.conexion_pg import ConexionPG
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine


class PostgresLoader:
    def __init__(self):
        self.conexion = ConexionPG()
        self.engine: Engine = self.conexion.engine

    def asegurar_tabla_y_pk(
        self, df: pd.DataFrame, schema: str, tabla: str, pk_col: str
    ):
        """
        Verifica si la tabla existe. Si no, la crea con pandas
        y le asigna la PK para que el UPSERT funcione desde la primera carga.
        """
        esquema_tabla = tabla if schema == "public" else f"{schema}.{tabla}"
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            # Verifica si la tabla existe en el esquema
            if not inspector.has_table(tabla, schema=schema):
                print(
                    f"⚠️ Tabla {esquema_tabla} no existe."
                    " Creando a partir del DataFrame..."
                )
                # Crea tabla vacía (solo estructura) para inferir tipos
                df.head(0).to_sql(
                    name=tabla,
                    con=conn,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                )

                # Asignar Primary Key
                # Necesitamos un bloque COMMIT para ALTER TABLE
                conn.commit()
                with self.engine.begin() as conn_alter:
                    query_pk = (
                        f"ALTER TABLE {esquema_tabla}"
                        f" ADD PRIMARY KEY ({pk_col});"
                    )
                    conn_alter.execute(text(query_pk))
                print(f"✅ Tabla {esquema_tabla} creada con PK en '{pk_col}'.")
            else:
                print(f"✅ Tabla {esquema_tabla} validada. Ya existe.")

    def upsert_tabla(
        self,
        df: pd.DataFrame,
        schema: str,
        tabla: str,
        constraint_cols: List[str],
    ):
        """
        Inserta datos, actualizando si ya existen
        según las columnas listadas en constraint_cols (UPSERT / ON CONFLICT).
        """
        if df.empty:
            return {
                "status": "ok",
                "filas_afectadas": 0,
                "msg": "DataFrame vacío.",
            }

        # Postgres no permite UPSERTs en lote si hay duplicados en el
        # origen. Deduplicamos manteniendo la última versión según las
        # keys de constraint.
        df_clean = df.drop_duplicates(
            subset=constraint_cols, keep="last"
        ).copy()

        # Asegurar que la tabla destino exista y tenga la PK antes del Upsert
        self.asegurar_tabla_y_pk(
            df_clean, schema, tabla, pk_col=constraint_cols[0]
        )

        esquema_tabla = tabla if schema == "public" else f"{schema}.{tabla}"
        temp_table = f"temp_{tabla}"

        with self.engine.begin() as conn:
            # 1. Crear tabla temporal
            df_clean.to_sql(
                name=temp_table,
                con=conn,
                schema=None,
                if_exists="replace",
                index=False,
            )

            # 2. Armar la query de UPSERT dinámica
            columnas = ", ".join([f'"{col}"' for col in df.columns])
            update_sets = ", ".join(
                [
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in df.columns
                    if col not in constraint_cols
                ]
            )
            conflict_target = ", ".join(
                [f'"{col}"' for col in constraint_cols]
            )

            query = f"""
            INSERT INTO {esquema_tabla} ({columnas})
            SELECT {columnas} FROM "{temp_table}"
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_sets};
            """

            # 3. Ejecutar y limpiar temporal
            resultado = conn.execute(text(query))
            conn.execute(text(f'DROP TABLE "{temp_table}";'))

            return {
                "status": "ok",
                "filas_afectadas": resultado.rowcount,
                "msg": f"Upsert exitoso en {esquema_tabla}",
            }
