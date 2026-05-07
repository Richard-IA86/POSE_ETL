"""
Generador de BD incremental (B52) desde reservorio estático (A2).
Toma BaseCostosPOSE.xlsx, aplica PKs y particiona salida por Mes.
Uso: python src/automatizacion/generador_b52_incremental.py
"""

import os
import hashlib
from datetime import datetime
import pandas as pd

ARCHIVO_A2 = "fuentes/compensaciones/BaseCostosPOSE.xlsx"
RUTA_SALIDA_B52 = "output/b52"


def generar_id_registro(row, columnas):
    """
    Genera un hash determinístico MD5 para cada fila.
    """
    # Convertimos los valores a string, manejando los nulos
    string_base = "".join(
        str(row[col]) if pd.notna(row[col]) else "" for col in columnas
    )
    return hashlib.md5(string_base.encode("utf-8")).hexdigest()


def procesar_incremental_b52():
    print("=" * 70)
    print("GENERADOR INCREMENTAL UPSERT-READY B52")
    print("=" * 70)

    # 1. Validar e iniciar
    if not os.path.exists(ARCHIVO_A2):
        print(f"❌ Error: No se encuentra el archivo fuente {ARCHIVO_A2}")
        return False

    os.makedirs(RUTA_SALIDA_B52, exist_ok=True)

    print(f"📥 Leyendo reservorio A2: {ARCHIVO_A2}...")
    df = pd.read_excel(ARCHIVO_A2)
    total_filas = len(df)
    print(f"   Filas procesadas: {total_filas:,}")

    # 2. Agregado de metadatos de auditoría
    timestamp_etl = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_timestamp_etl"] = timestamp_etl

    # 3. Generación de Primary Key Determinística
    print("🔑 Generando claves primarias (Hash + Duplicados)...")

    # Orden estable. Como es la salida de Excel, ya tiene orden.
    columnas_negocio = [c for c in df.columns if c not in ["_timestamp_etl"]]

    # Para lidiar con duplicados exactos, creamos un ranking.
    # El arg dropna=False es vital para no ignorar filas vacias.
    df["__row_num"] = (
        df.groupby(columnas_negocio, dropna=False).cumcount().astype(str)
    )

    # Creamos la columna de union previa al hash
    columnas_para_hash = columnas_negocio + ["__row_num"]

    # Optimización: Aplicar hash operando en strings
    df["_id_registro"] = (
        df[columnas_para_hash]
        .astype(str)
        .apply(lambda row: "".join(str(val) for val in row), axis=1)
    )
    df["_id_registro"] = df["_id_registro"].apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )

    # Limpieza de campo temporal
    df = df.drop(columns=["__row_num"])

    # 4. Particionamiento por Año-Mes
    print("📂 Exportando particiones (Año-Mes)...")

    # Extraer el Año-Mes desde la fecha
    df["__anio_mes"] = df["FECHA"].dt.strftime("%Y_%m").fillna("9999_99")

    particiones = df["__anio_mes"].unique()

    # Limpieza directorio previo
    print("   Limpiando exportaciones anteriores...")
    for archivo in os.listdir(RUTA_SALIDA_B52):
        if archivo.startswith("BaseCostosPOSE_B52_") and archivo.endswith(
            ".csv"
        ):
            os.remove(os.path.join(RUTA_SALIDA_B52, archivo))

    archivos_generados = 0
    for particion in sorted(particiones):
        df_part = df[df["__anio_mes"] == particion].copy()
        df_part = df_part.drop(columns=["__anio_mes"])

        nombre_archivo = f"BaseCostosPOSE_B52_{particion}.csv"
        ruta_archivo = os.path.join(RUTA_SALIDA_B52, nombre_archivo)

        # Exportar a CSV
        df_part.to_csv(ruta_archivo, index=False, encoding="utf-8", sep=",")
        archivos_generados += 1

        sys_print = (
            f"  ✅ Exportado: {nombre_archivo} ({len(df_part):,} filas)"
        )
        # Imprimir solo recientes para no abrumar consola
        if particion >= "2025_01" or particion == "9999_99":
            print(sys_print)

    print("      ... (y particiones previas ocultadas)")
    msg = f"\n🚀 Total de particiones CSV: {archivos_generados}"
    print(msg)
    return True


if __name__ == "__main__":
    procesar_incremental_b52()
