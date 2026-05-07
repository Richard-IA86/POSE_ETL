"""
Módulo para lectura de archivos Excel
"""

import pandas as pd
import os
from datetime import datetime  # noqa: F401


class ExcelReader:
    """Lector de archivos Excel con asignación de ID único"""

    def __init__(self):
        self.contador_id_global = 0

    def leer_archivo(self, ruta_archivo):
        """
        Lee un archivo Excel y asigna ID único a cada fila
        Busca la hoja "BASE DE DATOS", si no existe usa la primera hoja

        Args:
            ruta_archivo: Ruta completa al archivo Excel

        Returns:
            DataFrame con columnas de auditoría (_ID_INGESTA, _ARCHIVO_ORIGEN, _RUTA_ORIGEN) o None si hay error  # noqa: E501
        """
        try:
            print(f"  Leyendo: {os.path.basename(ruta_archivo)}...", end=" ")

            # Leer archivo Excel - USANDO CONTEXT MANAGER
            with pd.ExcelFile(ruta_archivo, engine="openpyxl") as xls:
                if len(xls.sheet_names) == 0:
                    print("⚠️ Sin hojas")
                    return None

                # Determinar qué hoja leer (prioridad: Anexar* > BASE DE DATOS > BASE > primera hoja)  # noqa: E501
                hoja_a_leer = None
                estrategia_seleccion = "Ninguna"

                # Crear mapa de nombres normalizados en mayúsculas -> nombre real  # noqa: E501
                sheet_map = {name.upper(): name for name in xls.sheet_names}

                # 1. Prioridad: "Anexar*" (case insensitive)
                for sheet_upper in sheet_map:
                    if sheet_upper.startswith("ANEXAR"):
                        hoja_a_leer = sheet_map[sheet_upper]
                        estrategia_seleccion = "Patrón 'Anexar*'"
                        break

                # 1.5. Prioridad: "ProntoPOSE_Limpia" (Específica Modificaciones)  # noqa: E501
                if not hoja_a_leer and "PRONTOPOSE_LIMPIA" in sheet_map:
                    hoja_a_leer = sheet_map["PRONTOPOSE_LIMPIA"]
                    estrategia_seleccion = "Nombre exacto 'ProntoPOSE_Limpia'"

                # 2. Prioridad: "BASE DE DATOS" (case insensitive)
                if not hoja_a_leer and "BASE DE DATOS" in sheet_map:
                    hoja_a_leer = sheet_map["BASE DE DATOS"]
                    estrategia_seleccion = "Nombre exacto 'BASE DE DATOS'"

                # 3. Prioridad: "BASE" (case insensitive)
                if not hoja_a_leer and "BASE" in sheet_map:
                    hoja_a_leer = sheet_map["BASE"]
                    estrategia_seleccion = "Nombre exacto 'BASE'"

                # 4. Prioridad: "Informe Mensual" (Formato 2026)
                if not hoja_a_leer and "INFORME MENSUAL" in sheet_map:
                    hoja_a_leer = sheet_map["INFORME MENSUAL"]
                    estrategia_seleccion = (
                        "Nombre exacto 'Informe Mensual' (Formato 2026)"
                    )

                # 5. Prioridad: "Tabla" - Común en históricos 2023-2025  # noqa: E501
                if not hoja_a_leer and "TABLA" in sheet_map:
                    hoja_a_leer = sheet_map["TABLA"]
                    estrategia_seleccion = "Nombre exacto 'Tabla'"

                # 6. Fallback: Primera hoja
                if not hoja_a_leer:
                    hoja_a_leer = xls.sheet_names[
                        0
                    ]  # Primera hoja como fallback
                    estrategia_seleccion = "Fallback (Primera hoja)"

                # Leer la hoja seleccionada
                df = pd.read_excel(xls, sheet_name=hoja_a_leer)

                # Formato 2026: eliminar columna MES (no usada en crudo)
                if estrategia_seleccion.startswith(
                    "Nombre exacto 'Informe Mensual'"
                ):
                    cols_upper = {c.upper(): c for c in df.columns}
                    if "MES" in cols_upper:
                        df = df.drop(columns=[cols_upper["MES"]])

            if df.empty:
                print("⚠️ Hoja vacía")
                return None

            # Asignar IDs únicos
            num_filas = len(df)
            df["_ID_INGESTA"] = range(
                self.contador_id_global, self.contador_id_global + num_filas
            )
            self.contador_id_global += num_filas

            # Agregar información de auditoría
            df["_ARCHIVO_ORIGEN"] = os.path.basename(ruta_archivo)
            df["_RUTA_ORIGEN"] = ruta_archivo
            df["_HOJA_ORIGEN"] = hoja_a_leer

            print(
                f"✅ ({num_filas} filas, hoja: '{hoja_a_leer}', criterio: {estrategia_seleccion})"  # noqa: E501
            )
            return df

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            return None

    def leer_carpeta(self, ruta_carpeta):
        """
        Lee todos los archivos Excel de una carpeta

        Args:
            ruta_carpeta: Ruta completa a la carpeta

        Returns:
            Lista de DataFrames con IDs asignados
        """
        dataframes = []

        if not os.path.exists(ruta_carpeta):
            print(f"❌ Carpeta no encontrada: {ruta_carpeta}")
            return dataframes

        archivos = [
            f
            for f in os.listdir(ruta_carpeta)
            if f.endswith((".xlsx", ".xls")) and not f.startswith("~$")
        ]

        archivos.sort()  # Orden alfabético

        for archivo in archivos:
            ruta_completa = os.path.join(ruta_carpeta, archivo)
            df = self.leer_archivo(ruta_completa)

            if df is not None:
                dataframes.append(df)

        return dataframes
