"""
Módulo para escritura de archivos Excel y reportes
"""

import pandas as pd
import os
from datetime import datetime

from schema_contract import aplicar_schema_contract


class ExcelWriter:
    """Escritor de archivos Excel con generación de reportes"""

    def __init__(
        self,
        ruta_reportes,
        schema_cols=None,
        schema_drop_extra=False,
    ):
        self.ruta_reportes = ruta_reportes
        self.reporte_consolidado = []

    def guardar_archivo(
        self,
        df,
        ruta_salida,
        nombre_archivo,
        ruta_origen=None,
        archivo_origen=None,
        stats_duplicados=None,
    ):
        """
        Guarda DataFrame en archivo Excel

        Args:
            df: DataFrame a guardar
            ruta_salida: Ruta del directorio de salida
            nombre_archivo: Nombre del archivo Excel
            ruta_origen: Ruta del archivo original de entrada (para auditoría)
            archivo_origen: Nombre del archivo original (para auditoría)
            stats_duplicados: Diccionario con estadísticas de duplicados

        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        if df is None or df.empty:
            print(f"⚠️ No hay datos para guardar en {nombre_archivo}")
            return False

        try:
            # Crear directorio si no existe
            os.makedirs(ruta_salida, exist_ok=True)

            # 1. Aplicar Schema Contract (orden y columnas canónicas)
            df_salida, informe_schema = aplicar_schema_contract(df.copy())

            # 2. Eliminar columna interna de ingesta
            if "_ID_INGESTA" in df_salida.columns:
                df_salida = df_salida.drop(columns=["_ID_INGESTA"])

            # Reportar ajustes del schema
            if informe_schema.get("columnas_faltantes_agregadas"):
                print(
                    "  [Schema] Columnas añadidas: "
                    f"{informe_schema['columnas_faltantes_agregadas']}"
                )
            if informe_schema.get("columnas_extra_descartadas"):
                print(
                    "  [Schema] Columnas extra descartadas: "
                    f"{informe_schema['columnas_extra_descartadas']}"
                )

            # 3. Ruta completa
            ruta_completa = os.path.join(ruta_salida, nombre_archivo)

            # 4. Guardar archivo con context manager para asegurar cierre
            with pd.ExcelWriter(ruta_completa, engine="openpyxl") as writer:
                df_salida.to_excel(writer, index=False)

            filas = len(df_salida)
            print(f"  ✅ Guardado: {nombre_archivo} ({filas} filas)")

            # Registrar en reporte
            # Manejar archivos_origen como lista o string
            if isinstance(archivo_origen, list):
                archivos_str = archivo_origen
            elif archivo_origen:
                archivos_str = [archivo_origen]
            else:
                archivos_str = [nombre_archivo]

            # Extraer hoja de origen si está disponible
            hoja_origen = (
                df["_HOJA_ORIGEN"].iloc[0]
                if "_HOJA_ORIGEN" in df.columns and not df.empty
                else "N/A"
            )

            # Obtener suma importe y rellenados desde stats
            stats = stats_duplicados if stats_duplicados else {}

            self.reporte_consolidado.append(
                {
                    "archivo": nombre_archivo,
                    "archivos_origen": archivos_str,
                    "num_archivos_origen": len(archivos_str),
                    "hoja_origen": hoja_origen,
                    "filas": filas,
                    "columnas": len(df_salida.columns),
                    "ruta_destino": ruta_completa,
                    "ruta_origen": (
                        ruta_origen if ruta_origen else ruta_completa
                    ),
                    "dup_origen": stats.get("duplicados_origen", 0),
                    "dup_proceso": stats.get("duplicados_proceso", 0),
                    "filas_sin_fecha": stats.get("filas_sin_fecha", 0),
                    "suma_importe": stats.get("suma_importe", 0.0),
                    "importe_sin_dato": stats.get("importe_sin_dato", 0),
                    "importe_costo_cero": stats.get("importe_costo_cero", 0),
                    "importe_rellenado": stats.get(
                        "filas_importe_cero_rellenadas", 0
                    ),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

            return True

        except Exception as e:
            print(f"  ❌ Error guardando {nombre_archivo}: {str(e)}")
            return False

    def guardar_individuales(self, df, ruta_salida, archivos_originales):
        """
        Guarda DataFrames individuales manteniendo nombres originales

        Args:
            df: DataFrame consolidado con _ID_INGESTA
            ruta_salida: Ruta del directorio de salida
            archivos_originales: Lista de nombres de archivos originales

        Returns:
            Número de archivos guardados exitosamente
        """
        # Por ahora guardamos todo consolidado
        # En futuro se puede separar por ID si es necesario
        archivos_guardados = 0

        for nombre_archivo in archivos_originales:
            # Aquí podrías filtrar df por algún criterio si es necesario
            if self.guardar_archivo(df, ruta_salida, nombre_archivo):
                archivos_guardados += 1

        return archivos_guardados

    def generar_reporte_final(self, stats_transformacion):
        """
        Genera reporte final de ejecución

        Args:
            stats_transformacion: Diccionario con estadísticas de transformación  # noqa: E501
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_reporte = f"reporte_ejecucion_{timestamp}.txt"
        ruta_reporte = os.path.join(self.ruta_reportes, nombre_reporte)

        try:
            with open(ruta_reporte, "w", encoding="utf-8") as f:
                f.write("=" * 70 + "\n")
                f.write("REPORTE DE EJECUCIÓN - NORMALIZADOR DE COSTOS\n")
                f.write("=" * 70 + "\n")
                f.write(
                    f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"  # noqa: E501
                )

                f.write("ESTADÍSTICAS DE TRANSFORMACIÓN:\n")
                f.write("-" * 70 + "\n")
                f.write(
                    f"Duplicados de origen detectados: {stats_transformacion.get('duplicados_origen', 0)}\n"  # noqa: E501
                )
                f.write(
                    f"Duplicados de proceso eliminados: {stats_transformacion.get('duplicados_proceso', 0)}\n"  # noqa: E501
                )
                f.write(
                    f"Total filas eliminadas: {stats_transformacion.get('filas_eliminadas', 0)}\n\n"  # noqa: E501
                )

                f.write("ARCHIVOS GENERADOS:\n")
                f.write("-" * 70 + "\n")

                total_filas = 0
                for i, registro in enumerate(self.reporte_consolidado, 1):
                    f.write(f"\n[{i}] {registro['archivo']}\n")
                    f.write(f"{'-' * 70}\n")

                    # Origen
                    num_archivos = registro.get("num_archivos_origen", 1)
                    if num_archivos > 1:
                        f.write(
                            f"Origen     : {num_archivos} archivos consolidados\n"  # noqa: E501
                        )
                        for arch in registro.get("archivos_origen", []):
                            f.write(f"             • {arch}\n")
                    else:
                        f.write(
                            f"Origen     : {registro.get('archivos_origen', ['N/A'])[0]}\n"  # noqa: E501
                        )

                    # Hoja, Filas, Duplicados
                    f.write(
                        f"Hoja       : {registro.get('hoja_origen', 'N/A')}\n"
                    )
                    f.write(
                        f"Filas      : {registro['filas']:,} procesadas | {registro['columnas']} columnas\n"  # noqa: E501
                    )

                    if registro.get("filas_sin_fecha", 0) > 0:
                        f.write(
                            f"⚠️ NO FECHA  : {registro['filas_sin_fecha']} (Requiere revisión manual)\n"  # noqa: E501
                        )

                    if registro.get("importe_rellenado", 0) > 0:
                        f.write(
                            f"⚠️ IMPORTE=0 : {registro['importe_rellenado']} filas rellenadas (Originalmente vacías)\n"  # noqa: E501
                        )

                    f.write(
                        f"Dup.Origen : {registro.get('dup_origen', 0)} (mantenidos)\n"  # noqa: E501
                    )
                    f.write(
                        "Dup.Proceso: "
                        f"{registro.get('dup_proceso', 0)} (eliminados)\n"
                    )
                    f.write(
                        "Suma Import: "
                        f"${registro.get('suma_importe', 0.0):,.2f}\n"
                    )

                    # Calidad de dato IMPORTE
                    sin_dato = registro.get("importe_sin_dato", 0)
                    costo_cero = registro.get("importe_costo_cero", 0)
                    if sin_dato > 0:
                        f.write(f"  Sin Dato (nulo→0): {sin_dato} filas\n")
                    if costo_cero > 0:
                        f.write(
                            f"  Costo Cero (=0 en origen): "
                            f"{costo_cero} filas\n"
                        )

                    # Rutas
                    f.write(f"Ruta Orig  : {registro['ruta_origen']}\n")
                    f.write(f"Ruta Dest  : {registro['ruta_destino']}\n")
                    f.write(f"Timestamp  : {registro['timestamp']}\n")

                    total_filas += registro["filas"]

                f.write("=" * 70 + "\n")
                f.write(f"TOTAL REGISTROS EXPORTADOS: {total_filas:,}\n")
                f.write("=" * 70 + "\n")

            print(f"\n📊 Reporte generado: {nombre_reporte}")

            # Generar reporte MD también
            self.generar_reporte_md()

        except Exception as e:
            print(f"⚠️ Error generando reporte: {str(e)}")

    def generar_reporte_md(self):
        """Genera reporte en formato Markdown para documentación"""
        ruta_md = "power_query/Arch_BD_CostoUnificada/REPORTE_INGESTA.md"

        # Agrupar por carpeta lógica
        grupos = {}
        for r in self.reporte_consolidado:
            # Intentar deducir el nombre de la "Colección" o "Segmento"
            # Si es consolidado, viene de config['carpetas_consolidadas']
            # Si es individual, viene de config['carpetas_individuales']

            ruta_dest = r["ruta_destino"]
            # data/output_normalized/consolidados/BD_Historico.xlsx
            # data/output_normalized/individuales/2023_Hist/archivo.xlsx

            partes = ruta_dest.replace("\\", "/").split("/")

            if "consolidados" in partes:
                # Usar el nombre del archivo de salida para identificar el grupo visualmente  # noqa: E501
                # O intentar recuperar el nombre de la carpeta origen del diccionario interno  # noqa: E501
                # Como simplificación, usaremos el nombre del archivo sin extensión  # noqa: E501
                grupo = os.path.splitext(r["archivo"])[0]
            elif "individuales" in partes:
                # El grupo es la subcarpeta dentro de 'individuales'
                idx = partes.index("individuales")
                if len(partes) > idx + 1:
                    grupo = partes[idx + 1]
                else:
                    grupo = "Otros"
            else:
                grupo = "General"

            if grupo not in grupos:
                grupos[grupo] = []
            grupos[grupo].append(r)

        try:
            os.makedirs(os.path.dirname(ruta_md), exist_ok=True)
            with open(ruta_md, "w", encoding="utf-8") as f:
                f.write("# REPORTE DE INGESTA AUTOMATIZADA\n")
                f.write(
                    f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                f.write("-" * 60 + "\n\n")

                # Ordenar grupos alfabéticamente
                for carpeta in sorted(grupos.keys()):
                    registros = grupos[carpeta]
                    f.write(f"## Carpeta: {carpeta}\n")
                    f.write(
                        "| Archivo | Hoja Detectada | Estado | Filas | Sin Fecha |\n"  # noqa: E501
                    )
                    f.write("|---|---|---|---|---|\n")
                    for reg in registros:
                        estado = "✅ Procesado"
                        # Limpiar nombre de hoja
                        hoja = str(reg.get("hoja_origen", "N/A")).strip()
                        f.write(
                            f"| {reg['archivo']} | {hoja} | {estado} | {reg['filas']} | {reg.get('filas_sin_fecha', 0)} |\n"  # noqa: E501
                        )
                    f.write("\n")
            print(f"📄 Reporte MD actualizado: {ruta_md}")
        except Exception as e:
            print(f"⚠️ No se pudo actualizar REPORTE_INGESTA.md: {e}")
