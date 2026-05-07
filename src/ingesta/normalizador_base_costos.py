"""
NORMALIZADOR DE COSTOS - Sistema de Ingesta Limpio
Versión 2.0 - Sin compilar, ejecución directa desde terminal
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Agregar directorio del script al path (reader, transformer, writer están en la misma carpeta)  # noqa: E501
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.ingesta.reader import ExcelReader  # noqa: E402
from src.ingesta.transformer import DataTransformer  # noqa: E402
from src.ingesta.writer import ExcelWriter  # noqa: E402

SEGMENTOS_VALIDOS = [
    "2021_2022_Historico",
    "Modificaciones",
    "2025_Compensaciones",
    "2023_2025_Hist",
    "2025_Ajustes",
    "2025",
    "2026",
]


def parsear_argumentos():
    """Parsea argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Normalizador de Costos - Grupo POSE",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--segmento",
        nargs="+",
        metavar="SEGMENTO",
        choices=SEGMENTOS_VALIDOS,
        help=(
            "Segmento(s) a procesar. Si se omite, se procesan todos.\n"
            f'Valores válidos: {chr(10).join("  " + s for s in SEGMENTOS_VALIDOS)}\n'  # noqa: E501
            "Ejemplos:\n"
            "  --segmento 2025_Corriente\n"
            "  --segmento 2025_Corriente 2025_Ajustes"
        ),
    )
    return parser.parse_args()


def cargar_configuracion():
    """Carga configuración desde config.json"""
    ruta_config = "config/config_normalizador.json"

    if not os.path.exists(ruta_config):
        print(f"❌ No se encontró config.json en: {ruta_config}")
        sys.exit(1)

    with open(ruta_config, "r", encoding="utf-8") as f:
        return json.load(f)


def mostrar_banner():
    """Muestra banner inicial"""
    print("\n" + "=" * 70)
    print("NORMALIZADOR DE COSTOS - GRUPO POSE")
    print("=" * 70)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def procesar_carpetas_consolidadas(config, reader, transformer, writer):
    """Procesa carpetas que se consolidan en un solo archivo"""
    print("📂 ESTRATEGIA 1: CARPETAS CONSOLIDADAS")
    print("=" * 70)

    carpetas_consolidadas = config["carpetas_consolidadas"]

    for nombre_carpeta, info in carpetas_consolidadas.items():
        print(f"\n📁 Procesando: {nombre_carpeta}...")

        # C2: Si hay lista explícita de archivos, leer solo esos; si no, leer toda la carpeta  # noqa: E501
        archivos_incluidos = info.get("archivos_incluidos", [])
        if archivos_incluidos:
            print(
                f"  📋 Modo C2 — archivos_incluidos: {len(archivos_incluidos)} archivo(s) definidos en config"  # noqa: E501
            )
            dataframes = []
            for nombre_archivo in archivos_incluidos:
                ruta_archivo = os.path.join(info["input"], nombre_archivo)
                if os.path.exists(ruta_archivo):
                    df = reader.leer_archivo(ruta_archivo)
                    if df is not None:
                        dataframes.append(df)
                else:
                    print(
                        f"  ⚠️  Archivo no encontrado (ignorado): {nombre_archivo}"  # noqa: E501
                    )
        else:
            # Comportamiento original: leer toda la carpeta
            dataframes = reader.leer_carpeta(info["input"])

        if not dataframes:
            print(f"  ⚠️ No se encontraron archivos en {nombre_carpeta}")
            continue

        # Consolidar y limpiar
        modo_dup = config.get("politica_duplicados", {}).get(
            nombre_carpeta, "soft"
        )
        df_consolidado = transformer.consolidar_dataframes(
            dataframes, modo_duplicados=modo_dup
        )

        # Aplicar filtro por año si está configurado
        if (
            "filtros_anio" in config
            and nombre_carpeta in config["filtros_anio"]
        ):
            anios_permitidos = config["filtros_anio"][nombre_carpeta]
            df_consolidado = transformer.filtrar_por_anio(
                df_consolidado, anios_permitidos
            )

        if df_consolidado is not None:
            # Guardar archivo consolidado
            ruta_salida = config["rutas_salida"]["consolidados"]
            # Extraer todos los archivos de origen únicos
            if "_ARCHIVO_ORIGEN" in df_consolidado.columns:
                archivos_origen = (
                    df_consolidado["_ARCHIVO_ORIGEN"].unique().tolist()
                )
                archivo_origen = archivos_origen  # Pasar lista completa
            else:
                archivo_origen = [nombre_carpeta]

            # Extraer ruta de la carpeta de origen
            ruta_origen = info["input"]

            # Obtener estadísticas antes de guardar
            stats = transformer.get_stats()

            writer.guardar_archivo(
                df_consolidado,
                ruta_salida,
                info["output"],
                ruta_origen,
                archivo_origen,
                stats,
            )

            # Mostrar estadísticas
            if (
                stats["duplicados_origen"] > 0
                or stats["duplicados_proceso"] > 0
            ):
                print(
                    f"  📊 Duplicados origen: {stats['duplicados_origen']} | "
                    f"Duplicados proceso eliminados: {stats['duplicados_proceso']}"  # noqa: E501
                )

            # Resetear estadísticas para siguiente carpeta consolidada
            transformer.reset_stats()


def procesar_carpetas_individuales(config, reader, transformer, writer):
    """Procesa carpetas manteniendo archivos individuales"""
    print("\n\n📂 ESTRATEGIA 2: CARPETAS INDIVIDUALES (Trazabilidad)")
    print("=" * 70)

    carpetas_individuales = config["carpetas_individuales"]

    for nombre_carpeta, info in carpetas_individuales.items():
        print(f"\n📁 Procesando: {nombre_carpeta}...")

        ruta_input = info["input"]

        if not os.path.exists(ruta_input):
            print(f"  ❌ Carpeta no encontrada: {ruta_input}")
            continue

        # Obtener lista de archivos
        archivos = [
            f
            for f in os.listdir(ruta_input)
            if f.endswith((".xlsx", ".xls")) and not f.startswith("~$")
        ]
        archivos.sort()

        # Crear subcarpeta de salida
        ruta_salida = os.path.join(
            config["rutas_salida"]["individuales"], info["output_folder"]
        )
        os.makedirs(ruta_salida, exist_ok=True)

        # Procesar cada archivo individualmente
        for archivo in archivos:
            ruta_completa = os.path.join(ruta_input, archivo)
            df = reader.leer_archivo(ruta_completa)

            if df is not None:
                # Normalizar
                df = transformer.normalizar_columnas(df)
                modo_dup_ind = config.get("politica_duplicados", {}).get(
                    nombre_carpeta, "soft"
                )
                df = transformer.detectar_duplicados(df, modo=modo_dup_ind)

                # Extraer datos de auditoría (para individuales es un solo archivo)  # noqa: E501
                archivo_origen = (
                    df["_ARCHIVO_ORIGEN"].iloc[0]
                    if "_ARCHIVO_ORIGEN" in df.columns and not df.empty
                    else archivo
                )
                ruta_origen = (
                    df["_RUTA_ORIGEN"].iloc[0]
                    if "_RUTA_ORIGEN" in df.columns and not df.empty
                    else ruta_completa
                )

                # Obtener estadísticas de este archivo
                stats_archivo = transformer.get_stats()

                # Guardar (pasar como lista para mantener consistencia)
                writer.guardar_archivo(
                    df,
                    ruta_salida,
                    archivo,
                    ruta_origen,
                    [archivo_origen],
                    stats_archivo,
                )

                # Resetear estadísticas para siguiente archivo
                transformer.reset_stats()


def main():
    """Función principal"""
    try:
        mostrar_banner()

        # Cargar configuración
        config = cargar_configuracion()
        print("✅ Configuración cargada correctamente\n")

        # Filtrar segmentos si se especificaron por argumento
        args = parsear_argumentos()
        if args.segmento:
            segmentos = args.segmento
            print(f"🎯 Modo segmentado — procesando: {', '.join(segmentos)}\n")
            config["carpetas_consolidadas"] = {
                k: v
                for k, v in config["carpetas_consolidadas"].items()
                if k in segmentos
            }
            config["carpetas_individuales"] = {
                k: v
                for k, v in config["carpetas_individuales"].items()
                if k in segmentos
            }
            if "filtros_anio" in config:
                config["filtros_anio"] = {
                    k: v
                    for k, v in config["filtros_anio"].items()
                    if k in segmentos
                }
            total_match = len(config["carpetas_consolidadas"]) + len(
                config["carpetas_individuales"]
            )
            if total_match == 0:
                print(f"❌ Ningún segmento coincidió con: {segmentos}")
                sys.exit(1)
        else:
            print("🔄 Modo completo — procesando todos los segmentos\n")

        # Inicializar componentes
        reader = ExcelReader()
        transformer = DataTransformer()
        schema_cfg = config.get("schema_contract", {})
        writer = ExcelWriter(
            config["rutas_salida"]["reportes"],
            schema_cols=schema_cfg.get("columnas", []),
            schema_drop_extra=schema_cfg.get("drop_extra", False),
        )

        # Procesar carpetas consolidadas
        procesar_carpetas_consolidadas(config, reader, transformer, writer)

        # Procesar carpetas individuales
        procesar_carpetas_individuales(config, reader, transformer, writer)

        # Generar reporte final
        print("\n" + "=" * 70)
        print("FINALIZANDO PROCESO")
        print("=" * 70)

        # Calcular totales desde reporte consolidado
        total_dup_origen = sum(
            r.get("dup_origen", 0) for r in writer.reporte_consolidado
        )
        total_dup_proceso = sum(
            r.get("dup_proceso", 0) for r in writer.reporte_consolidado
        )
        stats_totales = {
            "duplicados_origen": total_dup_origen,
            "duplicados_proceso": total_dup_proceso,
            "filas_eliminadas": total_dup_proceso,
        }

        writer.generar_reporte_final(stats_totales)

        # Resumen final visible
        print("\n" + "=" * 70)
        print("🎉 RESUMEN FINAL")
        print("=" * 70)
        n_consolidados = len(config["carpetas_consolidadas"])
        n_individuales = len(writer.reporte_consolidado) - n_consolidados
        print(
            f"📁 Total archivos procesados: {len(writer.reporte_consolidado)}"
        )
        print(f"   • Consolidados: {n_consolidados} segmento(s)")
        print(f"   • Individuales: {n_individuales} archivo(s)")
        print("\n📊 Estadísticas de duplicados:")
        print(
            f"   • Duplicados origen detectados: {total_dup_origen} (mantenidos)"  # noqa: E501
        )
        print(f"   • Duplicados proceso eliminados: {total_dup_proceso}")

        # Calcular total de filas
        total_filas = sum(r["filas"] for r in writer.reporte_consolidado)
        print(f"\n📊 Total registros exportados: {total_filas:,}")
        print("=" * 70)

        print("\n✅ Proceso completado exitosamente")
        print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ ERROR CRÍTICO: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
