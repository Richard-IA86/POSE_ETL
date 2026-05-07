import argparse
import json
import os
import shutil
import sys

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
        description="Alineador de archivos para ingesta (Staging) - Grupo POSE",  # noqa: E501
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--segmento",
        nargs="+",
        metavar="SEGMENTO",
        choices=SEGMENTOS_VALIDOS,
        help=(
            "Segmento(s) a alinear. Si se omite, se alinean todos.\n"
            f'Valores válidos: {chr(10).join("  " + s for s in SEGMENTOS_VALIDOS)}\n'  # noqa: E501
            "Ejemplos:\n"
            "  --segmento 2025_Corriente\n"
            "  --segmento 2025_Corriente 2025_Ajustes"
        ),
    )
    return parser.parse_args()


def cargar_configuracion():
    """Carga configuración base para saber rutas de origen"""
    ruta_config = "config/config_normalizador.json"
    with open(ruta_config, "r", encoding="utf-8") as f:
        return json.load(f)


def asegurar_directorio(ruta):
    if not os.path.exists(ruta):
        os.makedirs(ruta)


def main():
    print("=" * 70)
    print("ALINEACIÓN DE ARCHIVOS PARA INGESTA (STAGING)")
    print("=" * 70)

    # 1. Rutas Base
    # Resuelve dinámicamente: normalizador/ -> Pre_IngestaBD/ -> raíz del proyecto  # noqa: E501
    base_dir = os.path.dirname(  # noqa: F841
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )  # noqa: F841
    origen_norm = "data/output_normalized"

    # Esta será la carpeta "espejo" lista para que PQ la consuma (o para copiar a prod)  # noqa: E501
    # Usamos un nombre claro que indique que es para Ingesta
    destino_stage = "data/output_ready_for_pq"

    print(f"Origen : {origen_norm}")
    print(f"Destino: {destino_stage}\n")

    # Parsear argumentos
    args = parsear_argumentos()

    # 2. Definición del Mapa de Distribución
    # Qué archivo/carpeta de origen va a qué carpeta de destino
    distribucion = [
        {
            "origen_tipo": "consolidado",
            "archivo": "BD_Historico_2021_2022.xlsx",
            "carpeta_destino": "2021_2022_Historico",
        },
        {
            "origen_tipo": "consolidado",
            "archivo": "BD_Modificaciones.xlsx",
            "carpeta_destino": "Modificaciones",
        },
        {
            "origen_tipo": "consolidado",
            "archivo": "BD_Compensaciones_2025.xlsx",
            "carpeta_destino": "2025_Compensaciones",  # PQ no tiene esta carpeta aún en estructura base, pero la creamos si hace falta  # noqa: E501
            # NOTA: En tu estructura actual Compensaiones se une a alguna parte?  # noqa: E501
            # Revisitando estructura PQ: Parece que "2025_Compensaciones" no está en CarpetasObjetivo del .pq  # noqa: E501
            # CarpetasObjetivo = {"2021_2022_Historico", "2023_2025_Hist", "2025_Ajustes", "2025_Corriente"}  # noqa: E501
            # ¿Debería ir a Ajustes? Lo pondré en su propia por ahora.
        },
        {
            "origen_tipo": "individual",
            "carpeta_origen": "2023_2025_Hist",
            "carpeta_destino": "2023_2025_Hist",
        },
        {
            "origen_tipo": "individual",
            "carpeta_origen": "2025_Ajustes",
            "carpeta_destino": "2025_Ajustes",
        },
        {
            "origen_tipo": "individual",
            "carpeta_origen": "2025",
            "carpeta_destino": "2025",
        },
        {
            "origen_tipo": "individual",
            "carpeta_origen": "2026",
            "carpeta_destino": "2026",
        },
    ]

    # Filtrar segmentos si se especificaron por argumento
    if args.segmento:
        segmentos = args.segmento
        print(f"🎯 Modo segmentado — alineando: {', '.join(segmentos)}\n")
        distribucion = [
            item
            for item in distribucion
            if item["carpeta_destino"] in segmentos
        ]
        if not distribucion:
            print(f"❌ Ningún segmento coincidió con: {segmentos}")
            sys.exit(1)
    else:
        print("🔄 Modo completo — alineando todos los segmentos\n")

    # Limpieza inteligente:
    # - Modo segmentado: solo borra la subcarpeta del segmento (preserva el resto del staging)  # noqa: E501
    # - Modo completo: borra todo output_ready_for_pq/ y lo recrea limpio
    if args.segmento:
        for item in distribucion:
            ruta_segmento = os.path.join(
                destino_stage, item["carpeta_destino"]
            )
            if os.path.exists(ruta_segmento):
                print(f"🧹 Limpiando segmento: {item['carpeta_destino']}/")
                shutil.rmtree(ruta_segmento)
    else:
        if os.path.exists(destino_stage):
            print("🧹 Limpiando directorio de destino completo...")
            shutil.rmtree(destino_stage)
    asegurar_directorio(destino_stage)

    total_copiados = 0

    for item in distribucion:
        ruta_dst_final = os.path.join(destino_stage, item["carpeta_destino"])
        asegurar_directorio(ruta_dst_final)

        if item["origen_tipo"] == "consolidado":
            # Copiar un solo archivo
            src = os.path.join(origen_norm, "consolidados", item["archivo"])
            dst = os.path.join(ruta_dst_final, item["archivo"])

            if os.path.exists(src):
                print(
                    f"📄 Copiando CONSOLIDADO: {item['archivo']} -> {item['carpeta_destino']}/"  # noqa: E501
                )
                shutil.copy2(src, dst)
                total_copiados += 1
            else:
                print(f"⚠️  ADVERTENCIA: No se encontró origen {src}")

        elif item["origen_tipo"] == "individual":
            # Copiar todo el contenido de una carpeta
            src_folder = os.path.join(
                origen_norm, "individuales", item["carpeta_origen"]
            )

            if os.path.exists(src_folder):
                archivos = [
                    f for f in os.listdir(src_folder) if f.endswith(".xlsx")
                ]
                print(
                    f"📂 Copiando CARPETA: {item['carpeta_origen']} ({len(archivos)} archivos) -> {item['carpeta_destino']}/"  # noqa: E501
                )

                for arch in archivos:
                    shutil.copy2(
                        os.path.join(src_folder, arch),
                        os.path.join(ruta_dst_final, arch),
                    )
                    total_copiados += 1
            else:
                print(
                    f"⚠️  ADVERTENCIA: No se encontró carpeta origen {src_folder}"  # noqa: E501
                )

    print("=" * 70)
    print(
        f"✅ Alineación completada. Total archivos preparados: {total_copiados}"
    )
    print(f"Listo para consumo en: {destino_stage}")
    print("=" * 70)


if __name__ == "__main__":
    main()
