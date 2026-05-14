"""
=============================================================================
AUTOMATIZACIÓN ETL - FASE 2 Y 3 (POWER QUERY)
=============================================================================
Automatiza la actualización de modelos Excel con Power Query:
  - Fase 2: BaseCostoUnificada.xlsx (ETL principal)
  - Fase 3: BaseCostosPOSE.xlsx (Reportes OneDrive)

Características:
  ✅ Ejecución invisible (Excel no se muestra)
  ✅ Indicador visual de progreso con cronómetro
  ✅ Refresh secuencial por capas de dependencia PQ (evita race condition)
  ✅ Sistema de logs persistente
  ✅ Backup automático antes de sobrescribir
  ✅ Timeout configurable para evitar cuelgues infinitos
  ✅ Rotación automática de backups antiguos
  ✅ Configuración externalizada en JSON

Autor: GitHub Copilot + Richard
Fecha: 13 de marzo de 2026
Versión: 2.0 (Producción)
=============================================================================
"""

import win32com.client as win32
import pythoncom
import os
import sys
import time
import json
import shutil
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURACIÓN GLOBAL
# ============================================================================

# Ruta del archivo de configuración
CONFIG_FILE = Path("config/config_automatizacion.json")

# Variable global para controlar la animación
esta_cargando = False


# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================


def cargar_configuracion():
    """Carga la configuración desde el archivo JSON."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(
            f"❌ ERROR: No se encontró el archivo de configuración: {CONFIG_FILE}"  # noqa: E501
        )
        print("   Por favor, cree el archivo config_automatizacion.json")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(
            "❌ ERROR: El archivo de configuración tiene errores de sintaxis JSON:"  # noqa: E501
        )
        print(f"   {str(e)}")
        sys.exit(1)


def configurar_logging(config):
    """Configura el sistema de logging según la configuración."""
    if not config["logs"]["habilitar"]:
        return None

    # Crear carpeta de logs si no existe
    carpeta_logs = Path(config["rutas"]["carpeta_logs"])
    carpeta_logs.mkdir(parents=True, exist_ok=True)

    # Archivo de log con timestamp
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = carpeta_logs / f"actualizacion_pq_{timestamp}.log"

    # Configurar logging
    logging.basicConfig(
        level=getattr(logging, config["logs"]["nivel"]),
        format=config["logs"]["formato"],
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return logging.getLogger(__name__)


def crear_backup(ruta_archivo, carpeta_backups):
    """Crea un backup del archivo antes de sobrescribirlo."""
    try:
        ruta_origen = Path(ruta_archivo)
        if not ruta_origen.exists():
            return None

        carpeta_backups = Path(carpeta_backups)
        carpeta_backups.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_backup = (
            f"{ruta_origen.stem}_backup_{timestamp}{ruta_origen.suffix}"
        )
        ruta_backup = carpeta_backups / nombre_backup

        shutil.copy2(ruta_origen, ruta_backup)

        logging.info(f"📦 Backup creado: {ruta_backup.name}")
        print(f"📦 Backup creado: {ruta_backup.name}")

        return ruta_backup
    except Exception as e:
        logging.warning(f"⚠️ No se pudo crear backup: {str(e)}")
        print(f"⚠️ No se pudo crear backup: {str(e)}")
        return None


def limpiar_backups_antiguos(carpeta_backups, dias_retencion):
    """Elimina backups más antiguos que el período de retención."""
    try:
        carpeta = Path(carpeta_backups)
        if not carpeta.exists():
            return

        fecha_limite = datetime.now() - timedelta(days=dias_retencion)
        archivos_eliminados = 0

        for archivo in carpeta.glob("*_backup_*.xlsx"):
            # Extraer timestamp del nombre del archivo
            try:
                # Formato: NombreArchivo_backup_YYYYMMDD_HHMMSS.xlsx
                partes = archivo.stem.split("_backup_")
                if len(partes) == 2:
                    timestamp_str = partes[1]  # YYYYMMDD_HHMMSS
                    fecha_backup = datetime.strptime(
                        timestamp_str, "%Y%m%d_%H%M%S"
                    )

                    if fecha_backup < fecha_limite:
                        archivo.unlink()
                        archivos_eliminados += 1
                        logging.info(
                            f"🗑️ Backup antiguo eliminado: {archivo.name}"
                        )
            except (ValueError, IndexError):
                # Si no se puede parsear la fecha, ignorar el archivo
                continue

        if archivos_eliminados > 0:
            print(
                f"🗑️ Se eliminaron {archivos_eliminados} backup(s) antiguo(s)"
            )

    except Exception as e:
        logging.warning(f"⚠️ Error al limpiar backups: {str(e)}")


# =============================================================================
# ORDEN DE REFRESCO POR NIVELES DE DEPENDENCIA
# =============================================================================
# Causa del bug reportado: wb.RefreshAll() dispara TODAS las conexiones en
# paralelo como background jobs. Archiv_Consolidado_Final evaluaba su cadena
# interna (Crudo→Modif→Final) y terminaba ANTES de que la tabla independiente
# de Archiv_Consolidado_Crudo terminara su propia evaluación.
#
# Riesgo: si los archivos fuente cambian durante el refresh, Final y la tabla
# Crudo pueden mostrar datos distintos (evaluaciones de fuente independientes).
#
# Solución: refresh capa por capa. CalculateUntilAsyncQueriesDone() entre
# capas garantiza que cada nivel upstream completó antes del siguiente.
# =============================================================================
CAPAS_REFRESH_PQ = [
    # Capa 1: Conexión maestro (base de Dims)
    ["Con_Maestros"],
    # Capa 2: Dimensiones (independientes entre sí, dependen de Con_Maestros)
    [
        "Dim_Obras",
        "Dim_Calendario_TC",
        "Dim_TipComprobante",
        "Dim_Fuentes",
        "Dim_Excepciones",
    ],
    # Capa 3: Fuentes brutas (deben terminar ANTES que Modif y Final)
    [
        "Archiv_Consolidado_Crudo",  # ⚠️ CRÍTICO: upstream de Modif y Final
        "Archiv_Altas_Manuales",
        "Analisis_Filas_Descartadas",
    ],
    # Capa 4: Gestión (depende de Crudo + Altas)
    ["Archiv_Consolidado_Modif"],
    # Capa 5: Final enriquecido (depende de Modif + todas las Dims)
    ["Archiv_Consolidado_Final"],
    # Capa 6: Reservorio OneDrive — única conexión en BaseCostosPOSE.xlsx
    # (downstream Fase 2; en BaseCostoUnificada.xlsx no existe → omitible)
    ["BaseCostoPOSE"],
]


def refresh_secuencial_pq(wb, excel, timeout_seconds, logger):
    """
    Refresca conexiones Power Query respetando el orden de dependencia.

    Reemplaza a wb.RefreshAll() para evitar que consultas downstream (ej:
    Archiv_Consolidado_Final) terminen antes que sus dependencias upstream
    (ej: Archiv_Consolidado_Crudo) cuando Excel las evalúa en paralelo.

    Queries marcados como 'Connection Only' (no cargados) no aparecen en
    wb.Connections y son omitidos sin error; PQ los evalúa como parte de la
    cadena de sus dependientes.

    Args:
        wb: Workbook de Excel (objeto COM)
        excel: Aplicación Excel (objeto COM)
        timeout_seconds: Tiempo máximo de espera acumulado (referencia)
        logger: Logger configurado

    Returns:
        dict: {"refrescadas": [...], "no_encontradas": [...], "errores": [...]}
    """
    resultado = {"refrescadas": [], "no_encontradas": [], "errores": []}

    # Mapear conexiones disponibles (nombre limpio ↔ objeto COM)
    # Power Query genera nombres con prefijo "Query - " (Excel inglés)
    # o "Consulta - " (Excel español). Se normaliza para que las claves
    # del mapa coincidan con los nombres en CAPAS_REFRESH_PQ.
    PREFIJOS_PQ = ("Query - ", "Consulta - ")
    mapa_conexiones = {}
    for conn in wb.Connections:
        nombre_limpio = conn.Name
        for prefijo in PREFIJOS_PQ:
            if nombre_limpio.startswith(prefijo):
                nombre_limpio = nombre_limpio[len(prefijo) :]
                break
        mapa_conexiones[nombre_limpio] = conn

    logger.info(
        f"Conexiones cargadas en workbook: {sorted(mapa_conexiones.keys())}"
    )

    total_capas = len(CAPAS_REFRESH_PQ)
    for num_capa, queries_capa in enumerate(CAPAS_REFRESH_PQ, start=1):
        presentes = [q for q in queries_capa if q in mapa_conexiones]
        ausentes = [q for q in queries_capa if q not in mapa_conexiones]

        for q in ausentes:
            logger.debug(
                f"Capa {num_capa}: '{q}' no encontrada en wb.Connections "
                "(probablemente 'Connection Only' — se omite)."
            )
            resultado["no_encontradas"].append(q)

        if not presentes:
            logger.debug(
                f"Capa {num_capa}/{total_capas}: "
                "sin conexiones cargadas, se omite."
            )
            continue

        logger.info(f"Capa {num_capa}/{total_capas}: {presentes}")
        print(f"   └─ Capa {num_capa}/{total_capas}: {', '.join(presentes)}")

        inicio_capa = time.time()
        for nombre_query in presentes:
            conn = mapa_conexiones[nombre_query]
            try:
                conn.Refresh()
                resultado["refrescadas"].append(nombre_query)
            except Exception as e:
                logger.error(f"  ❌ Error refrescando '{nombre_query}': {e}")
                resultado["errores"].append(nombre_query)

        # Esperar que todos los jobs async de esta capa terminen antes de
        # iniciar la siguiente. Esto es la garantía clave del orden.
        excel.CalculateUntilAsyncQueriesDone()
        duracion_capa = int(time.time() - inicio_capa)
        logger.info(f"  ✅ Capa {num_capa} completada en {duracion_capa}s")

    # Alerta real: si ninguna conexión fue refrescada, el workbook está vacío
    # o CAPAS_REFRESH_PQ no contiene ningún nombre que exista aquí.
    if not resultado["refrescadas"]:
        logger.warning(
            "⚠️ Ninguna conexión fue refrescada en este workbook. "
            "Verificar que CAPAS_REFRESH_PQ tenga nombres activos. "
            f"Disponibles: {sorted(mapa_conexiones.keys())}"
        )

    return resultado


def animacion_espera(mensaje, timeout_seconds):
    """Muestra un indicador giratorio mientras Power Query trabaja."""
    caracteres = ["|", "/", "-", "\\"]
    i = 0
    inicio = time.time()

    while esta_cargando:
        tiempo_transcurrido = int(time.time() - inicio)
        minutos, segundos = divmod(tiempo_transcurrido, 60)

        # Verificar timeout
        if tiempo_transcurrido >= timeout_seconds:
            sys.stdout.write(
                f"\r⏰ TIMEOUT alcanzado ({timeout_seconds}s)                    \n"  # noqa: E501
            )
            sys.stdout.flush()
            break

        sys.stdout.write(
            f"\r{mensaje} {caracteres[i % 4]} (Tiempo: {minutos:02d}:{segundos:02d})"  # noqa: E501
        )
        sys.stdout.flush()
        time.sleep(0.2)
        i += 1

    sys.stdout.write("\r" + " " * 100 + "\r")  # Limpia la línea al terminar
    sys.stdout.flush()


def actualizar_excel(nombre_fase, ruta_archivo, config, logger):
    """
    Actualiza un archivo Excel ejecutando RefreshAll en Power Query.

    Args:
        nombre_fase: Nombre descriptivo de la fase (ej: "FASE 2: ETL UNIFICADA")  # noqa: E501
        ruta_archivo: Ruta completa al archivo Excel
        config: Diccionario con la configuración cargada
        logger: Logger configurado

    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    global esta_cargando

    ruta_absoluta = os.path.abspath(ruta_archivo)

    logger.info(f"{'='*70}")
    logger.info(f"[{nombre_fase}] Iniciando proceso...")
    print(f"\n{'='*70}")
    print(f"[{nombre_fase}] Abriendo modelo...")

    # Verificar existencia del archivo
    if not os.path.exists(ruta_absoluta):
        logger.error(
            f"❌ ERROR: No se encontró el archivo:\n   {ruta_absoluta}"
        )
        print(
            f"❌ ERROR: No se encontró el archivo en la ruta:\n   {ruta_absoluta}"  # noqa: E501
        )
        return False

    # Crear backup si está habilitado
    if config["opciones"]["crear_backup"]:
        crear_backup(ruta_absoluta, config["rutas"]["carpeta_backups"])

    excel = None
    wb = None

    try:
        # Inicializar COM
        pythoncom.CoInitialize()

        # Iniciar Excel de forma invisible
        logger.info("Iniciando aplicación Excel (modo invisible)...")
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = config["opciones"]["excel_visible"]
        excel.DisplayAlerts = False

        logger.info(f"Abriendo archivo: {Path(ruta_absoluta).name}")
        wb = excel.Workbooks.Open(ruta_absoluta)

        # Iniciar animación en hilo separado
        timeout_seconds = config["opciones"]["timeout_minutos"] * 60
        esta_cargando = True
        hilo_animacion = threading.Thread(
            target=animacion_espera,
            args=(
                "🔄 Ejecutando consultas de Power Query...",
                timeout_seconds,
            ),
        )
        hilo_animacion.daemon = True
        hilo_animacion.start()

        # ⭐ EJECUCIÓN POWER QUERY — REFRESH SECUENCIAL POR DEPENDENCIA
        # Se usa refresh_secuencial_pq() en vez de RefreshAll() para garantizar
        # que cada capa upstream (ej: Archiv_Consolidado_Crudo) complete antes
        # de iniciar la capa downstream (ej: Archiv_Consolidado_Final).
        # Ver CAPAS_REFRESH_PQ para el orden definido.
        logger.info(
            "Ejecutando refresh secuencial por capas de dependencia PQ..."
        )
        inicio_refresh = time.time()

        resultado_refresh = refresh_secuencial_pq(
            wb, excel, timeout_seconds, logger
        )

        if resultado_refresh["errores"]:
            errores_refresh = resultado_refresh["errores"]
            logger.warning(
                f"Conexiones con error durante refresh: {errores_refresh}"
            )

        tiempo_refresh = int(time.time() - inicio_refresh)
        minutos, segundos = divmod(tiempo_refresh, 60)

        # Detener animación
        esta_cargando = False
        hilo_animacion.join(timeout=1)

        logger.info(
            f"✅ Actualización completada en {minutos:02d}:{segundos:02d}"
        )
        print(
            f"✅ ¡Actualización de datos completada en {minutos:02d}:{segundos:02d}!"  # noqa: E501
        )

        # Guardar cambios
        logger.info("💾 Guardando cambios...")
        print("💾 Guardando cambios...")

        # Capturar timestamp ANTES de guardar para verificación post-save
        mtime_antes = os.path.getmtime(ruta_absoluta)

        wb.Save()

        # ⚠️ VERIFICACIÓN CRÍTICA: confirmar que el archivo en disco fue realmente  # noqa: E501
        # actualizado. Con ficheros en OneDrive, wb.Save() puede retornar OK pero el  # noqa: E501
        # cliente de sync puede conservar el archivo bloqueado y no escribir los bytes.  # noqa: E501
        time.sleep(2)  # pequeña espera para que OneDrive finalice el flush
        mtime_despues = os.path.getmtime(ruta_absoluta)

        if mtime_despues <= mtime_antes:
            msg = (
                f"❌ FALLO SILENCIOSO DE GUARDADO: wb.Save() reportó éxito pero "  # noqa: E501
                f"el archivo NO fue modificado en disco.\n"
                f"   Ruta      : {ruta_absoluta}\n"
                f"   mtime pre : {datetime.fromtimestamp(mtime_antes)}\n"
                f"   mtime post: {datetime.fromtimestamp(mtime_despues)}\n"
                f"   Causa probable: OneDrive tenía el archivo bloqueado para sync.\n"  # noqa: E501
                f"   Acción    : Abra el archivo manualmente, refresque y guarde."  # noqa: E501
            )
            logger.error(msg)
            print(f"\n{msg}")
            return False

        logger.info(
            "\u2705 Archivo guardado exitosamente "
            f"(mtime: {datetime.fromtimestamp(mtime_despues)})"
        )
        print("✅ Archivo guardado exitosamente")

        return True

    except Exception as e:
        esta_cargando = False
        error_msg = str(e)
        logger.error(f"❌ FATAL ERROR: {error_msg}")
        print("\n❌ FATAL ERROR: Power Query falló o Excel se atascó.")
        print(f"Detalle del error: {error_msg}")
        print(
            "Sugerencia: Abra el archivo manualmente "
            "para ver si hay un error en las consultas."
        )
        return False

    finally:
        # Limpieza de recursos COM
        try:
            if wb:
                wb.Close(SaveChanges=False)
            if excel:
                excel.Quit()
        except Exception:
            pass

        # Liberar COM
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================


def main():
    """Función principal del script."""

    # Limpiar pantalla
    os.system("cls" if os.name == "nt" else "clear")

    print("=" * 70)
    print("☕ SISTEMA DE ACTUALIZACIÓN AUTOMÁTICA POWER QUERY ☕")
    print("=" * 70)
    print("Versión: 2.0 (Producción)")
    print("Fecha: 13 de marzo de 2026")
    print("=" * 70)

    # Cargar configuración
    config = cargar_configuracion()
    logger = configurar_logging(config)

    if logger:
        logger.info("=" * 70)
        logger.info("INICIO DE PROCESO DE AUTOMATIZACIÓN ETL")
        logger.info("=" * 70)
        logger.info(f"Configuración cargada desde: {CONFIG_FILE}")

    # Limpiar backups antiguos
    if config["opciones"]["crear_backup"]:
        limpiar_backups_antiguos(
            config["rutas"]["carpeta_backups"],
            config["opciones"]["dias_retencion_backup"],
        )

    tiempo_total_inicio = time.time()

    # ========================================================================
    # FASE 2: Base Costo Unificada
    # ========================================================================
    archivo_base = config["rutas"]["base_costo_unificada"]
    exito_base = actualizar_excel(
        "FASE 2: ETL UNIFICADA", archivo_base, config, logger
    )

    if not exito_base:
        print("\n⚠️ Se abortó el proceso por un error en la Fase 2.")
        if logger:
            logger.error("Proceso abortado por error en Fase 2")
        sys.exit(1)

    # ========================================================================
    # TRANSICIÓN ENTRE FASES
    # ========================================================================
    pausa = config["opciones"]["pausa_entre_archivos_segundos"]
    print(f"\n[INFO] Transición intermedia ({pausa}s)...")
    if logger:
        logger.info(f"Pausa de {pausa}s entre fases")
    time.sleep(pausa)

    # ========================================================================
    # FASE 3: Refrescar BaseCostosPOSE.xlsx (lee Archiv_Consolidado_Final
    # desde BaseCostoUnificada.xlsx y carga los datos para ingesta A2)
    # ========================================================================
    print(f"\n{'='*70}")
    print("[FASE 3: REPORTING RESERVORIO] Iniciando...")
    if logger:
        logger.info("=" * 70)
        logger.info(
            "[FASE 3] Refrescando BaseCostosPOSE.xlsx" " → output/director/"
        )

    exito_reservorio = False
    excel_fase3 = None
    wb_reservorio = None

    try:
        ruta_unificada = os.path.abspath(
            config["rutas"]["base_costo_unificada"]
        )
        ruta_reservorio = os.path.abspath(config["rutas"]["reservorio"])

        # Ruta vieja que puede estar hardcodeada dentro del xlsx
        ruta_vieja = str(
            Path(ruta_unificada).parent.parent
            / "Planif_POSE"
            / "power_query"
            / "BaseCostoUnificada.xlsx"
        )

        pythoncom.CoInitialize()
        excel_fase3 = win32.DispatchEx("Excel.Application")
        excel_fase3.Visible = False
        excel_fase3.DisplayAlerts = False

        if logger:
            logger.info(f"Abriendo: {Path(ruta_reservorio).name}")
        print(f"   Abriendo {Path(ruta_reservorio).name}...")
        wb_reservorio = excel_fase3.Workbooks.Open(ruta_reservorio)

        # Corregir ruta dentro de la query PQ embebida si apunta al repo viejo
        PREFIJOS_PQ = ("Query - ", "Consulta - ")
        for conn in wb_reservorio.Connections:
            nombre = conn.Name
            for p in PREFIJOS_PQ:
                if nombre.startswith(p):
                    nombre = nombre[len(p) :]
                    break
            try:
                formula = conn.OLEDBConnection.CommandText
                if isinstance(formula, str) and "Planif_POSE" in formula:
                    formula_nueva = formula.replace(ruta_vieja, ruta_unificada)
                    conn.OLEDBConnection.CommandText = formula_nueva
                    if logger:
                        logger.info(f"Ruta corregida en query '{nombre}'")
                    print(f"   Ruta corregida en query '{nombre}'")
            except Exception:
                pass

        # Fase 3: refrescar TODAS las conexiones del reservorio.
        # BaseCostosPOSE.xlsx tiene sus propias queries (distintas a
        # CAPAS_REFRESH_PQ, que es exclusiva de BaseCostoUnificada.xlsx).
        # Usar refresh_secuencial_pq aquí provocaba que todas las
        # conexiones quedasen en "no_encontradas" → nada se refrescaba
        # pero el script reportaba éxito (errores == []).
        PREFIJOS_PQ_F3 = ("Query - ", "Consulta - ")
        refrescadas_f3: list[str] = []
        errores_f3: list[str] = []
        for conn in wb_reservorio.Connections:
            nombre = conn.Name
            for p in PREFIJOS_PQ_F3:
                if nombre.startswith(p):
                    nombre = nombre[len(p) :]
                    break
            try:
                conn.Refresh()
                refrescadas_f3.append(nombre)
                logger.info(f"  Refrescando: '{nombre}'")
            except Exception as e:
                logger.error(f"  ❌ Error refrescando '{nombre}': {e}")
                errores_f3.append(nombre)

        excel_fase3.CalculateUntilAsyncQueriesDone()
        logger.info(
            f"Conexiones refrescadas: {refrescadas_f3} | "
            f"Errores: {errores_f3}"
        )

        if not refrescadas_f3:
            msg = (
                "❌ Fase 3 FALLÓ: ninguna conexión fue refrescada en "
                f"{Path(ruta_reservorio).name}. "
                "Verificar nombres de queries en el workbook."
            )
            logger.error(msg)
            print(f"\n{msg}")
            exito_reservorio = False
        elif errores_f3:
            logger.warning(f"Conexiones con error: {errores_f3}")
            exito_reservorio = False
        else:
            exito_reservorio = True

        # xlOpenXMLWorkbook = 51 → .xlsx sin macros
        if exito_reservorio:
            if logger:
                logger.info(f"Guardando: {ruta_reservorio}")
            print(f"   Guardando {Path(ruta_reservorio).name}...")
            wb_reservorio.SaveAs(ruta_reservorio, FileFormat=51)

        wb_reservorio.Close(SaveChanges=False)
        excel_fase3.Quit()

        if exito_reservorio:
            if logger:
                logger.info(
                    "✅ Fase 3 completada — BaseCostosPOSE.xlsx actualizado"
                )
            print("✅ Fase 3 completada — BaseCostosPOSE.xlsx actualizado")

    except Exception as e:
        msg = f"❌ Error en Fase 3: {str(e)}"
        print(f"\n{msg}")
        if logger:
            logger.error(msg)
        exito_reservorio = False
    finally:
        try:
            if wb_reservorio:
                wb_reservorio.Close(SaveChanges=False)
        except Exception:
            pass
        try:
            if excel_fase3:
                excel_fase3.Quit()
        except Exception:
            pass
        pythoncom.CoUninitialize()

    # ========================================================================
    # RESUMEN FINAL
    # ========================================================================
    total_tiempo = int(time.time() - tiempo_total_inicio)
    mnts, segs = divmod(total_tiempo, 60)

    print(f"\n{'='*70}")
    if exito_base and exito_reservorio:
        print("✅ PROCESO FINALIZADO EXITOSAMENTE")
    else:
        print("⚠️ PROCESO FINALIZADO CON ADVERTENCIAS")
    print(f"Tiempo total: {mnts:02d}:{segs:02d}")
    print(f"{'='*70}")

    if logger:
        logger.info("=" * 70)
        if exito_base and exito_reservorio:
            logger.info(
                f"✅ PROCESO COMPLETADO - Tiempo total: {mnts:02d}:{segs:02d}"
            )
        else:
            logger.warning(
                f"⚠️ PROCESO CON ADVERTENCIAS - Tiempo total: {mnts:02d}:{segs:02d}"  # noqa: E501
            )
        logger.info("=" * 70)


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error inesperado: {str(e)}")
        sys.exit(1)
