# Proyecto: Base de Costo Unificada

Este directorio contiene la arquitectura unificada para el ETL de costos del proyecto POSE. Reemplaza las antiguas lógicas dispersas de `BD_POSE` y `BD_POSE_Auste`.

## 🔄 Flujo General del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│  PASO 1: PRE-INGESTA (Normalización Python)                 │
└─────────────────────────────────────────────────────────────┘

📁 Pre_IngestaBD/input_raw/      (Archivos originales usuarios)
    ├── 2021_2022_Historico/
    ├── 2023_2025_Hist/
    ├── ...
                ↓
    ┌───────────────────────┐
    │ normalizador_base_    │  • Limpieza de datos
    │ costos.py             │  • Estandarización Headers
    │                       │  • Tipado Estricto (Str/Date)
    │                       │  • 🆕 Filtrado por Año (config.json)
    └───────────────────────┘  • Validación
                ↓
📁 Pre_IngestaBD/output_normalized/
    ├── consolidados/
    ├── individuales/
                ↓
    ┌───────────────────────┐
    │ alinear_para_ingesta  │  • Distribución Automática
    │ .py                   │  • Espejado de Directorios
    └───────────────────────┘
                ↓
📁 Pre_IngestaBD/output_ready_for_pq/  (STAGING AREA)
    │  (Estructura idéntica a Power Query)
    └── [Carpetas listas para consumo]

┌─────────────────────────────────────────────────────────────┐
│  PASO 2: INGESTA A BASE DE DATOS (Power Query)              │
└─────────────────────────────────────────────────────────────┘

📁 BaseCostoUnificada/           
    │
    └── _Transformaciones/
         └── (Queries leen de Pre_IngestaBD/output_ready_for_pq)
                ↓
    ┌───────────────────────┐
    │  Archiv_Consolidado_  │  • Lee carpetas raíz
    │  Crudo.pq             │  • fn_LeerExcelNorm
    │                       │  • Transformaciones PQ
    └───────────────────────┘
                ↓
📊 BaseCostoUnificada.xlsx       (Base de datos final)
```

**Nota importante:** La copia manual entre PASO 1 y PASO 2 garantiza control de calidad y evita propagar errores a la base de datos operativa.

## Estructura del Proyecto

*   **`_Conexiones/`**: Scripts base para conectar a fuentes.
    *   `fn_LeerExcelNorm.pq`: Función inteligente de lectura (Primera hoja + Alertas).
    *   `Con_Maestros.pq`: Conexión única a tablas de referencia.

*   **`_Dimensiones/`**: Consultas de Maestros limpias y normalizadas.
    *   `Dim_Obras.pq`: Catálogo de Obras y Gerencias.
    *   `Dim_Calendario_TC.pq`: Tipo de cambio continuo.
    *   `Dim_Excepciones.pq`: Reglas de reasignación de Gerencia por fecha.
    *   `Dim_TipComprobante.pq`: Normalización de abreviaturas de tipo de comprobante (FC → FACTURA).

*   **`_Transformaciones/`**: Pipeline de datos en 4 capas.
    1.  **Crudo (`Archiv_Consolidado_Crudo`)**: Lectura masiva de carpetas históricas.
        *   Mantiene campo `DETALLE` intacto para parsing posterior.
        *   **NO** incluye `TIPO_COMPROBANTE` ni `NRO_COMPROBANTE` (se parsearán en capa Final).
    2.  **Altas (`Archiv_Altas_Manuales`)**: Lectura aislada de ajustes manuales (Safe Firewall).
        *   Mantiene campo `DETALLE` intacto.
    3.  **Gestión (`Archiv_Consolidado_Modif`)**: Unión de Crudo + Altas (con reglas de exclusión).
    4.  **Final (`Archiv_Consolidado_Final`)**: Join de Gestión + Dimensiones → Salida Limpia.
        *   **Parsing Centralizado**: Extrae `TIPO_COMPROBANTE` y `NRO_COMPROBANTE` desde `DETALLE`.
        *   Formato reconocido: `"dd/mm/yyyy SIGLA (Ref.XXXXXX) NRO_COMP - Prov.: ..."`
        *   Normaliza `TIPO_COMPROBANTE` usando `Dim_TipComprobante` (SIGLA → nombre completo).

*   **`Modificaciones/`**: Carpeta operativa para cargar ajustes manuales (`RegPRONTO_2025.xlsx`).
*   **`Tablas/`**: Archivos de soporte (Lookups, Maestros).

*   **`input_raw/`**: Carpeta de entrada para Pre-Ingesta (normalización Python).
    *   Contiene archivos originales de usuarios sin procesar
    *   Organizados en subcarpetas por tipo/período
    *   **NO usada por Power Query** (solo por normalizador)

*   **`output_normalized/`**: Carpeta de salida de Pre-Ingesta.
    *   `consolidados/`: Archivos fusionados (múltiples → uno)
    *   `individuales/`: Archivos procesados individualmente
    *   `reportes/`: Logs de ejecución con auditoría completa
    *   Archivos listos para copia manual a carpetas raíz

*   **`normalizador/`**: Sistema de Pre-Ingesta (Python v2.3).
    *   **Nueva ubicación:** `Pre_IngestaBD/normalizador/` ⚠️
    *   `normalizador_base_costos.py`: Script principal
    *   `config.json`: Configuración de carpetas, rutas y filtros de año
    *   `utils/`: Módulos de lectura, transformación y escritura
        *   `debug/`: Scripts de diagnóstico y validación (10 herramientas)
    *   🆕 **Filtrado por año**: Configurable en `config.json` (ej: solo 2021-2022)
    *   Ver documentación completa en [`Pre_IngestaBD/normalizador/README.md`](../Pre_IngestaBD/normalizador/README.md)

*   **Carpetas raíz** (`2021_2022_Historico/`, `2023_2025_Hist/`, etc.):
    *   **⚠️ OBSOLETAS - Solo para referencia histórica**
    *   Archivos normalizados ahora están en `../Pre_IngestaBD/output_ready_for_pq/`
    *   **Power Query lee desde:** `Pre_IngestaBD/output_ready_for_pq/` (estructura espejo)
    *   Se actualizan automáticamente mediante `alinear_para_ingesta.py`
    *   Estas carpetas locales pueden archivarse tras validación completa

## 🔍 **Auditoría del Flujo de Datos** ⭐ *NUEVO*

El sistema incluye controles automáticos de auditoría que validan la integridad de los datos en cada etapa del ETL:

### **Consultas de Control**
*   **`Control_Auditoria_ETL.pq`**: Panel unificado con métricas de filas e importes en cada checkpoint.
*   **`Analisis_Exclusiones_CP2.pq`**: Detalle de registros excluidos durante la consolidación con motivo específico.
*   **`Resumen_Exclusiones_CP2.pq`**: ⭐ Dashboard con desglose de exclusiones por tipo (Regla/Reemplazo/Error).
*   **`Analisis_Comparativo_Importes.pq`**: Tracking de importes por FUENTE a través de todo el pipeline.

### **Manejo de Altas y Bajas** ⚠️ *IMPORTANTE*

Las **Altas Manuales** pueden ser de dos tipos:
- **Alta Neta:** Registro nuevo que se adiciona (incremento neto de filas)
- **Alta Reemplazo:** Registro que corrige/anula uno existente en Crudo (sin cambio neto de filas)

El sistema **automáticamente detecta TODAS las exclusiones** mediante:
```
Exclusiones_Total = (Filas_Crudo + Filas_Altas) - Filas_Consolidadas
```

Esto incluye:
1. Exclusiones por regla de negocio (ej: PRONTO 2025 duplicado)
2. Exclusiones por reemplazo (altas que anulan registros del Crudo)
3. Bajas explícitas (si se implementan en futuro)

**Ver detalle completo:** [`ACLARACION_ALTAS_BAJAS.md`](./ACLARACION_ALTAS_BAJAS.md)

### **Consultas de Control**
*   **`Control_Auditoria_ETL.pq`**: Panel unificado con métricas de filas e importes en cada checkpoint.
*   **`Analisis_Exclusiones_CP2.pq`**: Detalle de registros excluidos durante la consolidación.
*   **`Analisis_Comparativo_Importes.pq`**: Tracking de importes por FUENTE a través de todo el pipeline.

### **Script de Validación Python**
*   **`validar_auditoria_etl.py`**: Ejecuta validaciones automáticas y genera reportes de estado.

### **Checkpoints de Auditoría**
1.  **CP1 - Extracción**: Cuenta filas y suma importes de `Archiv_Consolidado_Crudo` + `Archiv_Altas_Manuales`
2.  **CP2 - Gestión**: Valida la consolidación y exclusiones en `Archiv_Consolidado_Modif`
3.  **CP3 - Final**: Verifica integridad post-enriquecimiento en `Archiv_Consolidado_Final`

**📚 Documentación completa:** Ver [`PROPUESTA_AUDITORIA_FLUJO_DATOS.md`](./PROPUESTA_AUDITORIA_FLUJO_DATOS.md) y [`GUIA_IMPLEMENTACION_AUDITORIA.md`](./GUIA_IMPLEMENTACION_AUDITORIA.md)

### **⚡ Optimización de Rendimiento**

**IMPORTANTE:** Las consultas de auditoría están optimizadas con `Table.Buffer()` para evitar re-ejecuciones:
- ✅ Tiempo estimado sin auditoría: 5 min
- ✅ Tiempo estimado con auditoría: 6 min (solo +1 min)
- ❌ Sin optimización sería: 20+ min

**Ver guía completa:** [`OPTIMIZACION_TIEMPOS_PROCESO.md`](./OPTIMIZACION_TIEMPOS_PROCESO.md)

**Configuración recomendada:**
- Consultas principales: "Connection Only" (no cargar a Excel)
- Solo cargar: `Archiv_Consolidado_Final` + `Control_Auditoria_ETL`
- Ejecutar auditorías completas solo cuando sea necesario (ej: viernes)

## Instrucciones de Uso

### 1. Cargar Ajustes Manuales
Para agregar registros que no están en el sistema o corregir datos:
1.  Edite el archivo `Modificaciones/RegPRONTO_2025.xlsx`.
2.  Agregue filas respetando las columnas mínimas: `FECHA`, `OBRA_PRONTO`, `IMPORTE`, `FUENTE`.
3.  Refresque las consultas en Excel.

### 2. Verificar Alertas
Si en la tabla final ve la columna `ALERTA_HOJAS` con texto, significa que algún archivo Excel de origen tiene múltiples pestañas visibles que están siendo ignoradas (la función solo lee la primera).
*   **Acción:** Revisor el archivo origen y ocultar pestañas basura o separar en archivos distintos si son datos válidos.

### 3. Exclusiones Automáticas
El sistema automáticamente descarta registros de carpetas que tengan fecha **2025** y fuente **PRONTO** si ya existen en la carga Manual, para evitar duplicados.

### 4. Filtrado por Año en Pre-Ingesta 🆕
El normalizador Python permite configurar filtros de año por dataset en `../Pre_IngestaBD/normalizador/config.json`:

**Configuración actual:**
```json
"filtros_anio": {
  "2021_2022_Historico": [2021, 2022]
}
```

**¿Qué hace?**
- Solo pasan a la base los registros con fechas del año 2021 y 2022
- Se aplica automáticamente durante el proceso de normalización
- Los registros de otros años se excluyen antes de llegar a Power Query

**Ventajas:**
- ✅ Mantiene la base limpia de datos históricos no deseados
- ✅ Reduce tamaño de archivos procesados
- ✅ Configurable sin modificar código Python
- ✅ Aplicable a cualquier dataset (consolidado o individual)

**Para agregar más filtros:** Editar `config.json` y agregar la carpeta con sus años permitidos.

### 5. Parsing Centralizado de DETALLE (Capa Final)
El sistema extrae automáticamente `TIPO_COMPROBANTE` y `NRO_COMPROBANTE` desde el campo `DETALLE` en la capa Final del pipeline, **para todos los registros** que tengan el formato reconocible:
*   **Formato reconocido:** `"dd/mm/yyyy SIGLA (Ref.XXXXXX) NRO_COMP - Prov.: ..."`
*   **Ejemplo:** `"18/01/2023 FC (Ref.00107004) A-00002-00018595 - Prov.: FASTER"`
*   **Extracción:**
    *   `SIGLA` → `TIPO_COMPROBANTE` (ej: "FC")
    *   `NRO_COMP` → `NRO_COMPROBANTE` (ej: "A-00002-00018595")
*   **Normalización:** La sigla extraída ("FC") se normaliza a nombre completo ("FACTURA") usando `Dim_TipComprobante`
*   **Ventaja:** El campo `DETALLE` original permanece intacto hasta la capa Final, permitiendo auditoría completa

## Mantenimiento
Para cambiar la ubicación de las tablas maestras, edite únicamente el archivo `_Conexiones/Con_Maestros.pq`.
