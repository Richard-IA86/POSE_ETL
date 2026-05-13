# ETL A2 — Migración a BD_POSE_B52

**Última ejecución:** 2026-05-13
**Ejecutado en:** M2 (Isindur — Asus/Windows)
**Estado:** ✅ COMPLETO — `BaseCostosPOSE.xlsx` generado (36.3 MB)

> Historial de corridas al final del documento (sección 6).

---

## 1. Métricas del output final

`output/director/BaseCostosPOSE.xlsx` — generado 2026-05-13 17:10

| Métrica | Valor |
|---------|-------|
| **Archivos procesados** | 44 |
| **Total filas normalizadas** | 538,258 |
| **BaseCostoUnificada.xlsx** | ~100.8 MB |
| **BaseCostosPOSE.xlsx** | 36.3 MB |

> **Nota CODIGO_CUENTA:** Los 31 archivos del segmento `2023_2025_Hist`
> (2023-01 a 2025-07) no tenían la columna en origen → el normalizador
> la agrega vacía por schema fill. Los segmentos `2025` (ago-dic),
> `2025_Ajustes`, `2026` y `2025_Compensaciones` sí la tienen.

### Detalle de filas por segmento (run 2026-05-13)

| Segmento | Archivos | Filas |
|----------|:--------:|------:|
| `2023_2025_Hist` | 31 | 287,821 |
| `2025` (ago-dic) | 5 | 88,179 |
| `2026` | 4 | 70,268 |
| `BD_Modificaciones` | 1 | 71,910 |
| `BD_Compensaciones_2025` | 1 | 11,008 |
| `BD_Historico_2021_2022` | 1 | 7,876 |
| `2025_Ajustes` | 1 | 1,196 |
| **TOTAL** | **44** | **538,258** |

---

## 2. Tabla de fuentes → output_ready_for_pq

Todos los archivos en `output/output_ready_for_pq/` son consumidos por
`BaseCostoUnificada.xlsx` via Power Query (consulta `fn_LeerExcelNorm`).

### Segmento: `2021_2022_Historico` — 1 archivo (0.48 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2021_2022_Historico/` | `BD_Historico_2021_2022.xlsx` | 0.48 |

> Origen: consolidado histórico. Sin crudo homónimo en `fuentes/`.
> Filtro: años 2021–2022. Dupl. STRICT eliminados: 18 filas.

### Segmento: `2023_2025_Hist` — 31 archivos (22.9 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2023_2025_Hist/` | `2023-01 COSTO gerencias.xlsx` | 0.72 |
| | `2023-02 COSTO gerencias.xlsx` | 0.74 |
| | `2023-03 COSTO gerencias.xlsx` | 0.88 |
| | `2023-04 COSTO gerencias.xlsx` | 0.84 |
| | `2023-05 COSTO gerencias.xlsx` | 0.88 |
| | `2023-06 COSTO GERENCIAS.xlsx` | 0.97 |
| | `2023-07 COSTO GERENCIAS.xlsx` | 0.89 |
| | `2023-08 COSTO GERENCIAS.xlsx` | 0.93 |
| | `2023-09 COSTO GERENCIAS.xlsx` | 0.92 |
| | `2023-10 COSTO GERENCIAS.xlsx` | 0.98 |
| | `2023-11 COSTO GERENCIAS.xlsx` | 1.03 |
| | `2023-12 COSTO GERENCIAS.xlsx` | 0.59 |
| | `2024-01 COSTO GERENCIAS.xlsx` | 0.64 |
| | `2024-02 COSTO GERENCIAS.xlsx` | 0.46 |
| | `2024-03 COSTO GERENCIAS.xlsx` | 0.41 |
| | `2024-04 COSTO GERENCIAS.xlsx` | 0.42 |
| | `2024-05 COSTO GERENCIAS.xlsx` | 0.53 |
| | `2024-06 COSTO GERENCIAS.xlsx` | 0.51 |
| | `2024-07 COSTO GERENCIAS.xlsx` | 0.60 |
| | `2024-08 COSTO GERENCIAS.xlsx` | 0.63 |
| | `2024-09 COSTO GERENCIAS.xlsx` | 0.64 |
| | `2024-10 COSTO GERENCIAS.xlsx` | 0.59 |
| | `2024-11 COSTO GERENCIAS.xlsx` | 0.62 |
| | `2024-12 COSTO GERENCIAS.xlsx` | 0.60 |
| | `2025-01 COSTO GERENCIAS.xlsx` | 0.65 |
| | `2025-02 COSTO GERENCIAS.xlsx` | 0.67 |
| | `2025-03 COSTO GERENCIAS VF.xlsx` | 0.72 |
| | `2025-04 COSTO GERENCIAS VF.xlsx` | 0.78 |
| | `2025-05 COSTO GERENCIAS.xlsx` | 1.06 |
| | `2025-06 COSTO GERENCIAS.xlsx` | 0.98 |
| | `2025-07 COSTO GERENCIAS.xlsx` | 1.01 |

> Origen: 31 xlsx individuales en `fuentes/compensaciones/2023_2025_Hist/`.
> Sin columna CODIGO_CUENTA en ningún archivo de origen (comportamiento esperado).

### Segmento: `2025` — 5 archivos (7.8 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2025/` | `2025-08 COSTOS GERENCIAS.xlsx` | 1.52 |
| | `2025-09 COSTOS GERENCIAS.xlsx` | 1.64 |
| | `2025-10 COSTOS GERENCIAS.xlsx` | 1.68 |
| | `2025-11 COSTOS GERENCIAS.xlsx` | 1.50 |
| | `2025-12 COSTOS GERENCIAS.xlsx` | 1.44 |

### Segmento: `2025_Ajustes` — 1 archivo (0.12 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2025_Ajustes/` | `AJUSTES.xlsx` | 0.12 |

### Segmento: `2025_Compensaciones` — 1 archivo (0.85 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2025_Compensaciones/` | `BD_Compensaciones_2025.xlsx` | 0.85 |

> Origen consolidado desde 7 archivos fuente:
> `Comp BRIC-POSE`, `Comp CAC-POSE`, `Comp SYGSA-POSE`,
> `Incluir en costos POSE - Prest y Alq`, `2025 - COMPENSACION ANUAL RYR`,
> `Prest_Internas_PPO`, `2025 - ASFALTO`.
> Total procesado: 11,008 filas | Σ $605,614,687.05

### Segmento: `2026` — 4 archivos (actualizado 2026-05-13)

| Carpeta | Archivo |
|---------|--------|
| `output_ready_for_pq/2026/` | `2026-01 COSTOS GERENCIAS.xlsx` |
| | `2026-02 COSTOS GERENCIAS.xlsx` |
| | `2026-03 COSTOS GERENCIAS.xlsx` |
| | `2026-04 COSTOS GERENCIAS.xlsx` ← **nuevo** |

### Segmento: `Modificaciones` — 1 archivo (6.78 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/Modificaciones/` | `BD_Modificaciones.xlsx` | 6.78 |

> Origen: consolidado de modificaciones. Sin crudo homónimo en `fuentes/`.

---

## 3. Flujo completo del pipeline

```
ETL_BaseA2/input_raw/{segmento}/*.xlsx
        │  (xlsx fuente copiados manualmente a input_raw/)
        │  Nombres de carpeta en input_raw/ (≠ output_ready_for_pq/):
        │    BD_Historico_2021_2022/  →  output: 2021_2022_Historico/
        │    2023_2025_Hist/          →  output: 2023_2025_Hist/
        │    2025/                    →  output: 2025/
        │    2025_Ajustes/            →  output: 2025_Ajustes/
        │    BD_Compensaciones_2025/  →  output: 2025_Compensaciones/
        │    2026/                    →  output: 2026/
        │    BD_Modificaciones/       →  output: Modificaciones/
        ▼
[1] ETL_BaseA2/src/ingesta/normalizador_base_costos.py
        │  Ejecutar: python -m src.ingesta.normalizador_base_costos
        │  • Estandariza columnas al schema unificado (16 cols)
        │  • Aplica filtros de años según segmento
        │  • Elimina duplicados STRICT
        │  • Schema fill: agrega col vacía si no existe en origen
        │  • Genera REPORTE_INGESTA.md con métricas por archivo
        ▼
ETL_BaseA2/output/output_normalized/individuales/{segmento}/*.xlsx
        │
        ▼
[2] ETL_BaseA2/src/ingesta/alinear_para_ingesta.py
        │  Ejecutar: python -m src.ingesta.alinear_para_ingesta
        │  • Valida schema de 16 columnas exactas
        │  • Reordena columnas al orden canónico
        │  • Copia a output_ready_for_pq/ con nombre de segmento final
        ▼
ETL_BaseA2/output/output_ready_for_pq/{segmento}/*.xlsx
        │  44 archivos, 538,258 filas (run 2026-05-13)
        │  Carpeta EXCLUIDA de git (.gitignore)
        ▼
[3] ETL_BaseA2/scripts/Paso2_ActualizarPQ.py  (via COM Excel / pywin32)
     │  Ejecutar: python scripts/Paso2_ActualizarPQ.py
     │  Config: ETL_BaseA2/config/config_automatizacion.json
     │
     ├─ FASE 2: Refresh power_query/BaseCostoUnificada.xlsx
     │    Lee: ETL_BaseA2/output/output_ready_for_pq/ (Param_RutaBase)
     │    Consultas PQ (en orden de dependencia):
     │      Con_Maestros
     │      Dim_Obras, Dim_Calendario_TC, Dim_TipComprobante,
     │        Dim_Fuentes, Dim_Excepciones
     │      Archiv_Consolidado_Crudo ← lee output_ready_for_pq/ por carpeta
     │      Archiv_Altas_Manuales, Analisis_Filas_Descartadas
     │      Archiv_Consolidado_Modif
     │      Archiv_Consolidado_Final
     │      Debug_Normalizacion_Fuente, Debug_Parsing_Detalle
     │    → Guarda power_query/BaseCostoUnificada.xlsx (~100.8 MB)
     │    EXCLUIDO de git por tamaño
     │
     └─ FASE 3: Refresh output/director/BaseCostosPOSE.xlsx
          Lee: BaseCostoUnificada.xlsx
          → Guarda output/director/BaseCostosPOSE.xlsx (36.3 MB) ✅
          EXCLUIDO de git por tamaño
```

### Tiempos de ejecución por corrida

| Corrida | Fase 2 | Fase 3 | Total | Archivos | Filas |
|---------|-------:|-------:|------:|:--------:|------:|
| 2026-05-08 | 09:32 | ~03:00 | ~12:56 | 43 | 345,777 |
| 2026-05-13 | 17:49 | 05:32 | 23:21 | 44 | 538,258 |

> El incremento de tiempo 2026-05-13 refleja el crecimiento de datos
> (2026-04 nuevo + datos más voluminosos en todos los segmentos).

### Regla de negocio en Archiv_Consolidado_Crudo.pq

El segmento `2021_2022_Historico` contiene filas con `FUENTE = "COMP SYGSA-POSE"`
que ya están incluidas en `2025_Compensaciones`. Para evitar duplicados:

```powerquery
FilasValidas = Table.SelectRows(
    FilasBase,
    each not (
        Text.Contains([Folder Path], "2021_2022_Historico")
        and [FUENTE] = "COMP SYGSA-POSE"
    )
)
```

El segmento `Modificaciones/` es **excluido** de `CarpetasObjetivo` en la
consulta cruda — se procesa en `Archiv_Consolidado_Modif` por separado.

---

## 4. Validación comparativo_registros

Ejecutado: `python -m src.ingesta.comparativo_registros` (todos los segmentos)

| Segmento | Resultado | Notas |
|----------|-----------|-------|
| `2021_2022_Historico` | `sin crudo` (aviso) | Consolidado generado, sin archivo crudo homónimo |
| `2023_2025_Hist` (31 archivos) | filas/importe: **OK** | Exit code 1 por "0 cuentas" — esperado (columna inexistente en origen) |
| `2025` (5 archivos) | **OK** | Δ=0 en filas e importe |
| `2025_Ajustes` | **OK** | Δ=0 |
| `2025_Compensaciones` | `sin crudo` (aviso) | Consolidado de 7 fuentes |
| `2026` (3 archivos) | **OK** | Δ=0 |
| `Modificaciones` | `sin crudo` (aviso) | Consolidado generado |

> **Exit code 1 = falso positivo.** El comparativo marca BLOQUEANTE cuando
> `cuentas_unicas == 0`, pero en `2023_2025_Hist` nunca existió `CODIGO_CUENTA`
> en los fuentes originales. Filas e importes cuadran perfectamente en los
> 31 archivos.
>
> **Pendiente para M1:** ajustar lógica en `comparativo_registros.py` para
> no marcar BLOQUEANTE cuando la columna fue agregada vacía por schema fill
> (detectar si `crudo_cuentas == 0` también, no solo `ingesta_cuentas == 0`).

---

## 5. Fixes aplicados durante el desarrollo

### 2026-05-13

| Archivo | Fix |
|---------|-----|
| `ETL_BaseA2/power_query/_Transformaciones/Archiv_Consolidado_Crudo.pq` | `CarpetasObjetivo` tenía rutas incorrectas (`consolidados/` e `individuales/`). Corregido a nombres reales de segmento en `output_ready_for_pq/`. |
| `ETL_BaseA2/config/config_automatizacion.json` | Ruta `base_costo_unificada` era `"power_query/BaseCostoUnificada.xlsx"` (relativa a ETL_BaseA2/). Corregida a `"../power_query/BaseCostoUnificada.xlsx"` (el archivo está un nivel arriba). |

### 2026-05-08

| Archivo | Fix |
|---------|-----|
| `scripts/Paso2_ActualizarPQ.py` | Restaurar Fase 3 — corrección ruta `BaseCostosPOSE.xlsx` |

---

## 6. Historial de corridas

### Run 2026-05-13

- **Archivos procesados:** 44 (nuevo: `2026-04 COSTOS GERENCIAS.xlsx`)
- **Filas totales:** 538,258
- **BaseCostoUnificada.xlsx:** ~100.8 MB
- **BaseCostosPOSE.xlsx:** 36.3 MB
- **Commits POSE_ETL:**
  - `6c6bafe` — chore(m2): resultado ETL A2 completo 2026-05-13
  - `7a72f25` — chore(git): excluir input_raw y arch_hist_py; actualizar Loockups y REPORTE_INGESTA

### Run 2026-05-08

- **Archivos procesados:** 43
- **Filas totales:** 345,777
- **BaseCostosPOSE.xlsx:** 33 MB
- **Commits POSE_ETL:**
  - `390e077` — fix(paso2): restaurar Fase3
  - `95e2d6d` — chore(jornada): .gitignore + menu_ejecucion.bat
