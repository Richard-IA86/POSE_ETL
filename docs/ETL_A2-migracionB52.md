# ETL A2 — Migración a BD_POSE_B52

**Fecha de ejecución:** 2026-05-08
**Ejecutado en:** M2 (Isindur — Asus/Windows)
**Estado:** ✅ COMPLETO — `BaseCostosPOSE.xlsx` generado (33 MB)

---

## 1. Métricas del output final

`output/director/BaseCostosPOSE.xlsx` — generado 2026-05-08 01:40:37

| Métrica | Valor |
|---------|-------|
| **Total filas** | 345,777 |
| **Importe total (ARS)** | -86,725,584,699.03 |
| **Filas OBRA_PRONTO no nulas** | 345,777 (100%) |
| **Obras únicas (OBRA_PRONTO)** | 421 |
| **Filas CODIGO_CUENTA no vacías** | 84,057 |

> **Nota CODIGO_CUENTA:** Los 31 archivos del segmento `2023_2025_Hist`
> (2023-01 a 2025-07) no tenían la columna en origen → el normalizador
> la agrega vacía por schema fill. Las 84,057 filas con cuenta corresponden
> a `2025` (ago-dic), `2025_Ajustes`, `2026` y parcialmente `2025_Compensaciones`.

### Detalle COMPENSABLE

| Valor | Filas |
|-------|------:|
| SI | 248,904 |
| COMPENSABLE TALLER | 41,479 |
| COMPENSABLE GASTOS DE SEDE | 16,084 |
| DESARROLLOS | 11,607 |
| COMPENSABLE CAC | 10,426 |
| COMPENSABLE SYGSA | 5,551 |
| ACTIVOS | 5,158 |
| COMPENSABLE BRIC | 4,191 |
| COMPENSABLE EFVO | 714 |
| COMPENSABLE WITT | 579 |
| ADMINISTRACION | 547 |
| COMPENSABLE WORK PROJECTS | 481 |
| CAC | 36 |
| COMPENSABLE QUINCENA | 20 |
| **TOTAL** | **345,777** |

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

### Segmento: `2026` — 3 archivos (4.65 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/2026/` | `2026-01 COSTOS GERENCIAS.xlsx` | 1.58 |
| | `2026-02 COSTOS GERENCIAS.xlsx` | 1.51 |
| | `2026-03 COSTOS GERENCIAS.xlsx` | 1.56 |

### Segmento: `Modificaciones` — 1 archivo (6.78 MB)

| Carpeta | Archivo | MB |
|---------|---------|---:|
| `output_ready_for_pq/Modificaciones/` | `BD_Modificaciones.xlsx` | 6.78 |

> Origen: consolidado de modificaciones. Sin crudo homónimo en `fuentes/`.

---

## 3. Flujo completo del pipeline

```
fuentes/compensaciones/{segmento}/*.xlsx
        │  (xlsx crudos por período)
        ▼
[1] src/ingesta/normalizador_base_costos.py --segmento X
        │  • Estandariza columnas al schema unificado (16 cols)
        │  • Aplica filtros de años según segmento
        │  • Elimina duplicados STRICT
        │  • Agrega columnas vacías por schema fill (ej: CODIGO_CUENTA en 2023)
        ▼
output/output_normalized/individuales/{segmento}/*.xlsx   (o consolidados/)
        │
        ▼
[2] src/ingesta/alinear_para_ingesta.py --segmento X
        │  • Valida schema de 16 columnas exactas
        │  • Reordena columnas al orden canónico
        │  • Copia a output_ready_for_pq/{segmento}/
        ▼
output/output_ready_for_pq/{segmento}/*.xlsx   ← 43 archivos, ~43 MB total
        │
        ▼
[3] scripts/Paso2_ActualizarPQ.py  (via COM Excel / pywin32)
     ├─ FASE 2: Refresh BaseCostoUnificada.xlsx (6 capas PQ, 09:32)
     │    Capa 1: Con_Maestros
     │    Capa 2: Dim_Obras, Dim_Calendario_TC, Dim_TipComprobante,
     │            Dim_Fuentes, Dim_Excepciones
     │    Capa 3: Archiv_Consolidado_Crudo, Archiv_Altas_Manuales,
     │            Analisis_Filas_Descartadas
     │    Capa 4: Archiv_Consolidado_Modif
     │    Capa 5: Archiv_Consolidado_Final  (453s — capa más pesada)
     │    Capa 6: Debug_Normalizacion_Fuente, Debug_Parsing_Detalle
     │    → Guarda BaseCostoUnificada.xlsx (~147 MB)
     │
     └─ FASE 3: Refresh BaseCostosPOSE.xlsx (6 capas PQ, ~3 min)
          → Guarda output/director/BaseCostosPOSE.xlsx (33 MB) ✅
```

### Tiempos de ejecución (2026-05-08 — historia completa)

| Fase | Tiempo |
|------|--------|
| Fase 2 — BaseCostoUnificada.xlsx | 09:32 |
| Fase 3 — BaseCostosPOSE.xlsx | ~03:00 |
| **Total Paso2** | **12:56** |

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

## 5. Commits de esta jornada (POSE_ETL)

| Hash | Tipo | Descripción |
|------|------|-------------|
| `390e077` | fix(paso2) | Restaurar Fase3 — corrección ruta `BaseCostosPOSE.xlsx` |
| `95e2d6d` | chore(jornada) | `.gitignore` excluye `output/` y `data/`; `menu_ejecucion.bat` migrado desde Planif_POSE; reporte ingesta actualizado |

**Estado:** 2 commits adelante de `origin/main`. Push pendiente de aprobación.
