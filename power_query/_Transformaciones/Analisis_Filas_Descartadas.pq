let
    // =========================================================================
    // CONSULTA: Analisis_Filas_Descartadas
    // =========================================================================
    // Objetivo: Identificar filas eliminadas por falta de FECHA o datos nulos.
    // =========================================================================

    // 1. Rutas de Carpetas (Misma lógica que Crudo) — Derivadas de Param_RutaBase
    RutaRaiz = Param_RutaBase & "\data\output_ready_for_pq",
    CarpetasObjetivo = {"2021_2022_Historico", "2023_2025_Hist", "2025_Ajustes", "2025", "2025_Compensaciones", "2026", "Modificaciones"},    

    // 2. Conectar y Filtrar Archivos
    Source = Folder.Files(RutaRaiz),
    Filtrado = Table.SelectRows(Source, each
        ([Extension] = ".xlsx" or [Extension] = ".xls") and
        (not Text.StartsWith([Name], "~$")) and
        List.AnyTrue(List.Transform(CarpetasObjetivo, (carpeta) => Text.Contains([Folder Path], "\" & carpeta & "\")))
    // 3. Invocar Lectura
    LecturaContenido = Table.AddColumn(Filtrado, "Data", each fn_LeerExcelNorm([Content])),
    
    // 4. Expandir (Columnas clave + Metadata)
    ColumnasAExpandir = {"FECHA", "OBRA_PRONTO", "IMPORTE", "PROVEEDOR", "DETALLE", "FUENTE", "ALERTA_HOJAS"},
    Expandido = Table.ExpandTableColumn(LecturaContenido, "Data", ColumnasAExpandir),
    
    // 5. CLASIFICAR FILAS DESCARTADAS (Replica lógica de Archiv_Consolidado_Crudo)
    
    // A. Identificar Descartes por Nulos Estructurales
    DescartesNulos = Table.AddColumn(Expandido, "TIPO_DESCARTE", each
        if [FECHA] = null then "FECHA_NULA"
        else if [OBRA_PRONTO] = null then "OBRA_NULA"
        else if [FUENTE] = null then "FUENTE_NULA"
        else "VALIDO",
        type text
    ),
    
    // B. Identificar Descartes por Regla de Negocio (COMP SYGSA-POSE en 2021_2022_Historico)
    DescartesRegla = Table.AddColumn(DescartesNulos, "DESCARTE_REGLA_NEGOCIO", each
        if Text.Contains([Folder Path], "2021_2022_Historico") and [FUENTE] = "COMP SYGSA-POSE"
        then "COMP_SYGSA_HISTORICO_EXCLUIDO"
        else null,
        type nullable text
    ),
    
    // C. Filtrar SOLO filas con algún tipo de descarte
    SoloDescartes = Table.SelectRows(DescartesRegla, each 
        [TIPO_DESCARTE] <> "VALIDO" or [DESCARTE_REGLA_NEGOCIO] <> null
    ),
    
    // 6. Agregar Metadata de Origen
    ConMetadata = Table.AddColumn(SoloDescartes, "CARPETA_ORIGEN", each 
        List.Last(List.RemoveLastN(Text.Split([Folder Path], "\"), 1)),
        type text
    ),
    
    // 7. Selección Final con Columnas Relevantes
    Resultado = Table.SelectColumns(ConMetadata, {
        "Name",                      // Archivo origen
        "CARPETA_ORIGEN",           // Carpeta origen
        "FECHA",
        "OBRA_PRONTO",
        "FUENTE",
        "IMPORTE",
        "PROVEEDOR",
        "DETALLE",
        "TIPO_DESCARTE",            // Tipo de error nulo
        "DESCARTE_REGLA_NEGOCIO",  // Regla de negocio aplicada
        "ALERTA_HOJAS"              // Si tenía múltiples hojas
    })
in
    Resultado