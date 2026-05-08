let
    // =========================================================================
    // CONSULTA: Archiv_Consolidado_Crudo (Capa 1)
    // =========================================================================
    // Objetivo: Iterar carpetas, usar fn_LeerExcelNorm y consolidar.
    // =========================================================================

    // 1. Rutas de Carpetas (Configurable) — Derivadas de Param_RutaBase
    // Nueva arquitectura: normalizador graba en output\output_normalized\individuales
    RutaRaiz = Param_RutaBase & "\output\output_normalized\individuales",
    CarpetasObjetivo = {"2021_2022_Historico", "2023_2025_Hist", "2025_Ajustes", "2025", "2025_Compensaciones", "2026"},
    
    // 2. Conectar a Sistema de Archivos
    Source = Folder.Files(RutaRaiz),
    
    // 3. Filtrar por Carpetas Relevantes y Extensión Excel
    Filtrado = Table.SelectRows(Source, each 
        ([Extension] = ".xlsx" or [Extension] = ".xls") and 
        (not Text.StartsWith([Name], "~$")) and // Ignorar temporales
        List.AnyTrue(List.Transform(CarpetasObjetivo, (carpeta) => Text.Contains([Folder Path], "\" & carpeta & "\")))
    ),
    
    // 4. INVOCAR FUNCIÓN DE LECTURA (fn_LeerExcelNorm)
    // Asumimos que la función está disponible en el entorno con ese nombre
    LecturaContenido = Table.AddColumn(Filtrado, "Data", each fn_LeerExcelNorm([Content])),
    
    // 5. EXPANDIR DATOS CONSOLIDADOS
    // Al usar Thin Fact, sabemos exactamente qué columnas esperar
    // Se agrega ALERTA_HOJAS para capturar avisos de archivos con múltiples hojas
    // TIPO_COMPROBANTE y NRO_COMPROBANTE se expanden para intentar lectura directa
    // Si vienen nulos, se intentará parseo en capa Final
    ColumnasAExpandir = {"FECHA", "OBRA_PRONTO", "IMPORTE", "PROVEEDOR", "OBSERVACION", "RUBRO_CONTABLE", "CUENTA_CONTABLE", "DETALLE", "CODIGO_CUENTA", "FUENTE", "ALERTA_HOJAS", "TIPO_COMPROBANTE", "NRO_COMPROBANTE"},
    Expandido = Table.ExpandTableColumn(LecturaContenido, "Data", ColumnasAExpandir),
    
    // 6. Generar ID Único (Basado en nombre archivo + índice)
    // Útil para trazabilidad y bajas
    Indice = Table.AddIndexColumn(Expandido, "Idx", 1, 1, Int64.Type),
    ID_Unico = Table.AddColumn(Indice, "ID_UNICO", each [Name] & "-" & Text.From([Idx])),
    
    // 7. FILTRADO DE FILAS INVALIDAS
    // A. Descarte de nulos estructurales (Fecha, Obra, Fuente)
    FilasBase = Table.SelectRows(ID_Unico, each [FECHA] <> null and [OBRA_PRONTO] <> null and [FUENTE] <> null),
    
    // B. Reglas de Negocio Específicas
    // Excluir "COMP SYGSA-POSE" SOLO si viene de la carpeta "2021_2022_Historico"
    FilasValidas = Table.SelectRows(FilasBase, each not (Text.Contains([Folder Path], "2021_2022_Historico") and [FUENTE] = "COMP SYGSA-POSE")),
    
    // 8. Limpieza Final
    // DETALLE se mantiene intacto hasta capa Final para parsing centralizado
    // NOTA: Se preserva Name para trazabilidad en análisis y auditoría
    Seleccion = Table.SelectColumns(FilasValidas, {"Name", "ID_UNICO"} & ColumnasAExpandir),
    
    // 9. BUFFER para optimización de rendimiento
    // Esta consulta se referencia en múltiples lugares (Modif, Control_Auditoria, Analisis, etc.)
    // Buffer evita re-ejecución completa cada vez
    Buffered = Table.Buffer(Seleccion)
in
    Buffered