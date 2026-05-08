let
    // =========================================================================
    // DIMENSIÓN: OBRAS (Unificada)
    // =========================================================================
    // Objetivo: Proveer Descripción, Compensable y Gerencia Estándar por OBRA_PRONTO.
    // Aplica Protocolo de Limpieza v2.1 (Config_Consultas_ETL)
    // =========================================================================

    // 1. Conexión Centralizada
    Origen = Con_Maestros, 
    
    // 2. Obtener Datos desde SHEET
    // La sheet "Obras_Gerencias" tiene columnas duplicadas. Necesitamos:
    // - Columna H: "Obra" → OBRA_PRONTO
    // - Columna I: "Descripcion obra" → DESCRIPCION_OBRA  
    // - Columna J: "Gerente de obra" → GERENCIA
    // - Columna C: "COMPENSABLE" → COMPENSABLE
    SheetData = Origen{[Item="Obras_Gerencias",Kind="Sheet"]}[Data],
    ConHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars=true]),
    
    // 3. Desambiguar columnas duplicadas agregando sufijo por posición
    // Esto convierte: [Obra, ..., Obra] → [Obra, ..., Obra.1]
    ColNames = Table.ColumnNames(ConHeaders),
    ColNamesCounts = List.Accumulate(
        ColNames,
        [counts = [], seen = []],
        (state, name) =>
            let
                prevCount = List.PositionOf(state[seen], name, Occurrence.First),
                newCount = if prevCount = -1 then 0 else List.Count(List.Select(state[seen], each _ = name)),
                newName = if newCount = 0 then name else name & "." & Text.From(newCount)
            in
                [counts = state[counts] & {newName}, seen = state[seen] & {name}]
    )[counts],
    
    ConHeadersUnicos = Table.RenameColumns(
        ConHeaders,
        List.Zip({ColNames, ColNamesCounts})
    ),
    
    // 4. SELECCIONAR las 4 columnas por nombre desambiguado
    // Basado en estructura observada: COMPENSABLE (col 2), Obra (col 7), Descripcion obra (col 8), Gerente de obra (col 9)
    Seleccionadas = Table.SelectColumns(ConHeadersUnicos, {"Obra", "Descripcion obra", "Gerente de obra", "COMPENSABLE"}),
    
    // 5. RENOMBRAR a nombres estándar
    ConNombresFinal = Table.RenameColumns(Seleccionadas, {
        {"Obra", "OBRA_PRONTO"},
        {"Descripcion obra", "DESCRIPCION_OBRA"},
        {"Gerente de obra", "GERENCIA"},
        {"COMPENSABLE", "COMPENSABLE"}
    }),
    
    // 6. REMOVER HEADER ROW (fila 1 = nombres originales)
    SinHeaderRow = Table.Skip(ConNombresFinal, 1),
    
    // 5. NORMALIZACIÓN DE VALORES (Upper + Trim + Clean OBLIGATORIO)
    // Aplicar padding SOLO a valores numéricos puros (evita "HYDRA" -> "000HYDRA")
    Normalizado = Table.TransformColumns(SinHeaderRow, {
         {"OBRA_PRONTO", each 
            if _ <> null then
                let
                    txt = Text.From(_),
                    isNum = try Number.From(txt) otherwise null
                in
                    // Numéricos: padding | Alfanuméricos: UPPER (ej: "Sin Obra" -> "SIN OBRA")
                    if isNum <> null then Text.PadStart(txt, 8, "0") else Text.Upper(Text.Trim(txt))
            else null,
         type text},
         {"DESCRIPCION_OBRA", each if _ <> null then Text.Upper(Text.Trim(Text.Clean(Text.From(_)))) else null, type text},
         {"GERENCIA", each if _ <> null then Text.Upper(Text.Trim(Text.Clean(Text.From(_)))) else null, type text},
         {"COMPENSABLE", each if _ <> null then Text.Upper(Text.Trim(Text.Clean(Text.From(_)))) else null, type text}
    }),
    
    // 6. Validar Integridad (Filtrar nulos críticos)
    Filtrado = Table.SelectRows(Normalizado, each [OBRA_PRONTO] <> null),

    // 7. DEDUPLICACIÓN (Cardinalidad 1:1 OBLIGATORIA)
    Distinct = Table.Distinct(Filtrado, {"OBRA_PRONTO"})
in
    Distinct