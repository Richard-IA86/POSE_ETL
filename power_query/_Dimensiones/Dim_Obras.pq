let
    // =========================================================================
    // DIMENSIÓN: OBRAS (Unificada)
    // =========================================================================
    // Objetivo: Proveer Descripción, Compensable y Gerencia Estándar por OBRA_PRONTO.
    // Aplica Protocolo de Limpieza v2.1 (Config_Consultas_ETL)
    // =========================================================================

    // 1. Conexión Centralizada
    Origen = Con_Maestros, 
    
    // 2. Obtener Datos (Estrategia: Usar TABLA estructurada para aislar datos)
    // Se usa "Loockup_ObrasGerencia" según indicación del usuario (Nombre real en Excel)
    Data = Origen{[Item="Loockup_ObrasGerencia",Kind="Table"]}[Data],
    
    // 3. NORMALIZACIÓN ROBUSTA DE ENCABEZADOS
    // Paso crítico: Convertir todos los encabezados a Mayúsculas y quitar espacios
    // Esto arregla "Compensable ", "compensable", "Obra ", etc.
    HeadersLimpios = Table.TransformColumnNames(Data, each Text.Upper(Text.Trim(_))),
    
    // 4. Renombrar Columnas (Usando nombres en Mayúsculas previstos)
    Renombrar = Table.RenameColumns(HeadersLimpios, {
        {"OBRA", "OBRA_PRONTO"},            // Antes "Obra"
        {"DESCRIPCION OBRA", "DESCRIPCION_OBRA"}, // Antes "Descripcion obra"
        {"GERENTE DE OBRA", "GERENCIA"},    // Antes "Gerente de obra"
        // "COMPENSABLE" ya debería estar en mayúsculas y listo
        {"COMPENSABLE", "COMPENSABLE"}      // Auto-mapeo por seguridad
    }, MissingField.Ignore),

    // 5. Selección de Columnas
    Seleccion = Table.SelectColumns(Renombrar, {
        "OBRA_PRONTO", 
        "DESCRIPCION_OBRA", 
        "COMPENSABLE", 
        "GERENCIA" 
    }),
    
    // 6. NORMALIZACIÓN DE VALORES (Upper + Trim + Clean OBLIGATORIO)
    // Aplicar padding SOLO a valores numéricos puros (evita "HYDRA" -> "000HYDRA")
    Normalizado = Table.TransformColumns(Seleccion, {
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
         {"DESCRIPCION_OBRA", each Text.Upper(Text.Trim(Text.Clean(Text.From(_)))), type text},
         {"GERENCIA", each Text.Upper(Text.Trim(Text.Clean(Text.From(_)))), type text},
         {"COMPENSABLE", each Text.Upper(Text.Trim(Text.Clean(Text.From(_)))), type text}
    }),
    
    // 5. Validar Integridad (Filtrar nulos críticos)
    Filtrado = Table.SelectRows(Normalizado, each [OBRA_PRONTO] <> null),

    // 6. DEDUPLICACIÓN (Cardinalidad 1:1 OBLIGATORIA)
    Distinct = Table.Distinct(Filtrado, {"OBRA_PRONTO"})
in
    Distinct