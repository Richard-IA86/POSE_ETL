let
    // =========================================================================
    // DIMENSIÓN: TIPO COMPROBANTE
    // =========================================================================
    // Objetivo: Normalizar abreviaturas de tipo de comprobante a nombres estándar.
    // Mapea: ABREVIATURA (ej: "FC") → TIPO_COMPROBANTE (ej: "FACTURA")
    // =========================================================================

    // 1. Conexión Centralizada
    Origen = Con_Maestros,
    
    // 2. Obtener Datos
    Data = Origen{[Item="Loockup_TipComp",Kind="Table"]}[Data],
    
    // 3. Tipado de Columnas
    Tipos = Table.TransformColumnTypes(Data, {
        {"TIPO_COMPROBANTE", type text}, 
        {"COEF", Int64.Type}, 
        {"ABREVIATURA", type text}
    }),
    
    // 4. NORMALIZACIÓN ROBUSTA (UPPER + TRIM + CLEAN)
    Normalizado = Table.TransformColumns(Tipos, {
        {"TIPO_COMPROBANTE", each Text.Clean(Text.Trim(Text.Upper(Text.From(_)))), type text},
        {"ABREVIATURA", each Text.Clean(Text.Trim(Text.Upper(Text.From(_)))), type text}
    }),
    
    // 5. Deduplicación (Llave: ABREVIATURA + TIPO_COMPROBANTE)
    // En caso de duplicados, toma la primera ocurrencia
    SinDuplicados = Table.Distinct(Normalizado, {"ABREVIATURA", "TIPO_COMPROBANTE"}),
    
    // 6. Filtrar registros válidos (ABREVIATURA no nula)
    Filtrado = Table.SelectRows(SinDuplicados, each [ABREVIATURA] <> null and [ABREVIATURA] <> ""),
    
    // 7. Selección Final (Solo columnas relevantes para join)
    Seleccion = Table.SelectColumns(Filtrado, {"ABREVIATURA", "TIPO_COMPROBANTE", "COEF"})
in
    Seleccion
