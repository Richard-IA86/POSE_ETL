let
    // =========================================================================
    // CONSULTA DEBUG: Validación de Normalización de FUENTE
    // =========================================================================
    // Objetivo: Verificar que los valores de FUENTE se normalizan correctamente
    // =========================================================================

    // 1. Tomar muestra de Archiv_Consolidado_Modif
    Fuente = Archiv_Consolidado_Modif,
    
    // 2. Función de normalización AGRESIVA (debe coincidir con Dim_Fuentes)
    fnNormalizarFuente = (texto as nullable text) as nullable text =>
        if texto = null or texto = "" then null
        else
            let
                mayusculas = Text.Upper(Text.From(texto)),
                // Remover acentos
                sinAcentos = Text.Replace(Text.Replace(Text.Replace(Text.Replace(Text.Replace(
                    Text.Replace(Text.Replace(Text.Replace(Text.Replace(Text.Replace(
                    mayusculas,
                    "Á", "A"), "É", "E"), "Í", "I"), "Ó", "O"), "Ú", "U"),
                    "Ñ", "N"), "Ü", "U"), "À", "A"), "È", "E"), "Ì", "I"),
                // Remover caracteres especiales y espacios múltiples
                limpio = Text.Clean(Text.Trim(sinAcentos)),
                sinDoblesEspacios = Text.Trim(Text.Combine(List.RemoveNulls(Text.Split(limpio, " ")), " "))
            in
                sinDoblesEspacios,
    
    // 3. Normalizar FUENTE (mismo proceso que en Final)
    NormFuente = Table.TransformColumns(Fuente, {
        {"FUENTE", each fnNormalizarFuente(_), type nullable text}
    }),
    
    // 3. Join con Dim_Fuentes
    JoinFuente = Table.NestedJoin(
        NormFuente, 
        {"FUENTE"}, 
        Dim_Fuentes, 
        {"FUENTE_ORIG"}, 
        "DimFuente", 
        JoinKind.LeftOuter
    ),
    
    // 4. Expandir lookup
    ExpandFuente = Table.ExpandTableColumn(JoinFuente, "DimFuente", 
        {"FUENTE_INFORME"}, 
        {"FUENTE_NORMALIZADA"}
    ),
    
    // 5. Consolidar con fallback
    ConValidacion = Table.AddColumn(ExpandFuente, "FUENTE_FINAL", each
        if [FUENTE_NORMALIZADA] <> null 
        then [FUENTE_NORMALIZADA] 
        else [FUENTE],
        type nullable text
    ),
    
    // 6. Agregar indicadores de validación
    ConIndicadores = Table.AddColumn(ConValidacion, "NORMALIZADO", each
        [FUENTE_NORMALIZADA] <> null and [FUENTE] <> [FUENTE_NORMALIZADA],
        type logical
    ),
    
    ConSinCambio = Table.AddColumn(ConIndicadores, "SIN_CAMBIO", each
        [FUENTE_NORMALIZADA] = null or [FUENTE] = [FUENTE_NORMALIZADA],
        type logical
    ),
    
    ConSinLookup = Table.AddColumn(ConSinCambio, "SIN_LOOKUP", each
        [FUENTE_NORMALIZADA] = null,
        type logical
    ),
    
    // 7. Seleccionar columnas relevantes
    Resultado = Table.SelectColumns(ConSinLookup, {
        "ID_UNICO",
        "FUENTE",
        "FUENTE_NORMALIZADA",
        "FUENTE_FINAL",
        "NORMALIZADO",
        "SIN_CAMBIO",
        "SIN_LOOKUP",
        "FECHA",
        "OBRA_PRONTO",
        "IMPORTE"
    })
in
    Resultado
