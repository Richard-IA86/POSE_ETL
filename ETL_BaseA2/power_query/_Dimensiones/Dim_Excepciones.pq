let
    // =========================================================================
    // DIMENSIÓN: EXCEPCIONES DE GERENCIA (Reglas de Negocio)
    // =========================================================================
    // Objetivo: Definir reglas temporales (por fecha) para sobrescribir la Gerencia.
    // =========================================================================

    // 1. Conexión
    Origen = Con_Maestros,
    
    // 2. Obtener Datos
    // Se usa el nombre de objeto "Loockup_Excepciones_Gerencia"
    Data = Origen{[Item="Loockup_Excepciones_Gerencia",Kind="Table"]}[Data],
    
    // 3. NORMALIZACIÓN DE ENCABEZADOS (Espacios en nombres)
    // El origen tiene columnas con espacios: "OBRA_PRONTO ", " FECHA_DESDE ", etc.
    HeadersLimpios = Table.TransformColumnNames(Data, each Text.Upper(Text.Trim(_))),
    
    // 4. NORMALIZACIÓN DE VALORES
    // Aplicar padding SOLO a valores numéricos puros (evita "HYDRA" -> "000HYDRA")
    Normalizado = Table.TransformColumns(HeadersLimpios, {
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
        {"GERENCIA_OVERRIDE", each Text.Upper(Text.Trim(Text.Clean(Text.From(_)))), type text},
        {"ACTIVO", each Text.Upper(Text.Trim(Text.Clean(Text.From(_)))), type text}
    }),
    
    // 5. Filtrar solo reglas ACTIVAS y válidas
    ReglasActivas = Table.SelectRows(Normalizado, each 
        ([ACTIVO] = "SI") and 
        ([OBRA_PRONTO] <> null) and 
        ([FECHA_DESDE] <> null) and 
        ([FECHA_HASTA] <> null)
    ),

    // 6. Selección Final
    Seleccion = Table.SelectColumns(ReglasActivas, {
        "OBRA_PRONTO", "FECHA_DESDE", "FECHA_HASTA", "GERENCIA_OVERRIDE"
    }),

    // 7. Buffer Opcional (Para rendimiento en joins)
    Buffer = Table.Buffer(Seleccion)
in
    Buffer