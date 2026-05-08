let
    // =========================================================================
    // DIMENSIÓN: FUENTES NORMALIZADAS
    // =========================================================================
    // Objetivo: Normalizar nombres de fuentes variados a nombres estándar
    // Origen: Loockups.xlsx - Hoja "Fuente"
    // Estructura: FUENTE_ORIG → FUENTE_INFORME
    // =========================================================================
    
    // 1. CARGAR ARCHIVO Y TABLA
    // -------------------------------------------------------------------------
    Origen = Excel.Workbook(
        File.Contents(Param_RutaBase & "\data\sistema\Loockups.xlsx"), 
        null, 
        true
    ),
    
    TablaFuente = Origen{[Item="Fuente",Kind="Table"]}[Data],
    
    // 2. TIPAR COLUMNAS
    // -------------------------------------------------------------------------
    TiposCambiados = Table.TransformColumnTypes(TablaFuente, {
        {"FUENTE_ORIG", type text}, 
        {"FUENTE_INFORME", type text}
    }),
    
    // 3. NORMALIZACIÓN AGRESIVA DE CLAVE
    // -------------------------------------------------------------------------
    // Remueve acentos, espacios múltiples, caracteres especiales
    // Esto es vital para que el VLOOKUP no falle por variaciones menores
    fnNormalizarClave = (texto as nullable text) as nullable text =>
        if texto = null or texto = "" then null
        else
            let
                mayusculas = Text.Upper(texto),
                // Remover acentos
                sinAcentos = Text.Replace(Text.Replace(Text.Replace(Text.Replace(Text.Replace(
                    Text.Replace(Text.Replace(Text.Replace(Text.Replace(Text.Replace(
                    mayusculas,
                    "Á", "A"), "É", "E"), "Í", "I"), "Ó", "O"), "Ú", "U"),
                    "Ñ", "N"), "Ü", "U"), "À", "A"), "È", "E"), "Ì", "I"),
                // Remover saltos de línea y caracteres de control
                sinSaltos = Text.Replace(Text.Replace(sinAcentos, "#(lf)", " "), "#(cr)", " "),
                // Remover pipes y caracteres especiales comunes
                sinEspeciales = Text.Replace(Text.Replace(sinSaltos, "|", ""), "_", " "),
                // Limpiar caracteres invisibles
                limpio = Text.Clean(sinEspeciales),
                // Remover espacios múltiples y trimear
                sinDoblesEspacios = Text.Trim(Text.Combine(List.RemoveNulls(Text.Split(limpio, " ")), " "))
            in
                sinDoblesEspacios,
    
    // 4. APLICAR NORMALIZACIÓN A AMBAS COLUMNAS
    // -------------------------------------------------------------------------
    ClaveLimpia = Table.TransformColumns(TiposCambiados, {
        {"FUENTE_ORIG", each fnNormalizarClave(_), type text},
        {"FUENTE_INFORME", each if _ <> null then Text.Upper(Text.Trim(_)) else null, type text}
    }),

    // 5. ELIMINAR DUPLICADOS Y FILAS VACÍAS
    // -------------------------------------------------------------------------
    FiltroVacios = Table.SelectRows(ClaveLimpia, each 
        [FUENTE_ORIG] <> null and [FUENTE_ORIG] <> "" and
        [FUENTE_INFORME] <> null and [FUENTE_INFORME] <> ""
    ),
    
    SinDuplicados = Table.Distinct(FiltroVacios, {"FUENTE_ORIG"}),
    
    // 6. OPTIMIZACIÓN: BUFFER PARA EVITAR RELECTURAS
    // -------------------------------------------------------------------------
    Resultado = Table.Buffer(SinDuplicados)

in
    Resultado
