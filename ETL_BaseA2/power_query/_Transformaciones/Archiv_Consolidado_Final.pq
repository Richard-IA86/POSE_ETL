let
    // =========================================================================
    // TRANSFORMACIÓN FINAL: Consolidados
    // =========================================================================
    // Consuma Archiv_Consolidado_Modif (Thin Fact Table) y enriquece.
    // =========================================================================

    // Nota: Aquí se deben Referenciar las consultas previas una vez creadas.
    // Por ahora, asumimos que 'Archiv_Consolidado_Modif' está disponible en el entorno.
    Fuente = Archiv_Consolidado_Modif, 

    // RENOMBRAR: Mover columnas origen a nombres temporales para dar prioridad
    // Si no existen (viejo), no falla.
    FuentePrep = Table.RenameColumns(Fuente, {
        {"TIPO_COMPROBANTE", "_SRC_TIPO"}, 
        {"NRO_COMPROBANTE", "_SRC_NRO"}
    }, MissingField.Ignore),

    // -----------------------------------------------------------------------
    // 0. PARSING Y UNIFICACIÓN (PRIORIDAD: Origen > Parsing)
    // -----------------------------------------------------------------------
    // Extrae datos de campo DETALLE donde sea necesario (centralizado en capa Final)
    // PRIORIDAD: Si ya viene dato en _SRC_TIPO/_SRC_NRO (del Excel), se usa ese.
    // -----------------------------------------------------------------------
    
    ConParsing = Table.AddColumn(FuentePrep, "DatosExtraidos", each 
        let
            // Check fuente (Prioridad 1)
            srcT = try [_SRC_TIPO] otherwise null,
            srcN = try [_SRC_NRO] otherwise null,
            hasSrc = (srcT <> null and srcT <> "") and (srcN <> null and srcN <> ""),

            detalle = if [DETALLE] <> null then Text.From([DETALLE]) else "",
            fuente = if [FUENTE] <> null then Text.From([FUENTE]) else "",
            
            // Validar patrones soportados:
            //   1. (Ref.XXXXX( o (Ref.XXXXX) - Comprobantes normales
            //   2. (OT) XXXXXXXX - Operaciones
            // Solo procesar registros de fuente "PRONTO"
            tieneRef = Text.Contains(detalle, "(Ref."),
            tieneOT = Text.Contains(detalle, "(OT)"),
            tieneProv = Text.Contains(detalle, " - Prov"),
            esFuenteValida = fuente = "PRONTO",
            esParseable = ((tieneRef or tieneOT) and tieneProv) and esFuenteValida,
            
            resultado = if hasSrc then
                 [TipoParseado = srcT, NroParseado = srcN]
            else if esParseable then
                let
                    // ============================================================
                    // PASO 0: IDENTIFICAR TIPO DE FORMATO
                    // ============================================================
                    esFormatoOT = tieneOT,
                    esFormatoRef = tieneRef and not esFormatoOT,
                    
                    // ============================================================
                    // PASO 1: ENCONTRAR POSICIONES CLAVE
                    // ============================================================
                    posProv = Text.PositionOf(detalle, " - Prov"),
                    
                    // ============================================================
                    // PASO 2: EXTRACCIÓN DE TIPO_COMPROBANTE
                    // ============================================================
                    tipoLimpio = if esFormatoOT then
                        "OPERACION"
                    else if esFormatoRef then
                        // Formato (Ref.: Extraer SIGLA antes de (Ref.
                        let
                            posRef = Text.PositionOf(detalle, "(Ref."),
                            textoAntesRef = Text.Start(detalle, posRef),
                            textoAntesRefTrim = Text.TrimEnd(textoAntesRef),
                            posUltimoEspacio = Text.PositionOfAny(textoAntesRefTrim, {" "}, Occurrence.Last),
                            
                            tipoRaw = if posUltimoEspacio >= 0 
                                      then Text.Range(textoAntesRefTrim, posUltimoEspacio + 1) 
                                      else textoAntesRefTrim
                        in
                            if tipoRaw <> null and tipoRaw <> "" then Text.Trim(tipoRaw) else null
                    else
                        null,
                    
                    // ============================================================
                    // PASO 3: EXTRACCIÓN DE NRO_COMPROBANTE
                    // ============================================================
                    nroLimpio = if esFormatoOT then
                        // Formato (OT): Extraer número entre (OT) y - Prov
                        let
                            posOT = Text.PositionOf(detalle, "(OT)"),
                            inicioNumero = posOT + 4,  // Saltar "(OT)"
                            nroRaw = if inicioNumero > 0 and posProv > inicioNumero then
                                Text.Range(detalle, inicioNumero, posProv - inicioNumero)
                            else
                                null
                        in
                            if nroRaw <> null then Text.Trim(nroRaw) else null
                    else if esFormatoRef then
                        // Formatos (Ref.: con cierre ) o sin cierre (
                        let
                            posRef = Text.PositionOf(detalle, "(Ref.")
                        in
                            if posRef >= 0 and posProv > 0 then
                        let
                            // Extraer texto desde después de "(Ref." hasta " - Prov"
                            textoDesdeRef = Text.Range(detalle, posRef + 5), // +5 para saltar "(Ref."
                            
                            // Buscar separadores: "(" para formato nuevo, ")" para formato viejo
                            posSegundoParentesis = Text.PositionOf(textoDesdeRef, "("),
                            posCierreParentesis = Text.PositionOf(textoDesdeRef, ")"),
                            
                            // Determinar cuál aparece primero
                            inicioComprobante = if posSegundoParentesis >= 0 and (posCierreParentesis < 0 or posSegundoParentesis < posCierreParentesis) then
                                // Formato nuevo: (Ref.XXXXX( comprobante
                                posRef + 5 + posSegundoParentesis + 1
                            else if posCierreParentesis >= 0 then
                                // Formato viejo: (Ref.XXXXX) comprobante
                                posRef + 5 + posCierreParentesis + 1
                            else
                                // Fallback: buscar primer espacio
                                let
                                    posEspacio = Text.PositionOf(textoDesdeRef, " ")
                                in
                                    if posEspacio >= 0 then posRef + 5 + posEspacio + 1 else -1,
                            
                            // Extraer desde inicio del comprobante hasta " - Prov"
                            nroRaw = if inicioComprobante > 0 and posProv > inicioComprobante then
                                Text.Range(detalle, inicioComprobante, posProv - inicioComprobante)
                            else
                                null
                            in
                                if nroRaw <> null then Text.Trim(nroRaw) else null
                            else
                                null
                    else
                        null
                in
                    [TipoParseado = tipoLimpio, NroParseado = nroLimpio]
            else
                [TipoParseado = null, NroParseado = null]
        in
            resultado
    ),
    
    // Crear columnas TIPO_COMPROBANTE y NRO_COMPROBANTE desde parsing
    ConTipoComprobante = Table.AddColumn(ConParsing, "TIPO_COMPROBANTE", each 
        [DatosExtraidos][TipoParseado],
        type nullable text
    ),
    
    ConNroComprobante = Table.AddColumn(ConTipoComprobante, "NRO_COMPROBANTE", each 
        [DatosExtraidos][NroParseado],
        type nullable text
    ),
    
    // Limpiar columnas temporales (record y fuentes origen ya usadas)
    // Se eliminan _SRC_TIPO y _SRC_NRO porque ya se consolidaron en las columnas finales
    SinTemporales = Table.RemoveColumns(ConNroComprobante, {"DatosExtraidos", "_SRC_TIPO", "_SRC_NRO"}, MissingField.Ignore),

    // PRE-JOIN: Asegurar formato OBRA_PRONTO (Padding 8 dígitos) para garantizar Match con Dimensión
    // Si viene "139", convertir a "00000139". Si ya tiene ceros, dejar igual.
    SinTemporalesNorm = Table.TransformColumns(SinTemporales, {
        {"OBRA_PRONTO", each 
            if _ <> null then
                let
                    txt = Text.From(_),
                    isNum = try Number.From(txt) otherwise null
                in
                    // Numéricos: padding | Alfanuméricos: UPPER (ej: "Sin Obra" -> "SIN OBRA")
                    if isNum <> null then Text.PadStart(txt, 8, "0") else Text.Upper(Text.Trim(txt))
            else null, 
        type text}
    }),

    // -----------------------------------------------------------------------
    // 1. JOIN CON DIMENSIÓN OBRAS (Descripción, Compensable, Gerencia Std)
    // -----------------------------------------------------------------------
    JoinObra = Table.NestedJoin(SinTemporalesNorm, {"OBRA_PRONTO"}, Dim_Obras, {"OBRA_PRONTO"}, "DimObra", JoinKind.LeftOuter),
    ExpandObra = Table.ExpandTableColumn(JoinObra, "DimObra", 
        {"DESCRIPCION_OBRA", "COMPENSABLE", "GERENCIA"}, 
        {"DESCRIPCION_OBRA_LOOKUP", "COMPENSABLE_LOOKUP", "GERENCIA_STD"}
    ),

    // -----------------------------------------------------------------------
    // 2. JOIN CON DIMENSIÓN CALENDARIO (TC)
    // -----------------------------------------------------------------------
    // Se corrige referencia a campo "FECHA" (todo mayúscula por normalización en dimensión)
    JoinTC = Table.NestedJoin(ExpandObra, {"FECHA"}, Dim_Calendario_TC, {"FECHA"}, "DimTC", JoinKind.LeftOuter),
    ExpandTC = Table.ExpandTableColumn(JoinTC, "DimTC", {"TC"}, {"TC_LOOKUP"}),

    // -----------------------------------------------------------------------
    // 2.5. JOIN CON DIMENSIÓN TIPO COMPROBANTE (Normalización)
    // -----------------------------------------------------------------------
    // Normalizar abreviaturas (FC, NC, ND, etc.) a nombres completos (FACTURA, NOTA CREDITO, etc.)
    // Solo si TIPO_COMPROBANTE existe y no es nulo
    
    // A. Normalizar campo de unión para garantizar coincidencias
    NormTipoComp = Table.TransformColumns(ExpandTC, {
        {"TIPO_COMPROBANTE", each if _ <> null then Text.Clean(Text.Trim(Text.Upper(Text.From(_)))) else null, type nullable text}
    }),
    
    // B. Join con lookup de tipos de comprobante
    JoinTipoComp = Table.NestedJoin(
        NormTipoComp, 
        {"TIPO_COMPROBANTE"}, 
        Dim_TipComprobante, 
        {"ABREVIATURA"}, 
        "DimTipoComp", 
        JoinKind.LeftOuter
    ),
    
    // C. Expandir y consolidar: Priorizar lookup, fallback a valor original
    ExpandTipoComp = Table.ExpandTableColumn(JoinTipoComp, "DimTipoComp", 
        {"TIPO_COMPROBANTE", "COEF"}, 
        {"TIPO_COMPROBANTE_NORMALIZADO", "COEF_TIPO_COMP"}
    ),
    
    ConsolidarTipoComp = Table.AddColumn(ExpandTipoComp, "TIPO_COMPROBANTE_FINAL", each
        if [TIPO_COMPROBANTE_NORMALIZADO] <> null 
        then [TIPO_COMPROBANTE_NORMALIZADO] 
        else [TIPO_COMPROBANTE],
        type nullable text
    ),
    
    // D. Limpiar columnas temporales
    LimpiarTipoComp = Table.RemoveColumns(ConsolidarTipoComp, {"TIPO_COMPROBANTE", "TIPO_COMPROBANTE_NORMALIZADO"}),
    RenombrarTipoComp = Table.RenameColumns(LimpiarTipoComp, {{"TIPO_COMPROBANTE_FINAL", "TIPO_COMPROBANTE"}}),

    // -----------------------------------------------------------------------
    // 2.6. JOIN CON DIMENSIÓN FUENTES (Normalización)
    // -----------------------------------------------------------------------
    // Normalizar variaciones de nombres de fuente a nomenclatura estándar
    // Ejemplos: "IMPUTACION DIRECTA" → "IMPUT DIRECTA", "QUINCENA" → "QUINCENAS"
    
    // A. Función de normalización AGRESIVA (debe coincidir con Dim_Fuentes)
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
    
    // B. Normalizar campo de unión
    NormFuente = Table.TransformColumns(RenombrarTipoComp, {
        {"FUENTE", each fnNormalizarFuente(_), type nullable text}
    }),
    
    // C. Join con lookup de fuentes
    JoinFuente = Table.NestedJoin(
        NormFuente, 
        {"FUENTE"}, 
        Dim_Fuentes, 
        {"FUENTE_ORIG"}, 
        "DimFuente", 
        JoinKind.LeftOuter
    ),
    
    // D. Expandir y consolidar: Priorizar lookup, fallback a valor original
    ExpandFuente = Table.ExpandTableColumn(JoinFuente, "DimFuente", 
        {"FUENTE_INFORME"}, 
        {"FUENTE_NORMALIZADA"}
    ),
    
    ConsolidarFuente = Table.AddColumn(ExpandFuente, "FUENTE_FINAL", each
        if [FUENTE_NORMALIZADA] <> null 
        then [FUENTE_NORMALIZADA] 
        else [FUENTE],
        type nullable text
    ),
    
    // E. Limpiar columnas temporales
    LimpiarFuente = Table.RemoveColumns(ConsolidarFuente, {"FUENTE", "FUENTE_NORMALIZADA"}),
    RenombrarFuente = Table.RenameColumns(LimpiarFuente, {{"FUENTE_FINAL", "FUENTE"}}),

    // -----------------------------------------------------------------------
    // 3. APLICACIÓN DE EXCEPCIONES DE GERENCIA
    // -----------------------------------------------------------------------
    ExcepcionesBuffer = Table.Buffer(Dim_Excepciones),

    CalculoGerencia = Table.AddColumn(RenombrarFuente, "GERENCIA_FINAL", each
        let
            obra = [OBRA_PRONTO],
            fecha = [FECHA],
            // Buscar en reglas de excepción
            excepcion = Table.SelectRows(ExcepcionesBuffer, (r) => 
                r[OBRA_PRONTO] = obra and fecha >= r[FECHA_DESDE] and fecha <= r[FECHA_HASTA]
            ),
            resultado = if Table.RowCount(excepcion) > 0 then excepcion{0}[GERENCIA_OVERRIDE]
                        else if [GERENCIA_STD] <> null then [GERENCIA_STD]
                        else "SIN CLASIFICAR" // O null
        in
            resultado,
        type text
    ),

    // -----------------------------------------------------------------------
    // 4. CÁLCULO DE MÉTRICAS (USD)
    // -----------------------------------------------------------------------
    CalculoUSD = Table.AddColumn(CalculoGerencia, "IMPORTE_USD_FINAL", each
        if [TC_LOOKUP] <> null and [TC_LOOKUP] <> 0 
        then Number.Round([IMPORTE] / [TC_LOOKUP], 2)
        else null,
        type number
    ),

    // -----------------------------------------------------------------------
    // 5. RENOMBRADO DE COLUMNAS LOOKUP
    // -----------------------------------------------------------------------
    RenombradoInicial = Table.RenameColumns(CalculoUSD, {
        {"TC_LOOKUP", "TC"},
        {"IMPORTE_USD_FINAL", "IMPORTE_USD"},
        {"GERENCIA_FINAL", "GERENCIA"},
        {"DESCRIPCION_OBRA_LOOKUP", "DESCRIPCION_OBRA"},
        {"COMPENSABLE_LOOKUP", "COMPENSABLE"}
    }),
    
    // Eliminar columna COEF_TIPO_COMP (no requerida en BD)
    SinCoef = Table.RemoveColumns(RenombradoInicial, {"COEF_TIPO_COMP"}, MissingField.Ignore),

    // -----------------------------------------------------------------------
    // 6. FORMATEAR OBRA_PRONTO: Numéricos con padding 8 dígitos, Texto sin cambios
    // -----------------------------------------------------------------------
    // Lógica adaptada para BD híbrida:
    //   - Valores numéricos (139) → "00000139"
    //   - Valores texto ("pose 139") → "pose 139" (sin modificar)
    // El script Python intentará convertir a INT solo si es numérico (línea 194-195)
    ObraFormateadaFinal = Table.TransformColumns(
        SinCoef,
        {{"OBRA_PRONTO", each 
            if _ <> null then
                let
                    textoOriginal = Text.From(_),
                    // Intentar conversión a número para detectar si es numérico puro
                    esNumerico = try Number.From(textoOriginal) otherwise null,
                    resultado = if esNumerico <> null 
                                then Text.PadStart(textoOriginal, 8, "0")  // Aplicar padding
                                else Text.Upper(Text.Trim(textoOriginal))  // Texto: UPPER (ej: "Sin Obra" -> "SIN OBRA")
                in
                    resultado
            else 
                null, 
        type text}}
    ),
    
    // -----------------------------------------------------------------------
    // 7. SELECCIONAR COLUMNAS ESPERADAS POR 03_cargar_costos_A2.py + ID_UNICO
    // -----------------------------------------------------------------------
    // Según COLUMNAS_ESPERADAS líneas 45-52 del script Python
    SeleccionFinal = Table.SelectColumns(
        ObraFormateadaFinal,
        {
            "Name",                 // Nombre del archivo Excel de origen (para trazabilidad)
            "ID_UNICO",             // Identificador único para auditoría y debugging
            "OBRA_PRONTO",          // TEXT con padding "00000001" (Python convierte a INT)
            "FECHA",                // DATE (requerido)
            "IMPORTE",              // FLOAT (requerido)
            "TC",                   // FLOAT (opcional)
            "IMPORTE_USD",          // FLOAT (opcional)
            "GERENCIA",             // VARCHAR(200) (opcional)
            "PROVEEDOR",            // VARCHAR(300) (opcional)
            "NRO_COMPROBANTE",      // VARCHAR(100) (opcional)
            "TIPO_COMPROBANTE",     // VARCHAR(100) (opcional)
            "OBSERVACION",          // VARCHAR(4000) (opcional)
            "RUBRO_CONTABLE",       // VARCHAR(50) (opcional)
            "CUENTA_CONTABLE",      // VARCHAR(200) (opcional)
            "CODIGO_CUENTA",        // VARCHAR(50) (opcional)
            "COMPENSABLE",          // VARCHAR(50) (opcional)
            "FUENTE",               // VARCHAR(50) (opcional)
            "DESCRIPCION_OBRA",     // VARCHAR(300) (opcional)
            "DETALLE"               // VARCHAR(810) (opcional)
        },
        MissingField.UseNull  // Si falta alguna columna, rellenar con null
    ),
    
    // -----------------------------------------------------------------------
    // 8. REORDENAR COLUMNAS PARA FACILITAR VISUALIZACIÓN
    // -----------------------------------------------------------------------
    // Orden: ID_UNICO → Campos clave → Campos monetarios → Campos descriptivos
    ColumnasReordenadas = Table.ReorderColumns(
        SeleccionFinal,
        {
            "ID_UNICO",
            "OBRA_PRONTO",
            "FECHA",
            "IMPORTE",
            "TC",
            "IMPORTE_USD",
            "GERENCIA",
            "FUENTE",
            "PROVEEDOR",
            "NRO_COMPROBANTE",
            "TIPO_COMPROBANTE",
            "DESCRIPCION_OBRA",
            "DETALLE",
            "OBSERVACION",
            "RUBRO_CONTABLE",
            "CUENTA_CONTABLE",
            "CODIGO_CUENTA",
            "COMPENSABLE"
        }
    ),
    
    // -----------------------------------------------------------------------
    // 9. BUFFER para optimización de rendimiento
    // -----------------------------------------------------------------------
    // Esta consulta se referencia en Control_Auditoria_ETL y otros reportes
    // Buffer evita re-ejecución de todo el pipeline (JOINs + enriquecimientos)
    Buffered = Table.Buffer(ColumnasReordenadas)
in
    Buffered