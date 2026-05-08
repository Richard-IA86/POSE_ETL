let
    // =========================================================================
    // CONSULTA DEBUG: Análisis de Parsing de DETALLE
    // =========================================================================
    // Objetivo: Ver qué se está extrayendo del campo DETALLE
    // =========================================================================

    // 1. Tomar una muestra de Archiv_Consolidado_Modif
    Fuente = Archiv_Consolidado_Modif,
    
    // 2. Filtrar solo registros con DETALLE no nulo
    ConDetalle = Table.SelectRows(Fuente, each [DETALLE] <> null and [DETALLE] <> ""),
    
    // 3. APLICAR PARSING CON INFORMACIÓN DE DEBUG
    ConDebug = Table.AddColumn(ConDetalle, "ParsingDebug", each 
        let
            detalle = Text.From([DETALLE]),
            fuente = if [FUENTE] <> null then Text.From([FUENTE]) else "",
            
            // Validaciones básicas
            tiene_ref = Text.Contains(detalle, "(Ref."),
            tiene_ot = Text.Contains(detalle, "(OT)"),
            tiene_prov = Text.Contains(detalle, " - Prov"),
            es_fuente_pronto = fuente = "PRONTO",
            esParseable = ((tiene_ref or tiene_ot) and tiene_prov) and es_fuente_pronto,
            
            // Si es parseable, extraer posiciones
            debug_info = if esParseable then
                let
                    esFormatoOT = tiene_ot,
                    esFormatoRef = tiene_ref and not esFormatoOT,
                    
                    posProv = Text.PositionOf(detalle, " - Prov"),
                    
                    // Extraer TIPO
                    tipoLimpio = if esFormatoOT then
                        "OPERACION"
                    else if esFormatoRef then
                        let
                            posRef = Text.PositionOf(detalle, "(Ref."),
                            textoAntesRef = Text.Start(detalle, posRef),
                            textoAntesRefTrim = Text.TrimEnd(textoAntesRef),
                            posUltimoEspacio = Text.PositionOfAny(textoAntesRefTrim, {" "}, Occurrence.Last),
                            
                            tipoRaw = if posUltimoEspacio >= 0 
                                      then Text.Range(textoAntesRefTrim, posUltimoEspacio + 1) 
                                      else textoAntesRefTrim
                        in
                            Text.Trim(tipoRaw)
                    else
                        null,
                    
                    // Extraer NRO (soporta (OT), (Ref.XXX( o (Ref.XXX) )
                    nroLimpio = if esFormatoOT then
                        let
                            posOT = Text.PositionOf(detalle, "(OT)"),
                            inicioNumero = posOT + 4,
                            nroRaw = if inicioNumero > 0 and posProv > inicioNumero then
                                Text.Range(detalle, inicioNumero, posProv - inicioNumero)
                            else
                                null
                        in
                            if nroRaw <> null then Text.Trim(nroRaw) else null
                    else if esFormatoRef then
                        let
                            posRef = Text.PositionOf(detalle, "(Ref."),
                            textoDesdeRef = Text.Range(detalle, posRef + 5),
                            posSegundoParentesis = Text.PositionOf(textoDesdeRef, "("),
                            posCierreParentesis = Text.PositionOf(textoDesdeRef, ")"),
                            
                            inicioComprobante = if posSegundoParentesis >= 0 and (posCierreParentesis < 0 or posSegundoParentesis < posCierreParentesis) then
                                posRef + 5 + posSegundoParentesis + 1
                            else if posCierreParentesis >= 0 then
                                posRef + 5 + posCierreParentesis + 1
                            else
                                let
                                    posEspacio = Text.PositionOf(textoDesdeRef, " ")
                                in
                                    if posEspacio >= 0 then posRef + 5 + posEspacio + 1 else -1,
                            
                            nroRaw = if inicioComprobante > 0 and posProv > inicioComprobante then
                                Text.Range(detalle, inicioComprobante, posProv - inicioComprobante)
                            else
                                null
                            in
                                if nroRaw <> null then Text.Trim(nroRaw) else null
                    else
                        null,
                    
                    // Para debug: extraer texto completo entre marcador y - Prov
                    textoEntreRefYProv = if esFormatoOT then
                        let
                            posOT = Text.PositionOf(detalle, "(OT)")
                        in
                            if posOT >= 0 and posProv > posOT then
                                Text.Range(detalle, posOT + 4, posProv - (posOT + 4))
                            else
                                "N/A"
                    else if esFormatoRef then
                        let
                            posRef = Text.PositionOf(detalle, "(Ref.")
                        in
                            if posRef >= 0 and posProv > posRef then
                                Text.Range(detalle, posRef + 5, posProv - (posRef + 5))
                            else
                                "N/A"
                    else
                        "N/A"
                in
                    [
                        Parseable = "SÍ",
                        posRef = if esFormatoRef then Text.PositionOf(detalle, "(Ref.") else if esFormatoOT then Text.PositionOf(detalle, "(OT)") else -1,
                        posCierreRef = -1,
                        posProv = posProv,
                        TipoExtraido = tipoLimpio,
                        NroExtraido = nroLimpio,
                        EsValido = if tipoLimpio <> null and nroLimpio <> null then "SÍ" else "NO",
                        TextoEntreParentesis = if esFormatoOT then "N/A (formato OT)" else "N/A (sin cierre)",
                        TextoEntreParenYProv = textoEntreRefYProv
                    ]
            else
                [
                    Parseable = "NO",
                    posRef = if tiene_ref then Text.PositionOf(detalle, "(Ref.") else if tiene_ot then Text.PositionOf(detalle, "(OT)") else -1,
                    posCierreRef = -1,
                    posProv = if tiene_prov then Text.PositionOf(detalle, " - Prov") else -1,
                    TipoExtraido = null,
                    NroExtraido = null,
                    EsValido = "NO",
                    TextoEntreParentesis = "N/A",
                    TextoEntreParenYProv = "N/A"
                ]
        in
            debug_info
    ),
    
    // 4. Expandir información de debug
    ExpandDebug = Table.ExpandRecordColumn(ConDebug, "ParsingDebug", 
        {"Parseable", "posRef", "posCierreRef", "posProv", "TipoExtraido", "NroExtraido", "EsValido", "TextoEntreParentesis", "TextoEntreParenYProv"},
        {"Parseable", "PosRef", "PosCierreRef", "PosProv", "TipoExtraido", "NroExtraido", "EsValido", "TextoEntreParentesis", "TextoEntreParenYProv"}
    ),
    
    // 5. Seleccionar columnas relevantes para análisis
    Resultado = Table.SelectColumns(ExpandDebug, {
        "ID_UNICO",
        "FUENTE",
        "DETALLE",
        "Parseable",
        "PosRef",
        "PosCierreRef", 
        "PosProv",
        "TextoEntreParentesis",
        "TextoEntreParenYProv",
        "TipoExtraido",
        "NroExtraido",
        "EsValido"
    })
in
    Resultado
