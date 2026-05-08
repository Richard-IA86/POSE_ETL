let
    // =========================================================================
    // FUNCIÓN: fn_LeerExcelNorm
    // =========================================================================
    // Objetivo: Leer archivo Excel, promover encabezados y normalizar columnas
    // usando sinónimos conocidos. Implementa patrón 'Thin Fact'.
    // =========================================================================
    
    func = (contenidoBinario as binary) as table =>
    let
        // 1. Abrir Excel
        Libro = Excel.Workbook(contenidoBinario, null, true),
        
        // 2. ESTRATEGIA: SELECCIÓN DE HOJA POR PRIORIDAD (CON ALERTA)
        // Seleccionamos solo las hojas visibles
        HojasVisibles = Table.SelectRows(Libro, each [Kind] = "Sheet" and [Hidden] = false),
        
        // Detección de múltiples hojas (para aviso)
        CantidadHojas = Table.RowCount(HojasVisibles),
        NombresHojas = Text.Combine(HojasVisibles[Name], " | "),
        TieneMultiples = CantidadHojas > 1,

        // Detección de formato 2026: hoja "Informe Mensual" (case insensitive)
        NombresUpper = List.Transform(HojasVisibles[Name], each Text.Upper(Text.Trim(_))),
        IdxInformeMensual = List.PositionOf(NombresUpper, "INFORME MENSUAL"),
        EsFormato2026 = IdxInformeMensual >= 0,

        // Selección de hoja: "Informe Mensual" si existe, sino primera visible
        HojaSeleccionada = if EsFormato2026
            then HojasVisibles{IdxInformeMensual}[Data]
            else HojasVisibles{0}[Data],
        
        // 3. PROCESAMIENTO (Normalización y Limpieza)
        // A. Promover Encabezados
        EncabezadosRaw = Table.PromoteHeaders(HojaSeleccionada, [PromoteAllScalars=true]),
        
        // B. Normalizar Nombres (Mayúsculas y Trim)
        Encabezados = Table.TransformColumnNames(EncabezadosRaw, each Text.Upper(Text.Trim(_))),

        // B.1. Formato 2026: eliminar columna MES (no forma parte del esquema Crudo)
        EncabezadosSinMes = if EsFormato2026
            then Table.RemoveColumns(Encabezados, {"MES"}, MissingField.Ignore)
            else Encabezados,

        // C. Diccionario de Sinónimos
        MapaSinonimos = {
            // Variaciones con asterisco (comunes en 2025 Originales) y Limpias (Post-Python)
            {"FECHA*", "FECHA"}, {"IMPORTE*", "IMPORTE"}, {"PROVEEDOR*", "PROVEEDOR"}, 
            {"N° COMPROBANTE*", "NRO_COMPROBANTE"}, {"TIPO COMPROBANTE*", "TIPO_COMPROBANTE"}, 
            {"TIPO COMPROBANTE", "TIPO_COMPROBANTE"}, // Post-Python sin *
            {"OBSERVACION*", "OBSERVACION"}, {"DETALLE*", "DETALLE"}, 
            {"OBRA PRONTO*", "OBRA_PRONTO"}, {"OBRA_PRONTO*", "OBRA_PRONTO"},
            
            {"RUBRO CONTABLE*", "RUBRO_CONTABLE"}, {"RUBRO CONTABLE", "RUBRO_CONTABLE"},
            {"CUENTA CONTABLE*", "CUENTA_CONTABLE"}, {"CUENTA CONTABLE", "CUENTA_CONTABLE"},
            {"CODIGO CUENTA*", "CODIGO_CUENTA"}, {"CODIGO CUENTA", "CODIGO_CUENTA"},
            
            {"FUENTE*", "FUENTE"}, {"costos", "FUENTE"}, {"COSTOS", "FUENTE"},
            {"CORRESPONDE*", "CORRESPONDE"}, {"COMENTARIO*", "COMENTARIO"},
            
            // Variaciones Específicas 2025
            {"DESCRIPCION OBRA", "DESCRIPCION_OBRA"},

            // Variaciones históricas
            {"OBRA PRONTO", "OBRA_PRONTO"}, {"CODIGO OBRA", "OBRA_PRONTO"}, {"COD. OBRA", "OBRA_PRONTO"},
            {"CENTRO DE COSTO", "OBRA_PRONTO"}, {"CENTRO_COSTO", "OBRA_PRONTO"}, {"OBRA", "OBRA_PRONTO"},
            {"DETALLE CENTRO DE COSTO", "DESCRIPCION_OBRA"},
            {"FECHA COMPROBANTE", "FECHA"}, {"FEC. EMISION", "FECHA"}, {"PERIODO", "FECHA"},
            {"IMPORTE NETO", "IMPORTE"}, {"MONTO", "IMPORTE"}, {"TOTAL", "IMPORTE"},
            {"NRO COMPROBANTE", "NRO_COMPROBANTE"}, {"NUMERO", "NRO_COMPROBANTE"}, {"N° COMPROBANTE", "NRO_COMPROBANTE"},
            {"TIPO COMP", "TIPO_COMPROBANTE"}, {"TIPO", "TIPO_COMPROBANTE"},
            {"PROVEEDOR RAZON SOCIAL", "PROVEEDOR"}, {"RAZON SOCIAL", "PROVEEDOR"}, {"PROVEEDORES", "PROVEEDOR"},
            {"COSTO", "FUENTE"}, {"costos", "FUENTE"},
            {"CORRESPONDE", "CORRESPONDE"}, {"COMENTARIO", "COMENTARIO"}
        },
        
        RenombrarSiExiste = (tabla, old, new) => 
            if Table.HasColumns(tabla, old) then Table.RenameColumns(tabla, {{old, new}}) else tabla,
            
        TablaNormalizada = List.Accumulate(
            MapaSinonimos, 
            EncabezadosSinMes, 
            (estado, actual) => RenombrarSiExiste(estado, actual{0}, actual{1})
        ),
        
        // C.1 PRIORIZAR IMPORTE 2 (Si existe, reemplaza IMPORTE)
        // Contexto: Archivos de COSTO GERENCIAS tienen doble entrada (IMPORTE + IMPORTE 2)
        // IMPORTE 2 es el valor correcto (débito/crédito según corresponda)
        TablaConImporteCorregido = if Table.HasColumns(TablaNormalizada, "IMPORTE 2") then
            let
                // Eliminar IMPORTE si existe
                SinImporteViejo = if Table.HasColumns(TablaNormalizada, "IMPORTE") then
                    Table.RemoveColumns(TablaNormalizada, {"IMPORTE"})
                else
                    TablaNormalizada,
                // Renombrar IMPORTE 2 → IMPORTE
                ConImporteNuevo = Table.RenameColumns(SinImporteViejo, {{"IMPORTE 2", "IMPORTE"}})
            in
                ConImporteNuevo
        else
            TablaNormalizada,

        // D. LIMPIEZA FINAL: WHITELISTING (Solo columnas permitidas)
        // -------------------------------------------------------------
        // Definir qué columnas son legítimas para el modelo unificado.
        // Todo lo demás (columnas basura, cálculos intermedios del excel, etc.) se descarta aquí.
        ColumnasPermitidas = {
            "FECHA", "OBRA_PRONTO", "IMPORTE", "PROVEEDOR", 
            "DETALLE", "FUENTE", 
            "OBSERVACION", "RUBRO_CONTABLE", "CUENTA_CONTABLE", "CODIGO_CUENTA",
            "TIPO_COMPROBANTE", "NRO_COMPROBANTE",
            "CORRESPONDE", "COMENTARIO"
        },
        
        // Seleccionamos SOLO las columnas que existan en la lista permitida.
        // MissingField.UseNull asegura que si falta alguna columna (ej: TIPO_COMPROBANTE), se cree vacía.
        TablaLimpia = Table.SelectColumns(TablaConImporteCorregido, ColumnasPermitidas, MissingField.UseNull),

        // E. NORMALIZACIÓN DE FECHAS (Múltiples formatos)
        // -------------------------------------------------------------
        // Contexto: Archivos históricos tienen fechas en formatos mixtos:
        //   - "04/07/2023 " (dd/mm/yyyy con espacios)
        //   - "03-jul-2023" (dd-mmm-yyyy con nombres de mes)
        //   - "31-07-2023 " (dd-mm-yyyy con guiones)
        // Solución: Custom column que intenta múltiples parseos
        TablaConFechas = if Table.HasColumns(TablaLimpia, "FECHA") then
            let
                // Función helper para convertir fechas con múltiples intentos
                ConvertirFecha = (valor) =>
                    let
                        // Si ya es date, retornar
                        resultado = if Value.Is(valor, type date) then valor
                        else
                            let
                                textoLimpio = Text.Trim(Text.From(valor)),
                                
                                // Intento 1: dd/mm/yyyy (formato español estándar)
                                intento1 = try Date.From(textoLimpio, "es-ES") otherwise null,
                                
                                // Intento 2: dd-mmm-yyyy con nombres de mes (ej: "03-jul-2023")
                                // Power Query detecta automáticamente este formato con cultura
                                intento2 = if intento1 = null then 
                                    try Date.From(textoLimpio) otherwise null
                                else intento1,
                                
                                // Intento 3: Convertir manualmente dd-mm-yyyy
                                // Extraer partes si tiene formato XX-XX-XXXX
                                intento3 = if intento2 = null and Text.Contains(textoLimpio, "-") then
                                    try
                                        let
                                            partes = Text.Split(textoLimpio, "-"),
                                            dia = Number.From(partes{0}),
                                            mes = Number.From(partes{1}),
                                            anio = Number.From(partes{2}),
                                            fecha = #date(anio, mes, dia)
                                        in
                                            fecha
                                    otherwise null
                                else intento2
                            in
                                intento3
                    in
                        resultado,
                
                // Aplicar transformación
                FechaTransformada = Table.TransformColumns(
                    TablaLimpia,
                    {{"FECHA", each ConvertirFecha(_), type date}}
                )
            in
                FechaTransformada
        else
            TablaLimpia,

        // F. INYECCIÓN DE ALERTA (Si corresponde)
        TablaConAviso = if TieneMultiples then
            Table.AddColumn(TablaConFechas, "ALERTA_HOJAS", each "Multiples hojas (" & Text.From(CantidadHojas) & "): " & NombresHojas)
        else
            TablaConFechas

    in
        TablaConAviso
in
    func