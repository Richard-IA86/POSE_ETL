let
    // =========================================================================
    // CONSULTA: Archiv_Altas_Manuales
    // =========================================================================
    // Objetivo: Cargar registros manuales (RegPRONTO) con validación estricta y aislamiento.
    // =========================================================================

    // 0. Definición de Esquema (Mismas columnas que Crudo)
    // Se agregan TIPO_COMPROBANTE y NRO_COMPROBANTE para lectura directa
    // Si vienen vacíos, se intentará recuperar en capa Final
    ColumnasEsperadas = {
        "Name", "ID_UNICO", "FECHA", "OBRA_PRONTO", "IMPORTE", "PROVEEDOR", "OBSERVACION", 
        "RUBRO_CONTABLE", "CUENTA_CONTABLE", "DETALLE", "CODIGO_CUENTA", 
        "FUENTE", "ALERTA_HOJAS", "TIPO_COMPROBANTE", "NRO_COMPROBANTE"
    },

    // 1. Ruta del Archivo (Actualizado a Staging Normalizado) — Derivada de Param_RutaBase
    // Nueva arquitectura: consolidados en output\output_normalized\consolidados
    RutaRegPronto = Param_RutaBase & "\output\output_ready_for_pq\Modificaciones\BD_Modificaciones.xlsx",
    NombreArchivo = "BD_Modificaciones.xlsx",
    
    // 2. Leer y Normalizar
    // Usamos fn_LeerExcelNorm igual que en el proceso principal
    FuenteAltasRaw = try fn_LeerExcelNorm(File.Contents(RutaRegPronto)) otherwise null,

    // 3. Manejo de Errores y Tablas Vacías
    // Si falla la lectura o devuelve null, creamos una tabla vacía CON ESTRUCTURA
    TablaBase = if FuenteAltasRaw = null then 
             #table(List.RemoveItems(ColumnasEsperadas, {"Name", "ID_UNICO"}), {})
         else 
             FuenteAltasRaw,

    // 4. Estandarización de Estructura
    // Asegurar que exista ALERTA_HOJAS (puede no venir de fn_LeerExcelNorm si solo hay 1 hoja)
    ConAlerta = if Table.HasColumns(TablaBase, "ALERTA_HOJAS") then 
            TablaBase 
        else 
            Table.AddColumn(TablaBase, "ALERTA_HOJAS", each null, type text),

    // 5. Generación de ID Único
    ConIndice = if Table.IsEmpty(ConAlerta) then 
            ConAlerta 
        else 
            Table.AddIndexColumn(ConAlerta, "IdxManual", 1, 1, Int64.Type),
            
    ConID = if Table.HasColumns(ConIndice, "IdxManual") then
            Table.AddColumn(ConIndice, "ID_UNICO", each "MANUAL-" & Text.From([IdxManual]), type text)
        else
            Table.AddColumn(ConIndice, "ID_UNICO", each null, type text),
    
    // 5.5 Agregar columna Name (nombre del archivo de origen)
    ConName = Table.AddColumn(ConID, "Name", each NombreArchivo, type text),

    // 6. VALIDACIÓN Y LIMPIEZA (Clave: Nivelar con Crudo)
    // A. Filtrar nulos estructurales
    // Se ajusta para coincidir con la validación de Archiv_Consolidado_Crudo
    FilasValidas = Table.SelectRows(ConName, each [FECHA] <> null and [OBRA_PRONTO] <> null and [FUENTE] <> null),
    
    // B. Seleccion Final de Columnas
    // Garantiza que el Append posterior no falle por columnas extrañas
   ResultadoFinal = Table.SelectColumns(FilasValidas, ColumnasEsperadas, MissingField.UseNull),
    
    // 7. BUFFER para optimización de rendimiento
    // Esta consulta se referencia en Modif, Control_Auditoria, Analisis_Exclusiones, etc.
    Buffered = Table.Buffer(ResultadoFinal)
in
    Buffered