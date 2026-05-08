let
    // ⚠️ NOTA: Esta query corre en BaseCostosPOSE.xlsx (archivo separado).
    // No tiene acceso a Param_RutaBase (que vive en BaseCostoUnificada.xlsx).
    // Al cambiar de máquina, actualizar esta ruta manualmente.
    // ⚠️ BaseCostoUnificada.xlsx no se versiona (147 MB). Vive en power_query/ local.
    Origen = Excel.Workbook(File.Contents("C:\Dev\POSE_ETL\power_query\BaseCostoUnificada.xlsx"), null, true),
    Archiv_Consolidado_Final_Table = Origen{[Item="Archiv_Consolidado_Final",Kind="Table"]}[Data],
    #"Tipo cambiado" = Table.TransformColumnTypes(Archiv_Consolidado_Final_Table,{{"Name", type text}, {"ID_UNICO", type text}, {"OBRA_PRONTO", type text}, {"FECHA", type date}, {"IMPORTE", type number}, {"TC", type number}, {"IMPORTE_USD", type number}, {"GERENCIA", type text}, {"FUENTE", type text}, {"PROVEEDOR", type text}, {"NRO_COMPROBANTE", type text}, {"TIPO_COMPROBANTE", type text}, {"DESCRIPCION_OBRA", type text}, {"DETALLE", type text}, {"OBSERVACION", type text}, {"RUBRO_CONTABLE", type text}, {"CUENTA_CONTABLE", type text}, {"CODIGO_CUENTA", Int64.Type}, {"COMPENSABLE", type text}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Tipo cambiado",{"Name", "ID_UNICO"})
in
    #"Columnas quitadas"