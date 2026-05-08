let
    // =========================================================================
    // DIMENSIÓN: CALENDARIO Y TIPO DE CAMBIO
    // =========================================================================
    // Objetivo: Proveer TC continuo para cualquier fecha (Fill-Down).
    // =========================================================================

    // 1. Conexión
    Origen = Con_Maestros,
    Data = Origen{[Item="Loockup_TipoCambio",Kind="Table"]}[Data],

    // 2. NORMALIZACIÓN DE ENCABEZADOS (Mayúsculas)
    HeadersLimpios = Table.TransformColumnNames(Data, each Text.Upper(Text.Trim(_))),

    // 3. Selección y Tipos (Usando nombres normalizados: FECHA, TC VENDEDOR)
    Seleccion = Table.SelectColumns(HeadersLimpios, {"FECHA", "TC VENDEDOR"}),
    Tipos = Table.TransformColumnTypes(Seleccion, {{"FECHA", type date}, {"TC VENDEDOR", type number}}),

    // 4. Agrupar por Fecha (Max TC ante duplicados)
    Dedup = Table.Group(Tipos, {"FECHA"}, {{"TC_Real", each List.Max([TC VENDEDOR]), type nullable number}}),

    // 5. Generar Calendario Continuo (Min a Max)
    FechaMin = List.Min(Dedup[FECHA]),
    FechaMax = List.Max(Dedup[FECHA]),
    Dias = Duration.Days(FechaMax - FechaMin) + 1,
    
    ListaFechas = List.Dates(FechaMin, Dias, #duration(1, 0, 0, 0)),
    TablaCalendario = Table.FromList(ListaFechas, Splitter.SplitByNothing(), {"FECHA"}, null, ExtraValues.Error),
    TipoCalendario = Table.TransformColumnTypes(TablaCalendario, {{"FECHA", type date}}),

    // 6. Join con Datos Reales
    Join = Table.NestedJoin(TipoCalendario, {"FECHA"}, Dedup, {"FECHA"}, "Datos", JoinKind.LeftOuter),
    Expand = Table.ExpandTableColumn(Join, "Datos", {"TC_Real"}, {"TC_Real"}),

    // 7. FILL-DOWN (Rellenar huecos con el valor anterior)
    Ordenado = Table.Sort(Expand, {{"FECHA", Order.Ascending}}),
    Relleno = Table.FillDown(Ordenado, {"TC_Real"}),

    // 7. Salida Final
    Renombrado = Table.RenameColumns(Relleno, {{"TC_Real", "TC"}})
in
    Renombrado