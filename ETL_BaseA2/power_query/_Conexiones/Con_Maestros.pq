let
    // =========================================================================
    // MÓDULO: Con_Maestros
    // =========================================================================
    // Objetivo: Centralizar la conexión al archivo maestro de equivalencias.
    // Si cambia la ubicación del Excel, SOLO se modifica este archivo.
    // =========================================================================

    Ruta = Param_RutaBase & "\data\sistema\Loockups.xlsx",
    Origen = Excel.Workbook(File.Contents(Ruta), null, true)
in
    Origen