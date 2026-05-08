let
    // =========================================================================
    // CONSULTA: Archiv_Consolidado_Modif (Capa 2 - Gestión)
    // =========================================================================
    // Objetivo: Unir Crudo + Altas Manuales - Bajas.
    // NOTA: Para evitar error 'Formula.Firewall', las fuentes deben venir
    // de consultas separadas (Archiv_Consolidado_Crudo y Archiv_Altas_Manuales).
    // =========================================================================

    // 1. FUENTES (Referencias a consultas ya existentes)
    FuenteCruda = Archiv_Consolidado_Crudo,
    FuenteAltas = Archiv_Altas_Manuales,
    
    // 1.5 LIMPIEZA PREVIA (Exclusión Reglas de Negocio)
    // Eliminamos registros de origen "PRONTO" del año 2025 presentes en el Crudo
    // para evitar duplicarlos con la carga manual (Archiv_Altas_Manuales).
    FuenteCrudaLimpia = Table.SelectRows(FuenteCruda, each not (Date.Year([FECHA]) = 2025 and [FUENTE] = "PRONTO")),

    // 2. UNION (APPEND)
    // Unificar verticalmente Crudo (Limpio) + Altas
    Union = Table.Combine({FuenteCrudaLimpia, FuenteAltas}),
    
    // 3. GESTIÓN DE BAJAS (Opcional - Estructura preparada)
    // Si hubiera una tabla "Bajas.xlsx" con IDs a eliminar:
    // TablaBajas = ...
    // AntiJoin = Table.NestedJoin(Union, {"ID_UNICO"}, TablaBajas, {"ID_UNICO_BAJA"}, "Bajas", JoinKind.LeftAnti),
    
    // Por ahora pasamos directo la unión
    Resultado = Union,
    
    // 4. BUFFER CRÍTICO para optimización
    // Esta consulta se referencia en MÚLTIPLES lugares (Final, Control_Auditoria, Analisis_Exclusiones, etc.)
    // Buffer evita re-ejecución completa de Crudo+Altas cada vez (MAYOR IMPACTO en rendimiento)
    Buffered = Table.Buffer(Resultado)
in
    Buffered