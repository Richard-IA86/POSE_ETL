"""
_mapeo.py — Catálogo de mapeo de columnas Excel → BD para report_direccion (DESPACHOS).  # noqa: E501

NOMBRE_HOJA                  : str   nombre exacto de la hoja en el Excel
FILA_HEADERS                 : int   índice 0-based de la fila con encabezados reales  # noqa: E501
MAPEO_COLUMNAS               : dict  columna_excel → nombre_bd  (usada por reader.py)  # noqa: E501
COLUMNAS_OBLIGATORIAS        : list  nombres_bd para validación del Inspector
COLS_STAGING                 : list  columnas del CSV de staging (orden BASE_TOTAL)  # noqa: E501
COLUMNAS_OBLIGATORIAS_STAGING: list  nombres staging para validación del Validador  # noqa: E501
CONSTANTES_STAGING           : dict  valores fijos de esta fuente (REMITO, CAC, etc.)  # noqa: E501
DUP_COLS                     : list  clave de duplicados para segregar pendientes  # noqa: E501
MESES_ES                     : dict  mes número → nombre en español

Mantener sincronizado con ddl_despachos.sql y bd_loader_despachos.py (_MAP_VALIDADOS).  # noqa: E501
"""

# ── Estructura del Excel crudo ────────────────────────────────────────────────  # noqa: E501

# Hoja del Excel que contiene los despachos (tiene espacio trailing)
NOMBRE_HOJA: str = "DESPACHOS "

# Fila 0 es basura (total acumulado); los encabezados reales están en la fila 1
FILA_HEADERS: int = 1

# ── Archivos de lookup (relativos a input_raw/ del informe) ──────────────────

LOOCKUPS_FILE = "Loockups.xlsx"  # único archivo de lookups canónico
HOJA_OBRAS_GERENCIAS = "Obras_Gerencias"  # hoja dentro de LOOCKUPS_FILE
HOJA_GERENCIA_EQUIV = "GerenciEquivalente"  # hoja en LOOCKUPS_FILE
HOJA_TIPO_CAMBIO = "TipoCambio"
HOJA_EXCEPCIONES_GERENCIA = "Excepciones_Gerencia"

# ── Mapeo Excel crudo → nombre BD (usado solo por reader.py / InspectorStage) ─  # noqa: E501

MAPEO_COLUMNAS: dict[str, str] = {
    "FECHA": "fecha",
    "NV": "nta_vta",
    "FABRICA/CORRALON": "proveedor",
    "NRO_RTO": "nro_remito",
    "NRO CL": "cod_cli",
    "DESC_CLIEN": "desc_cliente",
    "N° OBRA": "obra_pronto",
    "COD": "cod_producto",
    "DESC_PROD": "desc_producto",
    "UNI": "unidad",
    "CANT_DESP": "cant_despachada",
    "$_DESPACH": "importe",
    "FECHA_DESP": "fecha_despacho",
    "FACTURA": "nro_factura",
}

# Columnas BD mínimas para que el Inspector declare calidad_ok=True
COLUMNAS_OBLIGATORIAS: list[str] = [
    "fecha",
    "obra_pronto",
    "importe",
    "nro_remito",
]

# ── Columnas del CSV de staging (orden y nombres BASE_TOTAL) ─────────────────

COLS_STAGING: list[str] = [
    "MES",
    "FECHA*",
    "OBRA PRONTO*",
    "DESCRIPCION OBRA",
    "GERENCIA",
    "DETALLE*",
    "IMPORTE*",
    "TIPO COMPROBANTE*",
    "N° COMPROBANTE*",
    "OBSERVACION*",
    "PROVEEDOR*",
    "RUBRO CONTABLE*",
    "CUENTA CONTABLE*",
    "CODIGO CUENTA*",
    "FUENTE*",
    "COMPENSABLE",
    "TC",
    "IMPORTE USD",
]

# Columnas staging que deben estar presentes y sin nulos para aceptar el staging  # noqa: E501
COLUMNAS_OBLIGATORIAS_STAGING: list[str] = [
    "FECHA*",
    "OBRA PRONTO*",
    "IMPORTE*",
    "TIPO COMPROBANTE*",
]

# ── Constantes de fuente DESPACHOS ───────────────────────────────────────────
# Valores fijos que esta fuente siempre aporta; otras fuentes tendrán los suyos.  # noqa: E501

CONSTANTES_STAGING: dict[str, str] = {
    "TIPO COMPROBANTE*": "REMITO",
    "RUBRO CONTABLE*": "Materiales",
    "CUENTA CONTABLE*": "MATERIALES",
    "CODIGO CUENTA*": "511700002",
    "FUENTE*": "CAC",
}

# ── Detección de duplicados ───────────────────────────────────────────────────  # noqa: E501

# Clave de duplicados sobre el crudo (columnas originales del Excel)
DUP_COLS: list[str] = [
    "FACTURA",
    "N° OBRA",
    "COD",
    "$_DESPACH",
    "FECHA_DESP",
    "NV",
]

# ── Auxiliar de fechas ────────────────────────────────────────────────────────  # noqa: E501

MESES_ES: dict[int, str] = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}
