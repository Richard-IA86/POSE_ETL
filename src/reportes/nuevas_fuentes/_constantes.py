"""
_constantes.py — Constantes del pipeline nuevas fuentes FDL.

Fuentes procesadas a partir de la tabla 'gg_fdl' ({MM}-{YYYY}.xlsx),
distinguidas por la columna TIPO DE EROGACION:
  GG FDL          — filas con TIPO_EROGACION = "OBRA"
  FACTURACION FDL — filas con TIPO_EROGACION en
                    {"VENTA DEPTO", "VENTA LOTE", "VENTA PRODUCTO"}

Valores extraídos del PowerQuery ggfdf.pq (fuente autoritativa).
"""

from __future__ import annotations

from typing import Any

# ── Clasificación por TIPO DE EROGACION ──────────────────────────────────────
TIPOS_EROGACION_GG: list[str] = ["OBRA"]
TIPOS_EROGACION_FACTURACION: list[str] = [
    "VENTA DEPTO",
    "VENTA LOTE",
    "VENTA PRODUCTO",
]

# ── Constantes contables GG FDL (TIPO = "OBRA") ──────────────────────────────
CUENTAS_POR_FUENTE: dict[str, dict[str, Any]] = {
    "GG FDL": {
        "RUBRO_CONTABLE": "Gastos Generales",
        "CUENTA_CONTABLE": "GASTOS GENERALES",
        "CODIGO_CUENTA": 511121300,
    },
}

# ── Constantes FACTURACION FDL — dinámico por TIPO_EROGACION ─────────────────
FACTURACION_FDL_CUENTAS: dict[str, dict[str, Any]] = {
    "VENTA DEPTO": {
        "RUBRO_CONTABLE": "Ingresos Operativos",
        "CUENTA_CONTABLE": "VENTA DEPARTAMENTOS",
        "CODIGO_CUENTA": 410101012,
    },
    "VENTA LOTE": {
        "RUBRO_CONTABLE": "Ingresos Operativos",
        "CUENTA_CONTABLE": "VENTA LOTES",
        "CODIGO_CUENTA": 410101013,
    },
    "VENTA PRODUCTO": {
        "RUBRO_CONTABLE": "Ingresos Operativos",
        "CUENTA_CONTABLE": ("VENTA DE MATERIALES PARA LA CONSTRUCCIÓN"),
        "CODIGO_CUENTA": 410101010,
    },
}

# ── Columnas del staging (idénticas al schema compartido BaseCostosPOSE) ─────
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
    "CORRESPONDE*",
    "COMENTARIO*",
    "FUENTE*",
    "COMPENSABLE",
    "TC",
    "IMPORTE USD",
]

# ── Nombres de archivo Loockups ──────────────────────────────────────────────
LOOCKUPS_FILE = "Loockups.xlsx"
HOJA_OBRAS_GERENCIAS = "Obras_Gerencias"
HOJA_EQUIV_DESC_OBRAS = "Equivalencias_DescObras"
# Pestaña a mantener en Loockups.xlsx:
# mapea CENTRO DE COSTO → OBRA PRONTO para filas GG FDL sin N° OBRA.
# Columnas requeridas: CENTRO_COSTO | OBRA_PRONTO
HOJA_CENTRO_COSTO_OBRA = "GG_FDL_CentroCosto"

# ── Lookup fallback: filas sin OBRA_PRONTO ni CENTRO_COSTO (no-OBRA) ────────
OBRAS_PROVISIONALES: dict[str, dict[str, str]] = {
    "RETIRO": {
        "DESCRIPCION_OBRA": "RETIRO SIN OBRA ASIGNADA",
        "GERENCIA": "SIN OBRA ASIGNADA",
        "COMPENSABLE": "",
    },
    "APORTE": {
        "DESCRIPCION_OBRA": "APORTE SIN OBRA ASIGNADA",
        "GERENCIA": "SIN OBRA ASIGNADA",
        "COMPENSABLE": "",
    },
}

# ── Auxiliar: nombres de meses en español ────────────────────────────────────
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

# ── Mapa de correcciones de obra (normaliz. post-unidecode) ─────────────────
MAPA_CORRECCIONES_OBRA: dict[str, str] = {}
