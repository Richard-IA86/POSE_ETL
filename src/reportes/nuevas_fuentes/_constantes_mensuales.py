"""
_constantes_mensuales.py — Constantes contables para la fuente MENSUALES.

Valores confirmados por QA spec (qa_ingesta_nuevas_fuentes.md §7.1):
  RUBRO CONTABLE*  = "Sueldos, Jornales y Cargas Sociales"
  CUENTA CONTABLE* = "SUELDOS Y JORNALES"
  CODIGO CUENTA*   = 511200002
  FUENTE*          = "MENSUALES"
"""

from __future__ import annotations

from typing import Any

CUENTA_MENSUALES: dict[str, Any] = {
    "RUBRO_CONTABLE": "Sueldos, Jornales y Cargas Sociales",
    "CUENTA_CONTABLE": "SUELDOS Y JORNALES",
    "CODIGO_CUENTA": 511200002,
}
