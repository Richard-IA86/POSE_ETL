"""
transformer_quincenas.py — Procesa DataFrames crudos de QUINCENAS.

Implementa:
1. Resiliencia de columnas (ignora extra como Aguinaldo si no se mapean).
2. Normalización pesada de textos.
3. Exportación de deuda de mapeo (Opción B: fallo estricto).
4. Desdoblamiento en ramas SUELDOS y CARGAS.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
from unidecode import unidecode

from ._constantes import (
    COLS_STAGING,
    HOJA_EQUIV_DESC_OBRAS,
    MAPA_CORRECCIONES_OBRA,
    MESES_ES,
)
from .transformer_fdl import _leer_obras_gerencias

logger = logging.getLogger(__name__)

_REPORT_DIR = Path(__file__).parents[2]
_COMMON_DIR = _REPORT_DIR / "report_gerencias" / "input_raw" / "common"
_PENDIENTES_MAPEO_CSV = _COMMON_DIR / "Obras_Pendientes_Mapeo.csv"

# Columnas que sí o sí deben venir del reader_quincenas
COLUMNAS_MINIMAS = [
    "LEGAJO Nº",
    "APELLIDO",
    "NOMBRE",
    "CATEGORIA",
    "TAREA",
    "OBRA",
    "FECHA",
    "QUINCENA",
    "SUBTOTAL SUELDO",
    "SUBTOTAL IMPUESTOS",
]


def _normalizar_texto(texto: str | None) -> str:
    """Normalización pesada de descripciones de obra."""
    if pd.isna(texto) or not texto:
        return ""
    t = unidecode(str(texto)).upper()
    t = t.replace("\n", " ").replace("\r", " ").replace("|", " ")
    t = re.sub(r"\s+", " ", t)
    t = t.strip()
    return MAPA_CORRECCIONES_OBRA.get(t, t)


def _reportar_pendientes_mapeo(
    df_sin_match: pd.DataFrame,
    nombre_archivo: str,
) -> None:
    """Guarda CSV de obras sin mapeo (Opción B) y lanza ValueError."""
    _COMMON_DIR.mkdir(parents=True, exist_ok=True)
    obras_unicas = df_sin_match.drop_duplicates(subset=["OBRA_LIMPIA"])
    df_report = pd.DataFrame(
        {
            "descripcion": obras_unicas["OBRA"].values,
            "nombre_archivo": nombre_archivo,
            "origen": "QUINCENAS",
        }
    )
    df_report.to_csv(_PENDIENTES_MAPEO_CSV, index=False, sep=";")
    mensaje = (
        f"Fallo B: {len(df_report)} obras sin match en "
        f"Loockups.xlsx. Exportado a {_PENDIENTES_MAPEO_CSV.name}."
    )
    logger.error(mensaje)
    raise ValueError(mensaje)


def _limpiar_reporte_pendientes() -> None:
    """Pisa el CSV con tabla vacía tras corrida exitosa."""
    _COMMON_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"descripcion": [], "nombre_archivo": [], "origen": []}
    ).to_csv(_PENDIENTES_MAPEO_CSV, index=False, sep=";")
    logger.info(
        "QUINCENAS: sin obras pendientes de mapeo (%s)",
        _PENDIENTES_MAPEO_CSV.name,
    )


def _expandir_lookups_con_equivalencias(
    loockups_path: Path,
    df_lookups: pd.DataFrame,
) -> pd.DataFrame:
    """Segunda pasada: expande df_lookups con alias de
    Equivalencias_DescObras.

    Para cada fila (ORIGEN, DESTINO) de la hoja de equivalencias,
    busca la entrada maestra en df_lookups (donde OBRA_LIMPIA ==
    DESTINO normalizado) y agrega una fila extra con OBRA_LIMPIA ==
    ORIGEN normalizado pero con los mismos OBRA_PRONTO/GERENCIA/
    COMPENSABLE.
    """
    try:
        df_equiv = pd.read_excel(
            loockups_path,
            sheet_name=HOJA_EQUIV_DESC_OBRAS,
            usecols=[
                "DESCRIPCION_OBRA_ORIGEN",
                "DESCRIPCION_OBRA_DESTINO",
            ],
        )
    except Exception as exc:  # hoja no existe aún: tolerar
        logger.warning(
            "No se pudo leer %s: %s",
            HOJA_EQUIV_DESC_OBRAS,
            exc,
        )
        return df_lookups

    df_equiv = df_equiv.dropna(
        subset=["DESCRIPCION_OBRA_ORIGEN", "DESCRIPCION_OBRA_DESTINO"]
    )
    if df_equiv.empty:
        return df_lookups

    # Descartar filas con destino vacío o "0" (basura Excel)
    df_equiv = df_equiv[
        ~df_equiv["DESCRIPCION_OBRA_DESTINO"]
        .astype(str)
        .str.strip()
        .str.upper()
        .isin({"", "0", "NAN"})
    ]
    if df_equiv.empty:
        return df_lookups

    df_equiv["ORIGEN_LIMPIA"] = df_equiv["DESCRIPCION_OBRA_ORIGEN"].apply(
        _normalizar_texto
    )
    df_equiv["DESTINO_LIMPIA"] = df_equiv["DESCRIPCION_OBRA_DESTINO"].apply(
        _normalizar_texto
    )

    filas_alias: list[dict] = []
    for _, fila_eq in df_equiv.iterrows():
        maestras = df_lookups[
            df_lookups["OBRA_LIMPIA"] == fila_eq["DESTINO_LIMPIA"]
        ]
        if maestras.empty:
            logger.warning(
                "Equivalencia sin destino en Obras_Gerencias: '%s'",
                fila_eq["DESCRIPCION_OBRA_DESTINO"],
            )
            continue
        for _, maestra in maestras.iterrows():
            filas_alias.append(
                {
                    "OBRA_LIMPIA": fila_eq["ORIGEN_LIMPIA"],
                    "OBRA_PRONTO": maestra["OBRA_PRONTO"],
                    "DESCRIPCION_OBRA": fila_eq["DESCRIPCION_OBRA_ORIGEN"],
                    "GERENCIA": maestra["GERENCIA"],
                    "COMPENSABLE": maestra["COMPENSABLE"],
                }
            )

    if not filas_alias:
        return df_lookups

    df_alias = pd.DataFrame(filas_alias)
    logger.info(
        "Equivalencias_DescObras: %d alias incorporados al lookup.",
        len(df_alias),
    )
    return pd.concat([df_lookups, df_alias], ignore_index=True)


def _crear_detalle(row: pd.Series) -> str:
    """Construye la cadena de DETALLE."""
    try:
        fecha_str = row["FECHA"].strftime("%d/%m/%Y")
    except Exception:
        fecha_str = str(row["FECHA"])

    leg = int(row["LEGAJO Nº"]) if pd.notna(row["LEGAJO Nº"]) else 0
    return (
        f"{fecha_str} - QUINCENA {row['QUINCENA']} - "
        f"LEGAJO {leg} - {row['APELLIDO']}{row['NOMBRE']} - "
        f"CAT {row['CATEGORIA']} - TAREA {row['TAREA']}"
    )


def transformar_quincenas(
    df_crudo: pd.DataFrame,
    loockups_path: Path,
    nombre_archivo: str = "",
) -> pd.DataFrame:
    """Implementa la lógica del Spec 8 para QUINCENAS."""
    if df_crudo.empty:
        return pd.DataFrame(columns=COLS_STAGING)

    # 1. VALIDADOR DEFENSIVO: Revisar que existan las columnas clave
    faltantes = [c for c in COLUMNAS_MINIMAS if c not in df_crudo.columns]
    if faltantes:
        raise ValueError(
            f"El crudo QUINCENAS no tiene estas columnas: {faltantes}"
        )

    # 2. SELECCIÓN ESTRICTA: Aislar solo lo que necesitamos.
    # Ignorar columnas extra (Aguinaldos/Bonus).
    df = df_crudo[COLUMNAS_MINIMAS].copy()

    # 3. PRE-INGESTA NORMALIZADORA
    df["OBRA_LIMPIA"] = df["OBRA"].apply(_normalizar_texto)

    # 3b. DEPURACIÓN: descartar filas sin legajo numérico o sin obra
    df = df[pd.to_numeric(df["LEGAJO Nº"], errors="coerce").notna()].copy()
    df = df[df["OBRA_LIMPIA"].str.len() > 0].copy()

    # 4. LEER Y VALIDAR CONTRA LOOKUPS (Aduana)
    # Pasada 1: Obras_Gerencias
    df_lookups = _leer_obras_gerencias(loockups_path)
    df_lookups["OBRA_LIMPIA"] = df_lookups["DESCRIPCION_OBRA"].apply(
        _normalizar_texto
    )
    # Pasada 2: Equivalencias_DescObras (expande con alias)
    df_lookups = _expandir_lookups_con_equivalencias(loockups_path, df_lookups)

    # Deduplicar lookup: si hay colisión de OBRA_LIMPIA, conservar la
    # entrada maestra (primera aparición, que proviene de Obras_Gerencias).
    n_antes = len(df_lookups)
    df_lookups = df_lookups.drop_duplicates(
        subset=["OBRA_LIMPIA"], keep="first"
    ).reset_index(drop=True)
    n_dedup = n_antes - len(df_lookups)
    if n_dedup:
        logger.info(
            "Lookup deduplicado: %d entradas eliminadas "
            "(OBRA_LIMPIA repetida).",
            n_dedup,
        )

    # Transformación general (aplica a SUELDOS y CARGAS)
    # Fecha: si Q=2, pisar el día al 15 (el crudo trae día 1 siempre)
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
    m_q2 = df["QUINCENA"] == 2
    df.loc[m_q2, "FECHA"] = df.loc[m_q2, "FECHA"].apply(
        lambda x: x.replace(day=15) if pd.notna(x) else x
    )

    df["DETALLE*"] = df.apply(_crear_detalle, axis=1)
    df["FECHA*"] = df["FECHA"].apply(
        lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None
    )
    df["MES"] = df["FECHA"].apply(
        lambda x: MESES_ES.get(x.month, "") if pd.notna(x) else ""
    )

    # Convertir a numérico (evita textos raros)
    df["SUBTOTAL SUELDO"] = pd.to_numeric(
        df["SUBTOTAL SUELDO"], errors="coerce"
    ).fillna(0)
    df["SUBTOTAL IMPUESTOS"] = pd.to_numeric(
        df["SUBTOTAL IMPUESTOS"], errors="coerce"
    ).fillna(0)

    # Join para traer datos maestros
    df = df.merge(
        df_lookups[["OBRA_LIMPIA", "OBRA_PRONTO", "GERENCIA", "COMPENSABLE"]],
        on="OBRA_LIMPIA",
        how="left",
    )

    # 5. VALIDACIÓN POST-MERGE: obras sin mapeo → Opción B
    df_sin_match = df[df["OBRA_PRONTO"].isna()]
    if not df_sin_match.empty:
        _reportar_pendientes_mapeo(df_sin_match, nombre_archivo)
    _limpiar_reporte_pendientes()

    # Creación rama SUELDOS
    df_sueldos = df.copy()
    df_sueldos["IMPORTE*"] = df_sueldos["SUBTOTAL SUELDO"] * -1
    df_sueldos["CUENTA CONTABLE*"] = "SUELDOS Y JORNALES"
    df_sueldos["CODIGO CUENTA*"] = 511200002

    # Creación rama CARGAS
    df_cargas = df.copy()
    df_cargas["IMPORTE*"] = df_cargas["SUBTOTAL IMPUESTOS"] * -1
    df_cargas["CUENTA CONTABLE*"] = "CONTRIBUCIONES Y CARGAS SOCIALES"
    df_cargas["CODIGO CUENTA*"] = 511200003

    # Unificar
    df_final = pd.concat([df_sueldos, df_cargas], ignore_index=True)

    # Filtrar sin ceros (Spec paso 8.2)
    df_final = df_final[df_final["IMPORTE*"] != 0].copy()

    # Mapeo de constantes con nombres COLS_STAGING
    df_final["TC"] = None
    df_final["IMPORTE USD"] = None
    df_final["FUENTE*"] = "QUINCENAS"
    df_final["PROVEEDOR*"] = "POSE"
    df_final["N° COMPROBANTE*"] = "-"
    df_final["TIPO COMPROBANTE*"] = "RECIBO"
    df_final["OBSERVACION*"] = "-"
    df_final["RUBRO CONTABLE*"] = "Sueldos, Jornales y Cargas Sociales"
    df_final["CORRESPONDE*"] = None
    df_final["COMENTARIO*"] = None
    df_final["DESCRIPCION OBRA"] = df_final["OBRA_LIMPIA"]
    df_final["OBRA PRONTO*"] = df_final["OBRA_PRONTO"]

    # Devolver staging
    return df_final[COLS_STAGING]
