"""
bd_loader_despachos.py — Carga del pipeline DESPACHOS a SQL Express.

Responsabilidades:
  - cargar_validados()   → DELETE por periodo + INSERT en despachos_validados
  - cargar_rechazados()  → INSERT en despachos_rechazados (acumulativo, sin DELETE)  # noqa: E501
  - resolver_rechazado() → UPDATE estado + migración opcional a validados

Todas las operaciones son transaccionales con rollback automático.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyodbc

from src.reportes.loader._conexion import (
    get_connection_string,
)

# ── Tablas destino ─────────────────────────────────────────────────────────────  # noqa: E501
_SCHEMA = "PRODUCCION"
_TBL_VALIDADOS = f"{_SCHEMA}.despachos_validados"
_TBL_RECHAZADOS = f"{_SCHEMA}.despachos_rechazados"

# ── Mapeo: columna CSV (BASE_TOTAL) → columna SQL ─────────────────────────────  # noqa: E501
# Las claves son los encabezados exactos del CSV de staging.
_MAP_VALIDADOS: dict[str, str] = {
    "MES": "mes",
    "FECHA*": "fecha",
    "OBRA PRONTO*": "obra_pronto",
    "DESCRIPCION OBRA": "descripcion_obra",
    "GERENCIA": "gerencia",
    "DETALLE*": "detalle",
    "IMPORTE*": "importe",
    "TIPO COMPROBANTE*": "tipo_comprobante",
    "N° COMPROBANTE*": "nro_comprobante",
    "OBSERVACION*": "observacion",
    "PROVEEDOR*": "proveedor",
    "RUBRO CONTABLE*": "rubro_contable",
    "CUENTA CONTABLE*": "cuenta_contable",
    "CODIGO CUENTA*": "codigo_cuenta",
    "FUENTE*": "fuente",
    "COMPENSABLE": "compensable",
    "TC": "tc",
    "IMPORTE USD": "importe_usd",
}

# Columnas del CSV que se cargan a rechazados (idénticas al validados)
_MAP_RECHAZADOS = _MAP_VALIDADOS.copy()

# Columnas SQL de la tabla validados (sin auditoría — la BD las autocompleta)
_COLS_VALIDADOS = list(_MAP_VALIDADOS.values()) + [
    "periodo",
    "run_id",
    "hash_archivo",
]
_COLS_RECHAZADOS = list(_MAP_RECHAZADOS.values()) + [
    "periodo",
    "motivo_rechazo",
    "run_id",
    "hash_archivo",
]


# ── Helpers internos ───────────────────────────────────────────────────────────  # noqa: E501


def _normalizar_df(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Renombra columnas CSV → SQL y agrega la columna 'periodo' (YYYY-MM)."""
    # Solo columnas mapeadas
    cols_presentes = [c for c in col_map if c in df.columns]
    df_out = df[cols_presentes].rename(columns=col_map).copy()

    # Derivar periodo desde fecha si no viene en el CSV
    if "periodo" not in df_out.columns and "fecha" in df_out.columns:
        df_out["periodo"] = pd.to_datetime(
            df_out["fecha"], errors="coerce"
        ).dt.strftime("%Y-%m")

    return df_out


def _bulk_insert(cursor, tabla: str, df: pd.DataFrame) -> int:
    """INSERT fila a fila dentro de una transacción abierta. Retorna filas insertadas."""  # noqa: E501
    cols = list(df.columns)
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT INTO {tabla} ({col_list}) VALUES ({placeholders})"
    for _, fila in df.iterrows():
        cursor.execute(sql, [None if pd.isna(v) else v for v in fila.tolist()])
    return len(df)


# ── API pública ────────────────────────────────────────────────────────────────  # noqa: E501


def cargar_validados(
    staging_csv: Path,
    run_id: str,
    hash_archivo: str,
) -> dict:
    """
    Carga el staging limpio a PRODUCCION.despachos_validados.

    Estrategia: DELETE por periodo + INSERT — idempotente por run.

    Returns
    -------
    dict con claves: ok, registros_cargados, eliminados_previos, error
    """
    df = pd.read_csv(staging_csv, sep=";", dtype=str)
    df_sql = _normalizar_df(df, _MAP_VALIDADOS)
    df_sql["run_id"] = run_id
    df_sql["hash_archivo"] = hash_archivo

    # Solo columnas que existen en la tabla
    df_sql = df_sql[[c for c in _COLS_VALIDADOS if c in df_sql.columns]]

    conn = None
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        cursor = conn.cursor()

        # DELETE selectivo por periodos presentes en el staging
        periodos = df_sql["periodo"].dropna().unique().tolist()
        ph = ", ".join("?" * len(periodos))
        cursor.execute(
            f"DELETE FROM {_TBL_VALIDADOS} WHERE periodo IN ({ph})", periodos
        )
        eliminados = cursor.rowcount

        insertados = _bulk_insert(cursor, _TBL_VALIDADOS, df_sql)
        conn.commit()

        return {
            "ok": True,
            "registros_cargados": insertados,
            "eliminados_previos": eliminados,
            "error": None,
        }

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {
            "ok": False,
            "registros_cargados": 0,
            "eliminados_previos": 0,
            "error": str(exc),
        }
    finally:
        if conn:
            conn.close()


def cargar_rechazados(
    pendientes_csv: Path,
    run_id: str,
    hash_archivo: str,
    motivo_rechazo: str = "Duplicado detectado por inspector",
) -> dict:
    """
    Carga los registros pendientes/rechazados a PRODUCCION.despachos_rechazados.  # noqa: E501

    No elimina registros previos — es acumulativo para mantener historial.
    Evita reinsertar filas del mismo run_id (idempotencia).

    Returns
    -------
    dict con claves: ok, registros_cargados, error
    """
    if not pendientes_csv.exists():
        return {"ok": True, "registros_cargados": 0, "error": None}

    df = pd.read_csv(pendientes_csv, sep=";", dtype=str)
    if df.empty:
        return {"ok": True, "registros_cargados": 0, "error": None}

    df_sql = _normalizar_df(df, _MAP_RECHAZADOS)
    df_sql["motivo_rechazo"] = motivo_rechazo
    df_sql["run_id"] = run_id
    df_sql["hash_archivo"] = hash_archivo

    df_sql = df_sql[[c for c in _COLS_RECHAZADOS if c in df_sql.columns]]

    conn = None
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        cursor = conn.cursor()

        # Idempotencia: si se reprocesa el mismo archivo, reemplazar solo sus pendientes.  # noqa: E501
        # Los registros ya resueltos (APROBADO/BAJADO) quedan intactos.
        cursor.execute(
            f"DELETE FROM {_TBL_RECHAZADOS} WHERE hash_archivo = ? AND estado_revision = 'PENDIENTE'",  # noqa: E501
            hash_archivo,
        )

        insertados = _bulk_insert(cursor, _TBL_RECHAZADOS, df_sql)
        conn.commit()

        return {"ok": True, "registros_cargados": insertados, "error": None}

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"ok": False, "registros_cargados": 0, "error": str(exc)}
    finally:
        if conn:
            conn.close()


def resolver_rechazado(
    id_fila: int,
    accion: str,
    comentario: Optional[str],
    usuario: str,
) -> dict:
    """
    Aplica la decisión del usuario sobre un registro rechazado.

    Acciones:
      - APROBADO  → UPDATE estado + INSERT en despachos_validados
      - BAJADO    → UPDATE estado (queda como registro descartado)
      - MODIFICAR → UPDATE estado (queda pendiente de corrección externa)

    Returns
    -------
    dict con claves: ok, accion_aplicada, error
    """
    accion = accion.upper()
    if accion not in ("APROBADO", "BAJADO", "MODIFICAR"):
        return {
            "ok": False,
            "accion_aplicada": None,
            "error": f"Acción inválida: '{accion}'. Valores: APROBADO | BAJADO | MODIFICAR",  # noqa: E501
        }

    conn = None
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        cursor = conn.cursor()

        # Leer la fila completa para poder migrarla si corresponde
        cursor.execute(
            f"SELECT * FROM {_TBL_RECHAZADOS} WHERE id = ?", id_fila
        )
        row = cursor.fetchone()
        if not row:
            return {
                "ok": False,
                "accion_aplicada": None,
                "error": f"No existe fila con id={id_fila} en {_TBL_RECHAZADOS}",  # noqa: E501
            }

        # Capturar nombres de columnas ANTES de ejecutar cualquier otro statement  # noqa: E501
        cols_desc = [desc[0] for desc in cursor.description]
        fila_dict = dict(zip(cols_desc, row))

        ts_ahora = datetime.now().isoformat(sep=" ", timespec="seconds")

        # UPDATE estado en rechazados
        cursor.execute(
            f"""UPDATE {_TBL_RECHAZADOS}
                SET estado_revision     = ?,
                    comentario_revision = ?,
                    fecha_revision      = ?,
                    revisado_por        = ?
                WHERE id = ?""",
            accion,
            comentario,
            ts_ahora,
            usuario,
            id_fila,
        )

        # Si APROBADO → migrar a validados
        if accion == "APROBADO":

            cols_val = [c for c in _COLS_VALIDADOS if c in fila_dict]
            vals = [fila_dict[c] for c in cols_val]
            ph = ", ".join("?" * len(cols_val))
            col_list = ", ".join(cols_val)
            cursor.execute(
                f"INSERT INTO {_TBL_VALIDADOS} ({col_list}) VALUES ({ph})",
                vals,
            )

        conn.commit()
        return {"ok": True, "accion_aplicada": accion, "error": None}

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"ok": False, "accion_aplicada": None, "error": str(exc)}
    finally:
        if conn:
            conn.close()


def get_pendientes_df() -> pd.DataFrame:
    """
    Retorna los registros con estado_revision = 'PENDIENTE' para el Dashboard.

    Returns
    -------
    DataFrame vacío si no hay pendientes o si la BD no está disponible.
    """
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=True)
        df = pd.read_sql(
            f"SELECT * FROM {_TBL_RECHAZADOS} WHERE estado_revision = 'PENDIENTE' ORDER BY id",  # noqa: E501
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_validados_df() -> pd.DataFrame:
    """
    Retorna todos los registros de despachos_validados para el Dashboard.

    Returns
    -------
    DataFrame vacío si la BD no está disponible.
    """
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=True)
        df = pd.read_sql(
            f"SELECT * FROM {_TBL_VALIDADOS} ORDER BY periodo, fecha",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def bajar_pendientes_masivo(
    comentario: str = "Duplicados reales — baja masiva",
    usuario: str = "richard",
) -> dict:
    """
    Marca como BAJADO todos los registros con
    estado_revision='PENDIENTE' en despachos_rechazados.

    Usar cuando la totalidad de los pendientes son
    duplicados reales confirmados.

    Returns
    -------
    dict: ok, registros_actualizados, error
    """
    conn = None
    try:
        conn = pyodbc.connect(get_connection_string(), autocommit=False)
        cursor = conn.cursor()
        ts_ahora = datetime.now().isoformat(sep=" ", timespec="seconds")
        cursor.execute(
            f"""UPDATE {_TBL_RECHAZADOS}
                SET estado_revision     = 'BAJADO',
                    comentario_revision = ?,
                    fecha_revision      = ?,
                    revisado_por        = ?
                WHERE estado_revision   = 'PENDIENTE'""",
            comentario,
            ts_ahora,
            usuario,
        )
        actualizados = cursor.rowcount
        conn.commit()
        return {
            "ok": True,
            "registros_actualizados": actualizados,
            "error": None,
        }
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {
            "ok": False,
            "registros_actualizados": 0,
            "error": str(exc),
        }
    finally:
        if conn:
            conn.close()
