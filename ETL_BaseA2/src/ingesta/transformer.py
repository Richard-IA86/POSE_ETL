"""
Módulo para transformación y limpieza de datos
"""

import pandas as pd
from datetime import datetime  # noqa: F401


class DataTransformer:
    """Transformador de datos con detección inteligente de duplicados"""

    def __init__(self):
        self.stats = {
            "duplicados_origen": 0,
            "duplicados_proceso": 0,
            "filas_eliminadas": 0,
            "filas_sin_fecha": 0,
            "importe_sin_dato": 0,
            "importe_costo_cero": 0,
        }

    def reset_stats(self):
        """Resetea contadores para siguiente archivo"""
        self.stats = {
            "duplicados_origen": 0,
            "duplicados_proceso": 0,
            "filas_eliminadas": 0,
            "filas_sin_fecha": 0,
            "importe_sin_dato": 0,
            "importe_costo_cero": 0,
        }

    def filtrar_por_anio(self, df, anios_permitidos):
        """
        Filtra DataFrame por año(s) específico(s)

        Args:
            df: DataFrame con columna FECHA normalizada (formato dd/mm/yyyy)
            anios_permitidos: Lista de años permitidos (ej: [2021, 2022])

        Returns:
            DataFrame filtrado
        """
        if df is None or df.empty:
            return df

        if "FECHA" not in df.columns:
            print("  ⚠️  No se encontró columna FECHA para filtrar por año")
            return df

        if not anios_permitidos:
            return df

        # Extraer año de la fecha dd/mm/yyyy
        # Manejar fechas vacías/nulas (que son strings vacíos tras normalizar)
        df["_ANIO_TEMP"] = pd.to_datetime(
            df["FECHA"], format="%d/%m/%Y", errors="coerce"
        ).dt.year

        # Filtrar
        filas_antes = len(df)
        df = df[df["_ANIO_TEMP"].isin(anios_permitidos)]
        filas_despues = len(df)

        # Estadísticas
        excluidos = filas_antes - filas_despues
        if excluidos > 0:
            print(
                f"  🗓️  Filtro año {anios_permitidos}: Excluidos {excluidos:,} registros ({filas_despues:,} restantes)"  # noqa: E501
            )
        else:
            print(
                f"  ✅ Filtro año {anios_permitidos}: Todos los registros cumplen el criterio"  # noqa: E501
            )

        # Limpiar columna temporal
        df = df.drop(columns=["_ANIO_TEMP"])

        return df

    def detectar_duplicados(self, df, modo: str = "soft"):
        """
        Detecta y elimina duplicados con política configurable.

        Modos:
          "soft" (predeterminado) — solo elimina duplicados de proceso
              (misma fila repetida por el pipeline, mismo _ID_INGESTA).
              Preserva duplicados de ORIGEN (mismo contenido pero
              distintos _ID_INGESTA, es decir, archivos distintos).
          "strict" — elimina TODOS los duplicados sobre los campos
              clave, sin importar el _ID_INGESTA.  Útil para segmentos
              donde se garantiza unicidad total.

        Args:
            df: DataFrame con columna _ID_INGESTA.
            modo: "soft" | "strict".  Cualquier otro valor usa "soft".

        Returns:
            DataFrame sin los duplicados según el modo elegido.
        """
        if df is None or df.empty:
            return df

        if "_ID_INGESTA" not in df.columns:
            print(
                "⚠️ No hay columna _ID_INGESTA, "
                "no se puede diferenciar duplicados"
            )
            return df

        modo_efectivo = modo if modo in ("soft", "strict") else "soft"
        print(f"  🔍 Política duplicados: {modo_efectivo.upper()}")

        # Campos para identificar duplicados (búsqueda flexible).
        # Los asteriscos (*) se eliminan en normalizar_columnas().
        posibles_campos = {
            "obra": [
                "OBRA",
                "OBRA_PRONTO",
                "DESCRIPCION_OBRA",
                "OBRA PRONTO",
            ],
            "fecha": ["FECHA"],
            "detalle": ["DETALLE"],
            "importe": ["IMPORTE"],
        }

        # Buscar campos disponibles en el DataFrame
        campos_disponibles = []
        for tipo, variantes in posibles_campos.items():
            for campo in variantes:
                if campo in df.columns:
                    campos_disponibles.append(campo)
                    break

        if not campos_disponibles:
            print("⚠️ No hay campos clave para detectar duplicados")
            return df

        print("  🔍 Campos clave: " f"{', '.join(campos_disponibles)}")

        try:
            if modo_efectivo == "strict":
                # STRICT: eliminar todos los duplicados sobre campos clave
                antes = len(df)
                df = df.drop_duplicates(
                    subset=campos_disponibles, keep="first"
                )
                eliminados = antes - len(df)
                self.stats["duplicados_proceso"] += eliminados
                self.stats["filas_eliminadas"] += eliminados
                if eliminados:
                    print(f"  [strict] {eliminados} duplicados eliminados")
            else:
                # SOFT: diferenciar origen vs proceso (vectorizado)
                dup_mask = df.duplicated(subset=campos_disponibles, keep=False)
                if dup_mask.any():
                    df_d = df.loc[dup_mask].copy()
                    df_d["_GRUPO_DUP"] = df_d.groupby(
                        campos_disponibles,
                        dropna=False,
                        sort=False,
                    ).ngroup()
                    df_d["_IDS_UNICOS"] = df_d.groupby("_GRUPO_DUP")[
                        "_ID_INGESTA"
                    ].transform("nunique")
                    df_d["_CUMCOUNT"] = df_d.groupby("_GRUPO_DUP").cumcount()

                    # ORIGEN: IDs distintos → conservar, solo contar
                    n_origen = int(
                        (
                            (df_d["_IDS_UNICOS"] > 1) & (df_d["_CUMCOUNT"] > 0)
                        ).sum()
                    )
                    self.stats["duplicados_origen"] += n_origen

                    # PROCESO: mismo ID → eliminar extras
                    indices_a_eliminar = df_d.loc[
                        (df_d["_IDS_UNICOS"] == 1) & (df_d["_CUMCOUNT"] > 0)
                    ].index.tolist()
                    n_proceso = len(indices_a_eliminar)
                    self.stats["duplicados_proceso"] += n_proceso
                    self.stats["filas_eliminadas"] += n_proceso

                    if indices_a_eliminar:
                        df = df.drop(index=indices_a_eliminar)

        except Exception as e:
            print(f"  ⚠️ Error detectando duplicados: {str(e)}")
            print(
                "  ⚠️ Continuando sin detección de duplicados "
                "para este archivo"
            )

        return df

    def normalizar_columnas(self, df):
        """
        Normaliza nombres de columnas y tipos de datos

        Args:
            df: DataFrame a normalizar

        Returns:
            DataFrame normalizado
        """
        if df is None or df.empty:
            return df

        # Normalizar nombres de columnas
        df.columns = (
            df.columns.str.strip()
            .str.upper()
            .str.replace("*", "", regex=False)
        )

        # Eliminar columnas duplicadas resultantes (ej: DETALLE y DETALLE* -> DETALLE duplicado)  # noqa: E501
        if df.columns.duplicated().any():
            print(
                f"  ⚠️ Columnas duplicadas eliminadas: {df.columns[df.columns.duplicated()].tolist()}"  # noqa: E501
            )
            df = df.loc[:, ~df.columns.duplicated()]

        # Prioridad IMPORTE2 / IMPORTE 2 > IMPORTE
        # (ej: archivos 2023_2025_Hist)
        # Si coexisten ambas columnas, la variante es la canónica.
        for _col_imp2 in ("IMPORTE2", "IMPORTE 2"):
            if _col_imp2 in df.columns and "IMPORTE" in df.columns:
                print(
                    f"  ℹ️  '{_col_imp2}' detectada junto a IMPORTE "
                    "→ se usa como columna canónica."
                )
                df = df.drop(columns=["IMPORTE"])
                break

        # Mapeo de columnas GLOBAL (Basado en sinónimos de Power Query)
        # Esto asegura que sin importar el origen, el destino final tenga nombres estandarizados  # noqa: E501
        mapeo_cols = {
            # Columnas Clave
            "CENTRO DE COSTO": "OBRA_PRONTO",
            "CENTRO_COSTO": "OBRA_PRONTO",
            "CODIGO OBRA": "OBRA_PRONTO",
            "COD. OBRA": "OBRA_PRONTO",
            "OBRA": "OBRA_PRONTO",
            "OBRA PRONTO": "OBRA_PRONTO",
            "COSTOS": "FUENTE",
            "COSTO": "FUENTE",
            "FECHA COMPROBANTE": "FECHA",
            "FEC. EMISION": "FECHA",
            "PERIODO": "FECHA",
            "FECHA_PERIODO": "FECHA",
            "IMPORTE2": "IMPORTE",
            "IMPORTE 2": "IMPORTE",
            "IMPORTE NETO": "IMPORTE",
            "MONTO": "IMPORTE",
            "TOTAL": "IMPORTE",
            # Otras columnas
            "DETALLE CENTRO DE COSTO": "DESCRIPCION_OBRA",
            "PROVEEDOR RAZON SOCIAL": "PROVEEDOR",
            "RAZON SOCIAL": "PROVEEDOR",
            "PROVEEDORES": "PROVEEDOR",
            "NRO COMPROBANTE": "NRO_COMPROBANTE",
            "NUMERO": "NRO_COMPROBANTE",
            "N° COMPROBANTE": "NRO_COMPROBANTE",
            "TIPO COMP": "TIPO_COMPROBANTE",
            "TIPO": "TIPO_COMPROBANTE",
            # Cuenta contable (compensaciones)
            "CODIGO CUENTA": "CODIGO_CUENTA",
            # Columnas presentes en compensaciones y otros formatos
            "RUBRO CONTABLE": "RUBRO_CONTABLE",
            "CUENTA CONTABLE": "CUENTA_CONTABLE",
            "OBSERVACION": "OBSERVACION",
            "TIPO COMPROBANTE": "TIPO_COMPROBANTE",
            "N° COMPROBANTE": "NRO_COMPROBANTE",
        }
        df.rename(columns=mapeo_cols, inplace=True)

        # Validar existencia de columnas críticas (Advertencia)
        cols_criticas = ["FECHA", "OBRA_PRONTO", "FUENTE", "IMPORTE"]
        faltantes = [c for c in cols_criticas if c not in df.columns]
        if faltantes:
            print(
                f"  ⚠️  Advertencia: Faltan columnas críticas estandarizadas: {faltantes}"  # noqa: E501
            )

        # --- CONVERSIÓN DE TIPOS DE DATOS (FINAL) ---

        # 1. Normalizar FECHA -> Estrategia Robusta "Todo Terreno"
        if "FECHA" in df.columns:
            # Paso 1: Conversión estándar (Maneja ISO, dd/mm/yy, y objetos fecha)  # noqa: E501
            fechas_dt = pd.to_datetime(
                df["FECHA"], dayfirst=True, errors="coerce"
            )

            # Paso 2: Rescate de Números de Serie Excel (ej: 45152) para los fallidos (NaT)  # noqa: E501
            # Si to_datetime falló pero el valor original existe, podría ser un número serial.  # noqa: E501
            mask_fallidos = fechas_dt.isna() & df["FECHA"].notna()
            if mask_fallidos.any():
                try:
                    # Intenta convertir solo los fallidos a número
                    numericos = pd.to_numeric(
                        df.loc[mask_fallidos, "FECHA"], errors="coerce"
                    )
                    # Convierte números seriales a fecha (Origen Excel estándar)  # noqa: E501
                    recuperados = pd.to_datetime(
                        numericos,
                        unit="D",
                        origin="1899-12-30",
                        errors="coerce",
                    )
                    # Rellenar los NaT originales con lo recuperado
                    fechas_dt = fechas_dt.fillna(recuperados)
                except Exception:
                    pass  # Si falla el rescate, se queda como NaT

            # Reportar fechas inválidas
            nulos = fechas_dt.isna().sum()
            self.stats["filas_sin_fecha"] = int(nulos)
            if nulos > 0:
                print(
                    f"  ⚠️  Advertencia: Columna 'FECHA' tiene {nulos} filas inválidas."  # noqa: E501
                )

            # Paso 3: Forzar formato string dd/mm/yyyy (Mimic de tu fórmula Excel)  # noqa: E501
            # dt.strftime convierte NaT a NaN (que en Excel es celda vacía)
            df["FECHA"] = fechas_dt.dt.strftime("%d/%m/%Y")

        # 2. Normalizar TEXTOS CLAVE (Evitar conversión a números)
        # OBRA_PRONTO, FUENTE, PROVEEDOR, NRO_COMPROBANTE, TIPO_COMPROBANTE
        for col in [
            "OBRA_PRONTO",
            "FUENTE",
            "PROVEEDOR",
            "NRO_COMPROBANTE",
            "TIPO_COMPROBANTE",
            "DETALLE",
            "DESCRIPCION_OBRA",
        ]:
            if col in df.columns:
                # Convertir a string
                # astype(str) convierte: NaN -> 'nan', None -> 'None'
                df[col] = df[col].astype(str)

                # Normalizar a MAYÚSCULAS (crítico para joins)
                # Ejemplo: "Sin Obra" -> "SIN OBRA" para hacer match con Dim_Obras  # noqa: E501
                df[col] = df[col].str.upper().str.strip()

                # Limpiar representaciones de nulos generadas por str()
                # Se eliminan 'NAN', 'NONE', '<NA>' (ya en mayúsculas)
                valores_nulos = ["NAN", "NONE", "<NA>", "NAT"]
                df[col] = df[col].replace(valores_nulos, "")

                # --- VALIDACIÓN ESPECÍFICA: OBRA_PRONTO ---
                # Normalización inteligente según tipo de dato:
                # - Numéricos puros: padding a 8 dígitos ("139" -> "00000139")
                # - Alfanuméricos: mantener sin cambios ("HYDRA" -> "HYDRA")
                if col == "OBRA_PRONTO":
                    # 1. Quitar decimales flotantes (.0) si existen (ej: "139.0" -> "139")  # noqa: E501
                    df[col] = df[col].str.replace(r"\.0$", "", regex=True)

                    # 2. Aplicar padding SOLO a valores 100% numéricos
                    # Evita convertir "HYDRA" en "000HYDRA"
                    mask_numericos = df[col].str.match(r"^\d+$", na=False)
                    if mask_numericos.any():
                        df.loc[mask_numericos, col] = df.loc[
                            mask_numericos, col
                        ].str.zfill(8)

                    # 3. Validación: detectar mixtos (números + letras)
                    mask_validos = df[col].notna() & (df[col] != "")
                    mask_solo_numeros = df[col].str.match(r"^\d+$", na=False)
                    mask_solo_alfanum = df[col].str.match(
                        r"^[A-Z\s\-]+$", na=False
                    )
                    mask_mixtos = (
                        mask_validos & ~mask_solo_numeros & ~mask_solo_alfanum
                    )

                    if mask_mixtos.any():
                        valores_mixtos = df.loc[mask_mixtos, col].unique()[
                            :5
                        ]  # Primeros 5
                        print(
                            f"      ⚠️  {mask_mixtos.sum()} OBRA_PRONTO con formato mixto detectados: {valores_mixtos.tolist()}"  # noqa: E501
                        )
                        print(
                            "      ⚠️  Estos valores serán rechazados en Power Query"  # noqa: E501
                        )

                # Devolver a None real aquellos que quedaron vacíos
                df[col] = df[col].replace("", None)

        # 3. Normalizar IMPORTE -> Float
        if "IMPORTE" in df.columns:
            df["IMPORTE"] = pd.to_numeric(df["IMPORTE"], errors="coerce")

            # --- Calidad de datos: "Sin Dato" vs "Costo Cero" ---
            # "Sin Dato" = era nulo/vacío en el origen (no tenía importe).
            # "Costo Cero" = era explícitamente 0 en el origen.
            # Ambos se rellenan con 0.0 para que PQ no descarte la fila,
            # pero se contabilizan por separado en stats.
            nulos_importe = int(df["IMPORTE"].isna().sum())
            ceros_origen = int((df["IMPORTE"] == 0.0).sum())

            self.stats["importe_sin_dato"] = nulos_importe
            self.stats["importe_costo_cero"] = ceros_origen

            if nulos_importe > 0:
                print(
                    f"  ⚠️  Sin Dato (IMPORTE nulo): {nulos_importe} filas "
                    "rellenadas con 0.0 para evitar descarte PQ."
                )
            if ceros_origen > 0:
                print(
                    f"  ℹ️  Costo Cero (IMPORTE=0 en origen): "
                    f"{ceros_origen} filas."
                )

            # Rellenar nulos con 0.0 (Power Query descarta nulos numéricos)
            if nulos_importe > 0:
                self.stats["filas_importe_cero_rellenadas"] = nulos_importe
                df["IMPORTE"] = df["IMPORTE"].fillna(0.0)

            # Calcular suma total del importe para validación (Cross-Check con Source)  # noqa: E501
            total_importe = df["IMPORTE"].sum()
            self.stats["suma_importe"] = float(total_importe)

        return df

    def consolidar_dataframes(self, lista_dfs, modo_duplicados: str = "soft"):
        """
        Consolida múltiples DataFrames en uno solo.

        Args:
            lista_dfs: Lista de DataFrames.
            modo_duplicados: "soft" | "strict" — política de duplicados.

        Returns:
            DataFrame consolidado.
        """
        if not lista_dfs:
            return None

        # Concatenar todos los DataFrames
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)

        # Aplicar normalización
        df_consolidado = self.normalizar_columnas(df_consolidado)

        # Detectar y eliminar duplicados según política configurada
        df_consolidado = self.detectar_duplicados(
            df_consolidado, modo=modo_duplicados
        )

        return df_consolidado

    def get_stats(self):
        """Retorna estadísticas de procesamiento"""
        return self.stats.copy()

    def reset_stats(self):  # type: ignore[no-redef]  # noqa: F811
        """Reinicia estadísticas"""
        self.stats = {
            "duplicados_origen": 0,
            "duplicados_proceso": 0,
            "filas_eliminadas": 0,
            "filas_sin_fecha": 0,
            "importe_sin_dato": 0,
            "importe_costo_cero": 0,
        }
