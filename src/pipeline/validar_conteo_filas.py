import pandas as pd
import os
import warnings
import glob

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# Configuration
BASE_DIR = r"C:\Dev\Planif_POSE\data\input_raw"
FOLDERS = [
    "2021_2022_Historico",
    "2023_2025_Hist",
    "2025_Ajustes",
    "2025",
    "2026",
    "Modificaciones",
]

# Synonym Map (mimicking fn_LeerExcelNorm.pq)
SYNONYMS = {
    # FECHA
    "FECHA*": "FECHA",
    "FECHA COMPROBANTE": "FECHA",
    "FEC. EMISION": "FECHA",
    "FECHA": "FECHA",
    # OBRA_PRONTO
    "OBRA PRONTO*": "OBRA_PRONTO",
    "OBRA_PRONTO*": "OBRA_PRONTO",
    "OBRA PRONTO": "OBRA_PRONTO",
    "CODIGO OBRA": "OBRA_PRONTO",
    "COD. OBRA": "OBRA_PRONTO",
    "CENTRO DE COSTO": "OBRA_PRONTO",
    "CENTRO_COSTO": "OBRA_PRONTO",
    "OBRA_PRONTO": "OBRA_PRONTO",
    # VALOR
    "IMPORTE*": "IMPORTE",
    "IMPORTE NETO": "IMPORTE",
    "MONTO": "IMPORTE",
    "TOTAL": "IMPORTE",
    "IMPORTE": "IMPORTE",
    "IMPORTE 2": "IMPORTE",
    # FUENTE
    "FUENTE*": "FUENTE",
    "costos": "FUENTE",
    "COSTOS": "FUENTE",
    "FUENTE": "FUENTE",
}


def normalize_header(column_name):
    if not isinstance(column_name, str):
        return str(column_name)

    col_upper = column_name.strip().upper()
    col_raw = (
        column_name.strip()
    )  # original casing for exact matches  # noqa: E501

    # Check exact match in synonyms (iterate keys)  # noqa: E501
    for key, target in SYNONYMS.items():
        # Handle wildcard in key
        key_clean = key.replace("*", "")
        if key_clean == col_raw or key_clean == col_upper:
            return target
        if "*" in key and key_clean in col_raw:  # noqa: E501
            return target

    # Direct Key match trial
    if col_upper in ["FECHA", "OBRA_PRONTO", "FUENTE"]:
        return col_upper

    return col_upper


def process_file(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet_names = xl.sheet_names

        file_valid_count = 0
        file_initial_count = 0
        processed_sheets = []

        # Iterate over ALL sheets (mimicking the new PQ multi-sheet logic)
        for sheet in sheet_names:
            try:
                df = pd.read_excel(filepath, sheet_name=sheet)

                # Normalize columns
                df.columns = [normalize_header(c) for c in df.columns]

                # Check if required columns exist  # noqa: E501

                required = ["FECHA", "OBRA_PRONTO", "FUENTE"]
                missing = [c for c in required if c not in df.columns]

                for m in missing:
                    df[m] = None

                # Filter Logic per row  # noqa: E501
                # [FECHA] <> null and [OBRA_PRONTO] <> null
                initial_count = len(df)

                if initial_count > 0:
                    df_valid = df.dropna(subset=required)
                    valid_count = len(df_valid)

                    file_valid_count += valid_count
                    file_initial_count += initial_count
                    processed_sheets.append(f"{sheet}({valid_count})")
            except Exception as e:
                print(f"  Warning: Could not read sheet {sheet}: {e}")

        return (
            file_valid_count,
            file_initial_count,
            ", ".join(processed_sheets),
        )

    except Exception as e:
        print(f"Error reading {os.path.basename(filepath)}: {e}")
        return 0, 0, "Error"


print(
    f"{'CARPETA':<30} | {'ARCHIVO':<40}"
    f" | {'FILAS VALIDAS':>15} | {'FILAS TOTALES':>15}"
)
print("-" * 110)

total_consolidado = 0
stats_por_carpeta = {}

for folder in FOLDERS:
    folder_path = os.path.join(BASE_DIR, folder)
    files = glob.glob(os.path.join(folder_path, "*.xlsx")) + glob.glob(
        os.path.join(folder_path, "*.xls")
    )

    count_folder = 0
    print(f"[{folder}]")

    for file in files:
        if "~$" in file:
            continue

        v, t, sheet = process_file(file)
        count_folder += v
        print(
            f"{'':< 30} | {os.path.basename(file):<40}"
            f" | {v:15,d} | {t:15,d} ({sheet})"
        )

    stats_por_carpeta[folder] = count_folder
    total_consolidado += count_folder
    print("-" * 110)

print("\nRESUMEN FINAL:")
for k, v in stats_por_carpeta.items():
    print(f"{k:<30}: {v:10,d}")
print(f"{'TOTAL CONSOLIDADO':<30}: {total_consolidado:10,d}")
