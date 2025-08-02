import os
import re
import pandas as pd

# Carpeta donde están los archivos .pkl
OUTPUT_DIR = "output"

# Patrones de agrupación
GROUP_PATTERNS = [
    "alquiler_departamentos",
    "alquiler_oficinas",
    "alquiler_locales-comerciales",
    "venta_departamentos",
    "venta_locales-comerciales",
    "venta_oficinas-comerciales"
]

# Expresión regular para extraer la fecha del nombre del archivo
DATE_REGEX = re.compile(r"(\d{4}_\d{2}_\d{2})\.pkl$")

def extract_scrap_date(filename):
    match = DATE_REGEX.search(filename)
    if match:
        return match.group(1).replace("_", "-")
    return None

def main():
    # Listar todos los archivos .pkl en la carpeta output
    files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".pkl")]

    # Agrupar archivos por patrón
    grouped_files = {pattern: [] for pattern in GROUP_PATTERNS}
    for f in files:
        for pattern in GROUP_PATTERNS:
            if pattern in f:
                grouped_files[pattern].append(f)
                break

    # Procesar cada grupo
    for pattern, file_list in grouped_files.items():
        dfs = []
        for fname in file_list:
            fpath = os.path.join(OUTPUT_DIR, fname)
            df = pd.read_pickle(fpath)
            # Si no tiene 'scrap_date', agregarlo desde el nombre del archivo
            if 'scrap_date' not in df.columns:
                scrap_date = extract_scrap_date(fname)
                df['scrap_date'] = scrap_date
            dfs.append(df)
        if dfs:
            df_concat = pd.concat(dfs, ignore_index=True)
            output_name = f"{pattern}_consolidado.pkl"
            output_path = os.path.join(OUTPUT_DIR, output_name)
            df_concat.to_pickle(output_path)
            print(f"Consolidado guardado en: {output_path}")

if __name__ == "__main__":
    main()