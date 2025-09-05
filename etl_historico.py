import pandas as pd
from bq_client import BigQueryClient
from validation import validate_departments, validate_jobs, validate_hired_employees
import os

# Configuración
PROJECT_ID = "migracionpoc"
DATASET = "migration_poc"
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

CHUNK_SIZE = 1000

# Flag para ejecutar en modo seguro (no inserta en BD)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

TABLE_SCHEMAS = {
    "departments": ["id", "name"],
    "jobs": ["id", "name"],
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
}

# Inicializar cliente
bq = BigQueryClient(PROJECT_ID, DATASET, credentials_path=CREDENTIALS_PATH)

# Mapear CSV a tabla y función de validación
DATA_DIR = "/app/data"

tables_config = {
    "departments": {
        "csv": os.path.join(DATA_DIR, "departments.csv"),
        "validator": validate_departments
    },
    "jobs": {
        "csv": os.path.join(DATA_DIR, "jobs.csv"),
        "validator": validate_jobs
    },
    "hired_employees": {
        "csv": os.path.join(DATA_DIR, "hired_employees.csv"),
        "validator": validate_hired_employees
    }
}

def read_csv_with_schema(csv_path, table_name, chunksize):
    expected_cols = TABLE_SCHEMAS[table_name]

    # Intentar leer con header
    try:
        test_df = pd.read_csv(csv_path, nrows=5, dtype=str)
        if set(expected_cols).issubset(test_df.columns):
            return pd.read_csv(csv_path, chunksize=chunksize, dtype=str)
    except Exception:
        pass

    # Si no tiene header correcto → forzar columnas
    return pd.read_csv(
        csv_path,
        chunksize=chunksize,
        header=None,
        names=expected_cols,
        dtype=str
    )


def process_csv(table_name, csv_path, validator):
    print(f"Procesando {csv_path} → {table_name}")

    for chunk in read_csv_with_schema(csv_path, table_name, CHUNK_SIZE):
        valid_rows = []
        for _, row in chunk.iterrows():
            row_dict = row.to_dict()
            validated, error = validator(row_dict)
            if error:
                if DRY_RUN:
                    print(f"[DRY RUN] DLQ {table_name}: {row_dict} → {error}")
                else:
                    bq.insert_dlq(table_name, row_dict, error)
            else:
                valid_rows.append(validated)

        if valid_rows:
            if DRY_RUN:
                print(f"[DRY RUN] Insertaría {len(valid_rows)} filas en {table_name}")
            else:
                bq.insert_rows(table_name, valid_rows)


if __name__ == "__main__":
    print(f"Iniciando ETL (DRY_RUN={DRY_RUN})")
    for table, cfg in tables_config.items():
        process_csv(table, cfg["csv"], cfg["validator"])
