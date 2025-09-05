from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import datetime
from google.cloud import storage
import os

class BigQueryClient:
    def __init__(self, project_id, dataset, credentials_path=None):
        if credentials_path:
            creds = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(project=project_id, credentials=creds)
        else:
            self.client = bigquery.Client(project=project_id)

        self.project_id = project_id
        self.dataset = dataset

    def _table_path(self, table_name: str) -> str:
        """Devuelve el path completo project.dataset.table"""
        return f"{self.project_id}.{self.dataset}.{table_name}"

    def insert_rows(self, table, rows):
        table_id = self._table_path(table)
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Errores insertando en {table}: {errors}")
        else:
            print(f"Insertados {len(rows)} registros en {table}")
        return 0 if errors else len(rows)


    def insert_dlq(self, table_name, raw_row, error_reason):
        #Guarda registros inválidos en la tabla DLQ
        table_id = self._table_path("dlq")
        row = {
            "table_name": table_name,
            "row_data": json.dumps(raw_row),
            "error": error_reason,
            "inserted_at": datetime.utcnow().isoformat()
        }
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            print(f"Error insertando en DLQ: {errors}")
        else:
            print(f"Registro inválido enviado a DLQ")

    def export_table_to_gcs(self, table_name: str, gcs_uri: str, file_format: str = "PARQUET"):
        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"
        destination_uri = gcs_uri
        if file_format.upper() == "PARQUET":
            destination_format = bigquery.DestinationFormat.PARQUET
        else:
            destination_format = bigquery.DestinationFormat.AVRO

        extract_job = self.client.extract_table(
            table_ref,
            destination_uri,
            location="US",
            job_config=bigquery.job.ExtractJobConfig(destination_format=destination_format)
        )
        extract_job.result()  # bloquea hasta completar
        print(f"✅ Exportado {table_ref} a {gcs_uri} como {file_format}")
        return extract_job

    # ------------------
    # Exportar tabla a fichero local (usa query y escribe Parquet en local)
    # ------------------
    def export_table_to_local_parquet(self, table_name: str, local_path: str):
        query = f"SELECT * FROM `{self.project_id}.{self.dataset}.{table_name}`"
        df = self.client.query(query).result().to_dataframe(create_bqstorage_client=True)
        # guardar parquet con pyarrow
        df.to_parquet(local_path, index=False)
        print(f"✅ Exportado {table_name} a {local_path}")
        return local_path

    # ------------------
    # Restaurar desde GCS Parquet/Avro
    # ------------------
    def restore_table_from_gcs(self, table_name: str, gcs_uri: str, source_format: str = "PARQUET", write_disposition="WRITE_TRUNCATE"):
        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig()
        if source_format.upper() == "PARQUET":
            job_config.source_format = bigquery.SourceFormat.PARQUET
        else:
            job_config.source_format = bigquery.SourceFormat.AVRO

        job_config.write_disposition = write_disposition  # overwrite by default
        load_job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"Restaurado {table_ref} desde {gcs_uri}")
        return load_job

    # ------------------
    # Restaurar desde fichero local Parquet/Avro (sube temporalmente a GCS o carga direct desde file)
    # ------------------
    def restore_table_from_local_file(self, table_name: str, local_file_path: str, source_format: str = "PARQUET", write_disposition="WRITE_TRUNCATE"):
        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig()
        if source_format.upper() == "PARQUET":
            job_config.source_format = bigquery.SourceFormat.PARQUET
        else:
            job_config.source_format = bigquery.SourceFormat.AVRO

        job_config.write_disposition = write_disposition

        with open(local_file_path, "rb") as f:
            load_job = self.client.load_table_from_file(f, table_ref, job_config=job_config)
            load_job.result()
        print(f"Restaurado {table_ref} desde archivo local {local_file_path}")
        return load_job
