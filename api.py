from flask import Flask, request, jsonify
from bq_client import BigQueryClient
from validation import validate_departments, validate_jobs, validate_hired_employees
import os
from functools import wraps
from google.cloud import bigquery
import logging
from datetime import datetime, timezone

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

PROJECT_ID = "migracionpoc"
DATASET = "migration_poc"
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Cliente de BigQuery
bq = BigQueryClient(PROJECT_ID, DATASET, credentials_path=CREDENTIALS_PATH)

# API Key
API_KEY = os.getenv("API_KEY")

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("x-api-key")
        if not API_KEY or key != API_KEY:
            logging.warning("Unauthorized access attempt")
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# Diccionario de validadores por tabla
VALIDATORS = {
    "departments": validate_departments,
    "jobs": validate_jobs,
    "hired_employees": validate_hired_employees,
}

@app.route("/")
@require_api_key
def home():
    """Ruta de prueba para verificar que la API funciona"""
    return jsonify({"message": "API de Migración funcionando"}), 200

@app.route("/ingest", methods=["POST"])
@require_api_key
def ingest_data():
    """Endpoint para insertar registros en BigQuery con validación"""
    try:
        payload = request.get_json(force=True)

        if not payload or "table" not in payload or "records" not in payload:
            return jsonify({"error": "El request debe incluir 'table' y 'records'"}), 400

        table = payload["table"]
        records = payload["records"]

        if table not in VALIDATORS:
            return jsonify({"error": f"Tabla '{table}' no soportada"}), 400

        if not isinstance(records, list) or len(records) == 0 or len(records) > 1000:
            return jsonify({"error": "Debe enviar entre 1 y 1000 registros"}), 400

        validator = VALIDATORS[table]
        valid_data, errors = [], []

        for i, record in enumerate(records, start=1):
            try:
                validated, error = validator(record)
                if error:
                    errors.append({"index": i, "record": record, "error": error})
                    bq.insert_dlq(table, record, error)
                else:
                    valid_data.append(validated)
            except Exception as e:
                errors.append({"index": i, "record": record, "error": str(e)})
                bq.insert_dlq(table, record, str(e))

        response = {"inserted": 0, "errors": errors}
        if valid_data:
            response["inserted"] = bq.insert_rows(table, valid_data)

        return jsonify(response), (200 if valid_data else 400)
    except Exception as e:
        logging.error(f"Ingest error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/backup/<table_name>", methods=["POST"])
@require_api_key
def backup_table(table_name):
    """Endpoint para respaldar una tabla en PARQUET o AVRO"""
    body = request.get_json(silent=True) or {}
    target = body.get("target", "local")
    fmt = body.get("format", "PARQUET").upper()
    try:
        if target == "gcs":
            gcs_uri = body["gcs_uri"]
            job = bq.export_table_to_gcs(table_name, gcs_uri, file_format=fmt)
            return jsonify({"status": "ok", "job": str(job.job_id)}), 200
        else:
            local_path = body.get("local_path", f"/tmp/{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet")
            path = bq.export_table_to_local_parquet(table_name, local_path)
            return jsonify({"status": "ok", "path": path}), 200
    except Exception as e:
        logging.error(f"Backup error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/restore/<table_name>", methods=["POST"])
@require_api_key
def restore_table(table_name):
    """Endpoint para restaurar una tabla desde PARQUET o AVRO"""
    body = request.get_json(force=True)
    source = body.get("source", "local")
    fmt = body.get("format", "PARQUET").upper()
    write_disp = body.get("write_disposition", "WRITE_TRUNCATE")
    try:
        if source == "gcs":
            gcs_uri = body["gcs_uri"]
            job = bq.restore_table_from_gcs(table_name, gcs_uri, source_format=fmt, write_disposition=write_disp)
        else:
            local_path = body["local_path"]
            job = bq.restore_table_from_local_file(table_name, local_path, source_format=fmt, write_disposition=write_disp)
        return jsonify({"status": "ok", "job": str(job.job_id)}), 200
    except Exception as e:
        logging.error(f"Restore error: {str(e)}")
        return jsonify({"error": str(e)}), 500

def validate_year(year: int):
    """Valida que el año sea razonable"""
    current_year = datetime.now(timezone.utc).year
    if year < 1900 or year > current_year:
        raise ValueError(f"El año debe estar entre 2000 y {current_year}")

@app.route("/analytics/hired_by_quarter/<int:year>", methods=["GET"])
@require_api_key
def hired_by_quarter(year):
    """Cantidad de empleados contratados por trimestre, cargo y departamento"""
    try:
        validate_year(year)
        query = f"""
        SELECT
            d.name AS department,
            j.name AS job,
            SUM(CASE WHEN EXTRACT(QUARTER FROM DATE(h.hired_timestamp)) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN EXTRACT(QUARTER FROM DATE(h.hired_timestamp)) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN EXTRACT(QUARTER FROM DATE(h.hired_timestamp)) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN EXTRACT(QUARTER FROM DATE(h.hired_timestamp)) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM `{PROJECT_ID}.{DATASET}.hired_employees` h
        JOIN `{PROJECT_ID}.{DATASET}.departments` d ON h.department_id = d.id
        JOIN `{PROJECT_ID}.{DATASET}.jobs` j ON h.job_id = j.id
        WHERE EXTRACT(YEAR FROM DATE(h.hired_timestamp)) = @year
        GROUP BY department, job
        ORDER BY department ASC, job ASC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("year", "INT64", year)]
        )
        df = bq.client.query(query, job_config=job_config).result().to_dataframe()
        return df.to_json(orient="records"), 200
    except Exception as e:
        logging.error(f"Hired_by_quarter error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/analytics/departments_above_average/<int:year>", methods=["GET"])
@require_api_key
def departments_above_average(year):
    """Departamentos con contrataciones por encima del promedio en un año"""
    try:
        validate_year(year)
        query = f"""
        WITH hires_by_dept AS (
            SELECT
                d.id AS department_id,
                d.name AS department,
                COUNT(*) AS hired
            FROM `{PROJECT_ID}.{DATASET}.hired_employees` h
            JOIN `{PROJECT_ID}.{DATASET}.departments` d ON h.department_id = d.id
            WHERE EXTRACT(YEAR FROM DATE(h.hired_timestamp)) = @year
            GROUP BY d.id, d.name
        ),
        avg_hires AS (
            SELECT AVG(hired) AS avg_hired FROM hires_by_dept
        )
        SELECT department_id AS ID, department AS Department, hired AS Hired
        FROM hires_by_dept
        CROSS JOIN avg_hires
        WHERE hired > avg_hired
        ORDER BY hired DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("year", "INT64", year)]
        )
        df = bq.client.query(query, job_config=job_config).result().to_dataframe()
        return df.to_json(orient="records"), 200
    except Exception as e:
        logging.error(f"Departments_above_average error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

