from flask import Flask, request, jsonify
from bq_client import BigQueryClient
from validation import validate_departments, validate_jobs, validate_hired_employees
import os
from functools import wraps

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
    return jsonify({"message": "API de Migración funcionando"})

@app.route("/ingest", methods=["POST"])
@require_api_key
def ingest_data():
    try:
        payload = request.get_json(force=True)

        if not payload or "table" not in payload or "records" not in payload:
            return jsonify({"error": "El request debe incluir 'table' y 'records'"}), 400

        table = payload["table"]
        records = payload["records"]

        # Validar tabla soportada
        if table not in VALIDATORS:
            return jsonify({"error": f"Tabla '{table}' no soportada"}), 400

        # Validar tamaño del lote
        if not isinstance(records, list) or len(records) == 0:
            return jsonify({"error": "Debe enviar entre 1 y 1000 registros"}), 400
        if len(records) > 1000:
            return jsonify({"error": "Máximo 1000 registros por request"}), 400

        validator = VALIDATORS[table]
        valid_data = []
        errors = []

        # Validar cada registro contra el diccionario
        for i, record in enumerate(records, start=1):
            try:
                validated, error = validator(record)
                if error:
                    errors.append({"index": i, "record": record, "error": error})
                    bq.insert_dlq(table, record, error)  # guardar en DLQ
                else:
                    valid_data.append(validated)
            except Exception as e:
                error_msg = str(e)
                errors.append({"index": i, "record": record, "error": error_msg})
                bq.insert_dlq(table, record, error_msg)

        # Respuesta
        response = {"inserted": 0, "errors": errors}

        if valid_data:
            inserted_count = bq.insert_rows(table, valid_data)
            response["inserted"] = inserted_count

        return jsonify(response), (200 if valid_data else 400)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/backup/<table_name>", methods=["POST"])
@require_api_key
def backup_table(table_name):
    body = request.get_json(silent=True) or {}
    target = body.get("target", "local")
    fmt = body.get("format", "PARQUET").upper()
    try:
        if target == "gcs":
            gcs_uri = body["gcs_uri"]
            job = bq.export_table_to_gcs(table_name, gcs_uri, file_format=fmt)
            return jsonify({"status": "ok", "job": str(job.job_id)}), 200
        else:
            local_path = body.get("local_path", f"/tmp/{table_name}.parquet")
            path = bq.export_table_to_local_parquet(table_name, local_path)
            return jsonify({"status": "ok", "path": path}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/restore/<table_name>", methods=["POST"])
@require_api_key
def restore_table(table_name):
    body = request.get_json(force=True)
    source = body.get("source", "local")
    fmt = body.get("format", "PARQUET").upper()
    write_disp = body.get("write_disposition", "WRITE_TRUNCATE")
    try:
        if source == "gcs":
            gcs_uri = body["gcs_uri"]
            job = bq.restore_table_from_gcs(table_name, gcs_uri, source_format=fmt, write_disposition=write_disp)
            return jsonify({"status": "ok", "job": str(job.job_id)}), 200
        else:
            local_path = body["local_path"]
            job = bq.restore_table_from_local_file(table_name, local_path, source_format=fmt, write_disposition=write_disp)
            return jsonify({"status": "ok", "job": str(job.job_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
