from dateutil import parser

def clean_str(val):
    if val is None:
        return None
    val = str(val).strip()

    if val.lower() in {"nan", "none", "null", ""}:
        return None
    return val


def validate_departments(row):
    row["id"] = clean_str(row.get("id"))
    row["name"] = clean_str(row.get("name"))

    if not row["id"] or not row["name"]:
        return None, "Campos obligatorios faltantes"
    try:
        row["id"] = int(row["id"])
    except ValueError:
        return None, "id no es entero"
    return row, None

def validate_jobs(row):
    row["id"] = clean_str(row.get("id"))
    row["name"] = clean_str(row.get("name"))

    if not row["id"] or not row["name"]:
        return None, "Campos obligatorios faltantes"
    try:
        row["id"] = int(row["id"])
    except ValueError:
        return None, "id no es entero"
    return row, None

def validate_hired_employees(row):
    row["id"] = clean_str(row.get("id"))
    row["name"] = clean_str(row.get("name"))
    row["datetime"] = clean_str(row.get("datetime"))
    row["department_id"] = clean_str(row.get("department_id"))
    row["job_id"] = clean_str(row.get("job_id"))

    # Validaciones
    if not all([row["id"], row["name"], row["datetime"], row["department_id"], row["job_id"]]):
        return None, "Campos obligatorios faltantes"

    try:
        row["id"] = int(row["id"])
        row["department_id"] = int(row["department_id"])
        row["job_id"] = int(row["job_id"])
    except ValueError:
        return None, "Valores de ID no son enteros"

    try:
        row["hired_timestamp"] = parser.isoparse(row["datetime"])
    except Exception:
        return None, "Fecha inválida"

    # Retornar solo las columnas necesarias para la tabla
    return {
        "id": row["id"],
        "name": row["name"],
        "hired_timestamp": row["hired_timestamp"].isoformat(),
        "department_id": row["department_id"],
        "job_id": row["job_id"]
    }, None
