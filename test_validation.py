from validation import validate_departments, validate_jobs, validate_hired_employees

# Casos de prueba
departments_tests = [
    {"id": "1", "name": "Ventas"},    # ✅ válido
    {"id": "2", "name": "NaN"},       # ❌ nombre inválido → DLQ
    {"id": "", "name": "Soporte"}     # ❌ id faltante → DLQ
]

jobs_tests = [
    {"id": "1", "name": "Developer"},  # ✅ válido
    {"id": "abc", "name": "QA"}        # ❌ id no entero → DLQ
]

hired_employees_tests = [
    {"id": "1", "name": "Juan", "datetime": "2020-01-01T12:00:00", "department_id": "1", "job_id": "2"},  # ✅ válido
    {"id": "2", "name": "NaN", "datetime": "2020-01-01T12:00:00", "department_id": "1", "job_id": "2"},   # ❌ nombre inválido
    {"id": "3", "name": "", "datetime": "fecha_mala", "department_id": "1", "job_id": "2"},            # ❌ fecha inválida
    {"id": "4", "name": "Luis", "datetime": "2020-01-01T12:00:00", "department_id": "abc", "job_id": "2"} # ❌ department_id no entero
]

def run_tests():
    print("\n--- Departments ---")
    for row in departments_tests:
        print(row, "=>", validate_departments(row))

    print("\n--- Jobs ---")
    for row in jobs_tests:
        print(row, "=>", validate_jobs(row))

    print("\n--- Hired Employees ---")
    for row in hired_employees_tests:
        print(row, "=>", validate_hired_employees(row))

if __name__ == "__main__":
    run_tests()
