# Proyecto Migración PoC

Este proyecto implementa:
- **ETL histórico**: carga inicial de datos desde CSV a BigQuery.
- **API REST**: validación e ingestión de datos en tablas de BigQuery.
- **Backups y Restore**: exportar/importar tablas en formato Parquet (local o GCS).
- **Seguridad**: todos los endpoints protegidos con API Key.

---

## Requisitos

- Docker y Docker Compose instalados
- Archivo de credenciales de Google Cloud (`migracionpoc-d50b7889462e.json`)

---

## Configuración

1. Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

'''env
PROJECT_ID=migracionpoc
DATASET=migration_poc
API_KEY=APIKEY
DRY_RUN=true
'''

DRY_RUN=true Es para pruebas, no inserta datos en la base
DRY_RUN=false Es para produccion, inserta datos directamente en la base

## Ejecución

Levanta los servicios con:

docker compose up --build

## Seguridad

Todos los endpoints requieren un API Key en el header HTTP:

x-api-key: APIKEY

## Ejemplos de uso

1. Verificar que la API responde

curl -H "x-api-key: APIKEY" http://localhost:5000/

Respuesta esperada:
{ "message": "API de Migración funcionando" }

2. Insertar datos en departments

curl -X POST http://localhost:5000/ingest \
  -H "Content-Type: application/json" \
  -H "x-api-key: APIKEY" \
  -d '{
        "table": "departments",
        "records": [
          { "id": 1, "name": "HR" },
          { "id": 2, "name": "IT" }
        ]
      }'

Respuesta esperada:
{
  "inserted": 2,
  "errors": []
}

3. Backup de tabla a archivo local

curl -X POST http://localhost:5000/backup/departments \
  -H "Content-Type: application/json" \
  -H "x-api-key: APIKEY" \
  -d '{ "target": "local", "local_path": "/tmp/departments.parquet" }'

4. Restore desde archivo local

curl -X POST http://localhost:5000/restore/departments \
  -H "Content-Type: application/json" \
  -H "x-api-key: APIKEY" \
  -d '{ "source": "local", "local_path": "/tmp/departments.parquet" }'

5. Contrataciones por trimestre (hired_by_quarter)

curl -H "x-api-key: supersecreta123" \
     http://localhost:5000/analytics/hired_by_quarter/2025

Respuesta esperada:

[
  {
    "department": "HR",
    "job": "Manager",
    "Q1": 2,
    "Q2": 1,
    "Q3": 0,
    "Q4": 3
  },
  {
    "department": "IT",
    "job": "Developer",
    "Q1": 5,
    "Q2": 2,
    "Q3": 1,
    "Q4": 4
  }
]

6. Departamentos con contrataciones sobre el promedio (departments_above_average)

curl -H "x-api-key: supersecreta123" \
     http://localhost:5000/analytics/departments_above_average/2025

Respuesta esperada:

[
  { "ID": 2, "Department": "IT", "Hired": 12 },
  { "ID": 1, "Department": "HR", "Hired": 6 }
]


## Dashboard de informacion

Una vez esten corriendo los servicios con docker-compose up, puedes visitar el dashboard de datos de contratación en
http://localhost:4000/

## Parar los servicios

docker compose down


