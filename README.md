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

## Parar los servicios

docker compose down

