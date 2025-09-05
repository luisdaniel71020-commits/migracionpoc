# Imagen base ligera con Python 3.10
FROM python:3.10-slim

# Evitar que Python guarde .pyc y habilitar logs inmediatos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Crear directorio de trabajo en el contenedor
WORKDIR /app

# Copiar requirements
COPY src/requirements.txt ./requirements.txt

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt db-dtypes

# Copiar el código fuente y datos
COPY src/ ./src
COPY data/ ./data

# Definir directorio por defecto
WORKDIR /app/src

# CMD neutro
CMD ["python", "etl_historico.py"]
