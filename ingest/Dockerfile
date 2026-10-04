# Imagen unica para scheduler, workers y cliente de Dask (las versiones deben coincidir).
FROM python:3.11-slim

WORKDIR /app
# Los workers de Dask arrancan con `dask worker` (sys.path no incluye /app): sin esto no importan cleaning.py
ENV PYTHONPATH=/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
