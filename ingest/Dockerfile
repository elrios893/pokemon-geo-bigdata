# Imagen unica para scheduler, workers y cliente de Dask (las versiones deben coincidir).
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
