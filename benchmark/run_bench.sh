#!/usr/bin/env bash
# Ejecuta el benchmark Dask vs Spark completo y deja el resultado en data/benchmark/results.jsonl.
# Requisitos: stack levantado (docker compose up -d) y Parquet limpio en data/clean (docker compose --profile ingest run --rm ingest).
# Uso: benchmark/run_bench.sh [--reset]    (--reset borra resultados anteriores)
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

OUT=data/benchmark/results.jsonl
if [ "${1:-}" = "--reset" ]; then rm -f "$OUT"; fi
mkdir -p data/benchmark

# Los workers de Spark montan ./data y el segundo worker es nuevo: asegurar que ambos esten arriba.
docker compose up -d spark-master spark-worker spark-worker-2 dask-scheduler dask-worker-1 dask-worker-2

echo "== Dask (1 y 2 workers; 2, 4 y 8 archivos) =="
docker compose --profile bench run --rm bench-dask

echo "== Spark (2 y 4 nucleos; 2, 4 y 8 archivos) =="
for cores in 2 4; do
  for size in 2 4 8; do
    echo "-- Spark: ${cores} nucleos, ${size} archivos --"
    docker compose --profile bench run --rm -e CORES_MAX="$cores" -e SIZE_FILES="$size" bench-spark 2>&1 | grep -E "^\{|Error|Exception" || true
  done
done
echo "Listo: $OUT"
