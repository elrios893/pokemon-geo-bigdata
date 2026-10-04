"""Benchmark Spark: la misma agregacion por celda de grilla que benchmark/dask_grid.py.

Una ejecucion = una aplicacion Spark (memoria limpia) con CORES_MAX nucleos (2 = 1 ejecutor, 4 = 2 ejecutores,
ambos de 2 nucleos y 768 MB) sobre los primeros SIZE_FILES archivos Parquet. Hace 1 corrida de calentamiento
(no cuenta) y REPS corridas medidas; el resultado completo vuelve al driver (collect), como en Dask.
La memoria es el pico de RSS del arbol de procesos de los ejecutores (metricas de Spark, REST del driver).

Variables: CORES_MAX (2), SIZE_FILES (8), REPS (3), CLEAN_DIR, OUT.
"""
import glob
import json
import os
import time
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, ShortType, StructField, StructType

CORES_MAX = int(os.environ.get("CORES_MAX", "2"))
SIZE = int(os.environ.get("SIZE_FILES", "8"))
REPS = int(os.environ.get("REPS", "3"))
FILES = sorted(glob.glob(os.environ.get("CLEAN_DIR", "/data/clean") + "/pokemon_spawns/*.parquet"))[:SIZE]
OUT = Path(os.environ.get("OUT", "/data/benchmark/results.jsonl"))
GRID = 0.01
# Esquema explicito: el Parquet limpio trae timestamps en nanosegundos (pandas/Dask) y Spark 3.5 no los lee
# ("Illegal Parquet type: INT64 (TIMESTAMP(NANOS))"). Pidiendo solo estas 3 columnas no se toca esa columna.
SCHEMA = StructType([StructField("pokemonId", ShortType()), StructField("lng", DoubleType()),
                     StructField("lat", DoubleType())])


def append(rec):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
    print(rec, flush=True)


def executors_peak_rss_mb(spark):
    sc = spark.sparkContext
    url = f"http://localhost:4040/api/v1/applications/{sc.applicationId}/executors"
    with urllib.request.urlopen(url, timeout=10) as r:
        execs = [e for e in json.load(r) if e["id"] != "driver"]
    peak = sum(e.get("peakMemoryMetrics", {}).get("ProcessTreeJVMRSSMemory", 0) for e in execs)
    return round(peak / 2**20, 1), len(execs)


def main():
    spark = (SparkSession.builder.appName(f"bench-grid-{CORES_MAX}c-{SIZE}f")
             .config("spark.cores.max", CORES_MAX)
             .config("spark.executor.cores", 2)
             .config("spark.executor.memory", "768m")
             .config("spark.sql.shuffle.partitions", 8)
             .config("spark.executor.processTreeMetrics.enabled", "true")
             .config("spark.executor.metrics.pollingInterval", "100ms")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    check = None
    for rep in range(REPS + 1):                   # rep 0 = calentamiento
        df = spark.read.schema(SCHEMA).parquet(*FILES)
        g = (df.groupBy(F.floor(F.col("lng") / GRID).alias("cx"), F.floor(F.col("lat") / GRID).alias("cy"))
               .agg(F.count("*").alias("size"), F.countDistinct("pokemonId").alias("nunique")))
        t0 = time.perf_counter()
        rows = g.collect()
        seconds = time.perf_counter() - t0
        check = {"cells": len(rows), "total": sum(r["size"] for r in rows), "species_sum": sum(r["nunique"] for r in rows)}
        append({"engine": "spark", "workers": CORES_MAX // 2, "cores": CORES_MAX, "files": SIZE, "rep": rep,
                "warmup": rep == 0, "seconds": round(seconds, 3), **check})
    time.sleep(1.5)                               # deja que los ejecutores reporten su ultimo pico
    peak, n_exec = executors_peak_rss_mb(spark)
    append({"engine": "spark", "workers": CORES_MAX // 2, "cores": CORES_MAX, "files": SIZE, "kind": "memoria",
            "peak_rss_mb": peak, "ejecutores": n_exec, "total": check["total"]})
    spark.stop()


if __name__ == "__main__":
    main()
