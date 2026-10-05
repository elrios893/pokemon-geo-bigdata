"""Benchmark Dask: agregacion por celda de grilla (0.01 grados) sobre el Parquet limpio.

Operacion (identica en benchmark/spark_grid.py): por celda (cx, cy) = floor(lng/0.01), floor(lat/0.01),
contar avistamientos y especies distintas. El resultado completo vuelve al driver.

Configuraciones: 1 worker (2 hilos) y 2 workers (4 hilos); tamanos: 2, 4 y 8 archivos Parquet (~0.56, 1.1, 2.24 M filas).
Para cada (workers, tamano) se reinician los workers (memoria limpia), se hace 1 corrida de calentamiento
(no cuenta) y REPS corridas medidas. La memoria es el pico de RSS sumado de los workers que participan
(muestreo cada 0.2 s, incluye el estado base del proceso).

Variables: DASK_SCHEDULER, CLEAN_DIR, OUT, REPS (3), SIZES ("2,4,8"), WORKERS ("1,2").
"""
import glob
import json
import os
import threading
import time
from pathlib import Path

import dask
import dask.dataframe as dd
import numpy as np
from distributed import Client

SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
FILES = sorted(glob.glob(os.environ.get("CLEAN_DIR", "/data/clean") + "/pokemon_spawns/*.parquet"))
OUT = Path(os.environ.get("OUT", "/data/benchmark/results.jsonl"))
REPS = int(os.environ.get("REPS", "3"))
SIZES = [int(x) for x in os.environ.get("SIZES", "2,4,8").split(",")]
WORKERS = [int(x) for x in os.environ.get("WORKERS", "1,2").split(",")]
GRID = 0.01


def _rss():
    import psutil
    return psutil.Process().memory_info().rss


def with_cells(p):
    p = p.copy()
    p["cx"] = np.floor(p["lng"] / GRID).astype("int64")
    p["cy"] = np.floor(p["lat"] / GRID).astype("int64")
    return p[["cx", "cy", "pokemonId"]]


def run_once(files, addrs):
    """Conteo por celda y especies distintas por celda. `nunique` en el groupby de Dask (agregacion con conjuntos de
    Python) resulto ~10 veces mas lento; se usa lo mismo que hace Spark internamente: deduplicar (celda, especie) y contar."""
    df = dd.read_parquet(files, columns=["pokemonId", "lng", "lat"])
    meta = with_cells(df._meta_nonempty).iloc[:0]
    cells = df.map_partitions(with_cells, meta=meta)
    size = cells.groupby(["cx", "cy"]).size()
    species = cells.drop_duplicates(split_out=8).groupby(["cx", "cy"]).size()
    t0 = time.perf_counter()
    res_size, res_species = dask.compute(size, species, workers=addrs, allow_other_workers=False)
    seconds = time.perf_counter() - t0
    return seconds, {"cells": int(len(res_size)), "total": int(res_size.sum()), "species_sum": int(res_species.sum())}


class Sampler(threading.Thread):
    def __init__(self, client, addrs):
        super().__init__(daemon=True)
        self.client, self.addrs, self.peak, self.stop = client, addrs, 0, threading.Event()

    def run(self):
        while not self.stop.is_set():
            try:
                self.peak = max(self.peak, sum(self.client.run(_rss, workers=self.addrs).values()))
            except Exception:
                pass
            time.sleep(0.2)


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    client = Client(SCHEDULER)
    client.wait_for_workers(2)
    for n_workers in WORKERS:
        for size in SIZES:
            client.restart()
            client.wait_for_workers(2)
            addrs = sorted(client.scheduler_info()["workers"])[:n_workers]
            files = FILES[:size]
            sampler = Sampler(client, addrs)
            sampler.start()
            seconds_list, check = [], None
            for rep in range(REPS + 1):          # rep 0 = calentamiento
                seconds, check = run_once(files, addrs)
                rec = {"engine": "dask", "workers": n_workers, "cores": 2 * n_workers, "files": size,
                       "rep": rep, "warmup": rep == 0, "seconds": round(seconds, 3), **check}
                with OUT.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec) + "\n")
                print(rec, flush=True)
                if rep:
                    seconds_list.append(seconds)
            sampler.stop.set()
            sampler.join()
            mem = {"engine": "dask", "workers": n_workers, "cores": 2 * n_workers, "files": size, "kind": "memoria",
                   "peak_rss_mb": round(sampler.peak / 2**20, 1), "total": check["total"]}
            with OUT.open("a", encoding="utf-8") as f:
                f.write(json.dumps(mem) + "\n")
            print(mem, flush=True)
    client.close()


if __name__ == "__main__":
    main()
