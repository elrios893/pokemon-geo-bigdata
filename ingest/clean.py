"""Limpieza distribuida con Dask: NDJSON crudo -> Parquet limpio, con conteo por regla.

Lee cada archivo por bloques de lineas (particiones), aplica cleaning.clean_record en los workers,
deduplica globalmente y escribe Parquet en CLEAN_DIR. Guarda ademas clean_stats.json con los
conteos antes/despues de cada regla (insumo directo del informe).

Variables de entorno (todas opcionales):
  DASK_SCHEDULER  tcp://dask-scheduler:8786
  RAW_DIR         /data/raw            (catchemall_part*.json)
  CLEAN_DIR       /data/clean
  SAMPLE_STRIDE   4                    muestreo sistematico: se toma 1 de cada K lineas (1 = todo, ~2.2 M con K=4)
  CHUNK_LINES     200000               lineas por particion (se ajusta a multiplo de K)
"""
import itertools
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import dask
import dask.dataframe as dd
import pandas as pd
from dask import delayed
from distributed import Client

from cleaning import COLUMNS, clean_record

SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
RAW_DIR = Path(os.environ.get("RAW_DIR", "/data/raw"))
CLEAN_DIR = Path(os.environ.get("CLEAN_DIR", "/data/clean"))
STRIDE = max(1, int(os.environ.get("SAMPLE_STRIDE", "4")))
CHUNK_LINES = int(os.environ.get("CHUNK_LINES", "200000")) // STRIDE * STRIDE or STRIDE
SPLIT_OUT = int(os.environ.get("SPLIT_OUT", "8"))
DEDUP_KEY = ["pokemonId", "lng", "lat", "appeared_utc"]

META = pd.DataFrame({
    "id": pd.Series(dtype="object"),
    "pokemonId": pd.Series(dtype="int16"),
    "source": pd.Series(dtype="object"),
    "lng": pd.Series(dtype="float64"),
    "lat": pd.Series(dtype="float64"),
    "appeared_utc": pd.Series(dtype="datetime64[ns]"),
    "appeared_local": pd.Series(dtype="datetime64[ns]"),
    "local_hour": pd.Series(dtype="int8"),
    "local_dow": pd.Series(dtype="int8"),
})


def count_lines(path):
    n = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 22), b""):
            n += block.count(b"\n")
    return n


def process_chunk(path, start, end, stride=1):
    """Se ejecuta en un worker: lee 1 de cada `stride` lineas de [start, end) de path y aplica las reglas.

    `start` es multiplo de `stride`, asi la seleccion equivale a (numero de linea % stride == 0) en todo el archivo."""
    from cleaning import clean_record as rule  # import local: el worker usa su propia copia

    stats = Counter()
    rows = []
    with open(path, "rb") as f:
        for raw in itertools.islice(f, start, end, stride):
            raw = raw.strip()
            if not raw:
                continue
            stats["leidas"] += 1
            try:
                doc = json.loads(raw)
            except ValueError:
                stats["json_invalido"] += 1
                continue
            rec, why = rule(doc)
            if why:
                stats[why] += 1
            else:
                rows.append(rec)
    stats["tras_reglas"] = len(rows)
    df = pd.DataFrame(rows, columns=COLUMNS) if rows else META.copy()
    df = df.astype(META.dtypes.to_dict())
    return df, dict(stats)


def main():
    t0 = time.time()
    files = sorted(RAW_DIR.glob("catchemall_part*.json"))
    if not files:
        sys.exit(f"No hay catchemall_part*.json en {RAW_DIR}. Ejecuta primero ingest/download.py")

    client = Client(SCHEDULER)
    n_workers = len(client.scheduler_info()["workers"])
    print(f"Dask: {n_workers} workers en {SCHEDULER}", flush=True)

    parts, stats_parts, plan = [], [], []
    for f in files:
        total = count_lines(f)
        plan.append({"archivo": f.name, "lineas_archivo": total, "lineas_tomadas": -(-total // STRIDE)})
        for start in range(0, total, CHUNK_LINES):
            end = min(start + CHUNK_LINES, total)
            res = delayed(process_chunk, nout=2)(str(f), start, end, STRIDE)
            parts.append(res[0])
            stats_parts.append(res[1])
    print(f"Plan: {len(parts)} particiones ({CHUNK_LINES:,} lineas c/u); muestra={'completa' if STRIDE == 1 else f'1 de cada {STRIDE} lineas'}", flush=True)

    df = dd.from_delayed(parts, meta=META)
    # split_out reparte la deduplicacion (shuffle por clave) en varias particiones de salida;
    # sin esto Dask junta todo en una sola y un solo worker cargaria con el dataset completo.
    dedup = df.drop_duplicates(subset=DEDUP_KEY, split_out=SPLIT_OUT)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    out = CLEAN_DIR / "pokemon_spawns"
    # una sola pasada: estadisticas por regla + escritura del Parquet deduplicado
    write = dedup.to_parquet(str(out), engine="pyarrow", write_index=False, overwrite=True, compute=False)
    stat_list, _ = dask.compute(stats_parts, write)

    total = Counter()
    for s in stat_list:
        total.update(s)
    final_rows = int(dd.read_parquet(str(out)).shape[0].compute())
    after_rules = total["tras_reglas"]

    report = {
        "muestreo_una_de_cada": STRIDE,
        "archivos": plan,
        "leidas": total["leidas"],
        "descartadas_por_regla": {
            "json_invalido": total["json_invalido"],
            "coordenadas_invalidas": total["coordenadas_invalidas"],
            "sin_pokemonId": total["sin_pokemonId"],
            "fecha_invalida": total["fecha_invalida"],
        },
        "tras_reglas_por_registro": after_rules,
        "duplicados_eliminados": after_rules - final_rows,
        "filas_finales": final_rows,
        "particiones_parquet": len(list(out.glob("*.parquet"))),
        "segundos": round(time.time() - t0, 1),
    }
    (CLEAN_DIR / "clean_stats.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    client.close()


if __name__ == "__main__":
    main()
