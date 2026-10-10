"""Arranque de un solo comando: carga los datos solo si faltan.

`docker compose up -d --wait` ejecuta este script (servicio `ingest`) antes de levantar la API:
  - Si MongoDB ya tiene `spawns` completa (indice 2dsphere creado al final de la carga), no hace nada.
  - Si no, ejecuta el flujo de siempre: download.py -> clean.py -> load_mongo.py.
Ademas, si hubo carga o faltan las agregaciones, borra la marca /data/.agg_done para que el job de Spark las recalcule.

Variables de entorno: MONGO_URI, MONGO_DB, MARK_FILE (por defecto /data/.agg_done), FORCE_INGEST=1 (recarga siempre).
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
STEPS = ("download.py", "clean.py", "load_mongo.py")


def needs_load(spawns_count, index_names, catalog_count, force=False):
    """True si hay que ejecutar la ingesta. Funcion pura (se prueba sin MongoDB).

    load_mongo.py crea el indice 2dsphere al terminar de insertar: si existe, la carga anterior se completo."""
    if force:
        return True
    return not (spawns_count > 0 and "location_2dsphere" in index_names and catalog_count > 0)


def main():
    from pymongo import MongoClient

    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=5000)[os.environ.get("MONGO_DB", "pokemon")]
    mark = Path(os.environ.get("MARK_FILE", "/data/.agg_done"))

    n = db.spawns.estimated_document_count()
    load = needs_load(n, db.spawns.index_information().keys(), db.pokemon_catalog.estimated_document_count(),
                      os.environ.get("FORCE_INGEST") == "1")
    if load:
        print(f"[arranque] spawns tiene {n:,} documentos o la carga esta incompleta: se ejecuta la ingesta", flush=True)
        for script in STEPS:
            print(f"[arranque] python {script}", flush=True)
            subprocess.run([sys.executable, str(HERE / script)], check=True)
    else:
        print(f"[arranque] datos ya cargados ({n:,} documentos en spawns): se omite la ingesta", flush=True)

    # Las agregaciones de Spark dependen de spawns: si hubo carga o faltan, que se recalculen.
    if load or db.agg_grid.estimated_document_count() == 0:
        mark.unlink(missing_ok=True)
        print("[arranque] las agregaciones de Spark se recalcularan", flush=True)


if __name__ == "__main__":
    main()
