"""Carga el Parquet limpio en MongoDB por lotes, con GeoJSON valido e indice 2dsphere.

Cada particion del Parquet se carga en paralelo desde un worker de Dask (insert_many por lotes).
El documento usa el `_id` original de Mongo (24 hex): garantiza unicidad y vuelve la carga idempotente.

Coleccion `spawns`:
  _id, pokemonId, source,
  location {type: "Point", coordinates: [lng, lat]},     <- GeoJSON, orden [longitud, latitud]
  appeared_utc (Date), local_date ("YYYY-MM-DD"), local_hour (0-23), local_dow (0=lunes)
Coleccion `pokemon_catalog`: 151 pokemon de PokeAPI (nombre, tipos, estadisticas).

Variables de entorno: MONGO_URI (obligatoria), MONGO_DB, PARQUET_DIR, CATALOG_FILE, BATCH_SIZE,
DASK_SCHEDULER, DROP_FIRST (1 = vaciar la coleccion antes de cargar).
"""
import json
import os
import sys
import time
from pathlib import Path

COLLECTION = "spawns"
CATALOG_COLLECTION = "pokemon_catalog"


def to_document(rec):
    """Convierte un registro limpio (dict) en un documento GeoJSON para Mongo. Funcion pura."""
    return {
        "_id": rec["id"],
        "pokemonId": int(rec["pokemonId"]),
        "source": rec["source"],
        "location": {"type": "Point", "coordinates": [float(rec["lng"]), float(rec["lat"])]},
        "appeared_utc": rec["appeared_utc"],
        "local_date": rec["local_date"],
        "local_hour": int(rec["local_hour"]),
        "local_dow": int(rec["local_dow"]),
    }


def load_partition(path):
    """Se ejecuta en un worker: lee un archivo Parquet e inserta sus documentos por lotes."""
    import pandas as pd
    from pymongo import MongoClient
    from pymongo.errors import BulkWriteError

    batch_size = int(os.environ.get("BATCH_SIZE", "10000"))
    client = MongoClient(os.environ["MONGO_URI"])
    coll = client[os.environ.get("MONGO_DB", "pokemon")][COLLECTION]

    df = pd.read_parquet(path)
    df["local_date"] = df["appeared_local"].dt.strftime("%Y-%m-%d")
    df["appeared_utc"] = df["appeared_utc"].dt.to_pydatetime()
    cols = ["id", "pokemonId", "source", "lng", "lat", "appeared_utc", "local_date", "local_hour", "local_dow"]
    inserted = dup = 0
    batch = []
    for row in df[cols].itertuples(index=False, name=None):
        batch.append(to_document(dict(zip(cols, row))))
        if len(batch) >= batch_size:
            i, d = _flush(coll, batch, BulkWriteError)
            inserted, dup = inserted + i, dup + d
            batch = []
    if batch:
        i, d = _flush(coll, batch, BulkWriteError)
        inserted, dup = inserted + i, dup + d
    client.close()
    return {"archivo": Path(path).name, "filas": len(df), "insertados": inserted, "duplicados_id": dup}


def _flush(coll, batch, bulk_error):
    try:
        res = coll.insert_many(batch, ordered=False)
        return len(res.inserted_ids), 0
    except bulk_error as exc:
        errs = exc.details.get("writeErrors", [])
        if any(e.get("code") != 11000 for e in errs):  # solo se tolera clave duplicada
            raise
        return exc.details.get("nInserted", 0), len(errs)


def main():
    import dask
    from dask import delayed
    from distributed import Client
    from pymongo import ASCENDING, GEOSPHERE, MongoClient

    t0 = time.time()
    uri = os.environ["MONGO_URI"]
    db_name = os.environ.get("MONGO_DB", "pokemon")
    parquet_dir = Path(os.environ.get("PARQUET_DIR", "/data/clean/pokemon_spawns"))
    catalog_file = Path(os.environ.get("CATALOG_FILE", "/data/catalog/pokemon_catalog.json"))
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        sys.exit(f"No hay Parquet en {parquet_dir}. Ejecuta primero clean.py")

    mongo = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = mongo[db_name]
    if os.environ.get("DROP_FIRST", "1") == "1":
        db[COLLECTION].drop()
        print(f"Coleccion {COLLECTION} vaciada", flush=True)

    client = Client(os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786"))
    print(f"Dask: {len(client.scheduler_info()['workers'])} workers; {len(files)} particiones Parquet", flush=True)
    results = dask.compute(*[delayed(load_partition)(str(f)) for f in files])
    client.close()

    # catalogo PokeAPI (151 documentos)
    if catalog_file.exists():
        catalog = json.loads(catalog_file.read_text(encoding="utf-8"))
        db[CATALOG_COLLECTION].drop()
        db[CATALOG_COLLECTION].insert_many([{"_id": c["pokemonId"], **c} for c in catalog])
        print(f"Catalogo: {len(catalog)} pokemon", flush=True)

    # indices: 2dsphere (obligatorio) + pokemonId para filtros por especie
    coll = db[COLLECTION]
    coll.create_index([("location", GEOSPHERE)], name="location_2dsphere")
    coll.create_index([("pokemonId", ASCENDING)], name="pokemonId_1")

    report = {
        "particiones": list(results),
        "filas_parquet": sum(r["filas"] for r in results),
        "insertados": sum(r["insertados"] for r in results),
        "duplicados_id": sum(r["duplicados_id"] for r in results),
        "documentos_en_mongo": coll.estimated_document_count(),
        "indices": sorted(coll.index_information().keys()),
        "segundos": round(time.time() - t0, 1),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    mongo.close()


if __name__ == "__main__":
    main()
