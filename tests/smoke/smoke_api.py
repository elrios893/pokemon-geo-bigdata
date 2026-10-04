"""Pruebas de humo de la API contra un MongoDB real (el stack efimero de CI).

Siembra un conjunto minimo y conocido de documentos (no depende del dataset), crea el indice 2dsphere
y verifica cada endpoint. Sale con codigo 1 si falla cualquier comprobacion (eso bloquea el despliegue).

Variables: API_URL (http://api:5000), MONGO_URI, MONGO_DB. Solo usa la biblioteca estandar y pymongo.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

from pymongo import GEOSPHERE, MongoClient

API = os.environ.get("API_URL", "http://api:5000")
fallos = []


def get(path, body=None):
    req = urllib.request.Request(API + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"} if body is not None else {},
                                 method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(nombre, condicion, detalle=""):
    print(("  OK    " if condicion else "  FALLA ") + nombre + (f"  -> {detalle}" if not condicion else ""), flush=True)
    if not condicion:
        fallos.append(nombre)


def sembrar():
    db = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=15000)[os.environ.get("MONGO_DB", "pokemon_ci")]
    for c in ("spawns", "pokemon_catalog", "agg_hotspots", "agg_grid", "agg_time", "agg_species", "agg_species_hour"):
        db[c].delete_many({})
    db.pokemon_catalog.insert_many([
        {"pokemonId": 16, "name": "pidgey", "types": ["normal", "flying"]},
        {"pokemonId": 19, "name": "rattata", "types": ["normal"]},
    ])

    def sp(i, pid, lng, lat):
        return {"_id": f"s{i}", "pokemonId": pid, "source": "TEST",
                "location": {"type": "Point", "coordinates": [lng, lat]},
                "appeared_utc": datetime(2016, 9, 21, 12, 0, i), "local_date": "2016-09-21",
                "local_hour": 7, "local_dow": 2}
    db.spawns.insert_many([
        sp(1, 16, -73.9855, 40.7580),      # en el punto de consulta (Times Square)
        sp(2, 16, -73.9850, 40.7585),      # ~70 m
        sp(3, 19, -73.9840, 40.7590),      # ~170 m
        sp(4, 19, 2.3522, 48.8566),        # Paris: fuera de cualquier consulta sobre Nueva York
    ])
    db.spawns.create_index([("location", GEOSPHERE)])
    db.agg_hotspots.insert_one({"_id": 1, "rank": 1, "cell_id": "-7399_4075", "count": 3, "puntos_distintos": 3})
    db.agg_grid.insert_one({"_id": "-7399_4075", "cell_id": "-7399_4075", "count": 3, "lat_c": 40.755, "lng_c": -73.985})
    db.agg_time.insert_one({"_id": "hour:07", "granularity": "hour", "key": "07", "count": 4})
    db.agg_species.insert_one({"_id": 16, "pokemonId": 16, "name": "pidgey", "types": ["normal", "flying"], "count": 2})
    db.agg_species_hour.insert_one({"_id": "16:7", "pokemonId": 16, "local_hour": 7, "count": 2})


def esperar_api():
    for _ in range(30):
        try:
            if get("/health")[0] == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    sys.exit("La API no respondio /health en 30 s")


def main():
    sembrar()
    esperar_api()
    ts = "lat=40.758&lng=-73.9855"
    caja = {"type": "Polygon", "coordinates": [[[-74.0, 40.74], [-73.97, 40.74], [-73.97, 40.77], [-74.0, 40.77], [-74.0, 40.74]]]}

    s, b = get("/health")
    check("GET /health", s == 200 and b.get("status") == "ok", b)

    s, b = get(f"/near?{ts}&radius=500")
    check("GET /near (3 dentro de 500 m, orden por cercania)", s == 200 and b["count"] == 3 and b["results"][0]["id"] == "s1", b)
    s, b = get(f"/near?{ts}&radius=500&pokemonId=16")
    check("GET /near filtra por especie y enriquece con el catalogo",
          s == 200 and b["count"] == 2 and b["results"][0]["name"] == "pidgey", b)
    s, b = get(f"/near?{ts}&radius=100")
    check("GET /near respeta el radio (2 dentro de 100 m)", s == 200 and b["count"] == 2, b)

    s, b = get("/within?count=true", caja)
    check("POST /within (3 en Manhattan, Paris queda fuera) + total", s == 200 and b["count"] == 3 and b["total"] == 3, b)
    s, b = get("/within?pokemonId=19&count=true", caja)
    check("POST /within filtra por especie", s == 200 and b["count"] == 1 and b["total"] == 1, b)
    s, b = get("/within?limit=2&count=true", caja)
    check("POST /within trunca con limit y reporta el total", s == 200 and b["truncated"] and b["count"] == 2 and b["total"] == 3, b)

    s, b = get(f"/geonear?{ts}&radius=500")
    d = [r["distance_m"] for r in b.get("results", [])]
    check("GET /geonear (distancias calculadas y ascendentes)", s == 200 and len(d) == 3 and d == sorted(d) and d[0] < 1 and 50 < d[1] < 90, b)

    s, b = get("/stats/hotspots")
    check("GET /stats/hotspots", s == 200 and b["results"][0]["puntos_distintos"] == 3, b)
    s, b = get("/stats/grid?min_lat=40&max_lat=41")
    check("GET /stats/grid con filtro", s == 200 and b["count"] == 1, b)
    s, b = get("/stats/time?granularity=hour")
    check("GET /stats/time", s == 200 and b["results"][0]["count"] == 4, b)
    s, b = get("/stats/species")
    check("GET /stats/species", s == 200 and b["results"][0]["name"] == "pidgey", b)
    s, b = get("/stats/species/16/hours")
    check("GET /stats/species/16/hours", s == 200 and b["name"] == "pidgey" and b["results"][0]["local_hour"] == 7, b)

    for nombre, ruta, cuerpo in (("sin parametros", "/near", None), ("radio excesivo", f"/near?{ts}&radius=999999", None),
                                 ("poligono invalido", "/within", {"type": "Point", "coordinates": [0, 0]}),
                                 ("granularidad invalida", "/stats/time?granularity=anio", None)):
        s, b = get(ruta, cuerpo)
        check(f"entrada invalida ({nombre}) -> 400 con mensaje", s == 400 and "error" in b, (s, b))
    s, b = get("/no-existe")
    check("ruta inexistente -> 404 JSON", s == 404 and "error" in b, (s, b))

    print(f"\n{'FALLARON ' + str(len(fallos)) + ': ' + ', '.join(fallos) if fallos else 'Todas las pruebas de humo pasaron'}")
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
