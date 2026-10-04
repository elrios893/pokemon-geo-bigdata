from datetime import datetime

from flask import Blueprint, current_app, jsonify, request

from . import queries
from .validation import (ValidationError, parse_flag, parse_limit, parse_lat_lng, parse_pokemon_id,
                         parse_polygon, parse_radius)

bp = Blueprint("api", __name__)
MAX_TIME_MS = 8000


def _db():
    return current_app.config["DB"]


def _catalog():
    """Catalogo de 151 pokemon, cargado una vez (nombre y tipos por pokemonId)."""
    cache = current_app.config.get("_CATALOG")
    if cache is None:
        cache = {c["pokemonId"]: c for c in _db().pokemon_catalog.find({})}
        current_app.config["_CATALOG"] = cache
    return cache


def _spawn(doc):
    cat = _catalog().get(doc.get("pokemonId"), {})
    out = {
        "id": doc["_id"],
        "pokemonId": doc.get("pokemonId"),
        "name": cat.get("name"),
        "types": cat.get("types"),
        "source": doc.get("source"),
        "location": doc.get("location"),
        "appeared_utc": _iso(doc.get("appeared_utc")),
        "local_date": doc.get("local_date"),
        "local_hour": doc.get("local_hour"),
        "local_dow": doc.get("local_dow"),
    }
    if "distance_m" in doc:
        out["distance_m"] = round(doc["distance_m"], 1)
    return out


def _iso(value):
    return value.isoformat() + "Z" if isinstance(value, datetime) else value


def _strip(doc, drop=("_id",)):
    return {k: v for k, v in doc.items() if k not in drop}


@bp.errorhandler(ValidationError)
def _bad_request(exc):
    return jsonify(error=str(exc)), 400


@bp.get("/near")
def near():
    """GET /near?lat=&lng=&radius=[&pokemonId=][&limit=]  ->  avistamientos cercanos ($near, por distancia)."""
    lat, lng = parse_lat_lng(request.args)
    radius = parse_radius(request.args)
    pid = parse_pokemon_id(request.args)
    limit = parse_limit(request.args)
    cur = _db().spawns.find(queries.near_filter(lng, lat, radius, pid)).limit(limit).max_time_ms(MAX_TIME_MS)
    results = [_spawn(d) for d in cur]
    return jsonify(query={"lat": lat, "lng": lng, "radius_m": radius, "pokemonId": pid},
                   count=len(results), results=results)


@bp.post("/within")
def within():
    """POST /within  (cuerpo: GeoJSON Polygon o Feature)[?pokemonId=][&limit=][&count=true]  ->  avistamientos dentro ($geoWithin).

    Con count=true se agrega `total` (count_documents sobre el mismo filtro): `results` esta limitado, el total no."""
    polygon = parse_polygon(request.get_json(silent=True))
    pid = parse_pokemon_id(request.args)
    limit = parse_limit(request.args)
    want_total = parse_flag(request.args, "count")
    flt = queries.within_filter(polygon, pid)
    cur = _db().spawns.find(flt).limit(limit + 1).max_time_ms(MAX_TIME_MS)
    docs = list(cur)
    truncated = len(docs) > limit
    results = [_spawn(d) for d in docs[:limit]]
    body = {"query": {"pokemonId": pid, "vertices": len(polygon["coordinates"][0])},
            "count": len(results), "truncated": truncated, "results": results}
    if want_total:
        body["total"] = _db().spawns.count_documents(flt, maxTimeMS=MAX_TIME_MS)
    return jsonify(body)


@bp.get("/geonear")
def geonear():
    """GET /geonear?lat=&lng=&radius=[&pokemonId=][&limit=]  ->  como /near pero con distance_m ($geoNear)."""
    lat, lng = parse_lat_lng(request.args)
    radius = parse_radius(request.args)
    pid = parse_pokemon_id(request.args)
    limit = parse_limit(request.args)
    pipeline = queries.geonear_pipeline(lng, lat, radius, pid, limit)
    results = [_spawn(d) for d in _db().spawns.aggregate(pipeline, maxTimeMS=MAX_TIME_MS)]
    return jsonify(query={"lat": lat, "lng": lng, "radius_m": radius, "pokemonId": pid},
                   count=len(results), results=results)


# ---------------- resultados de Spark (colecciones agg_*) ----------------

@bp.get("/stats/hotspots")
def stats_hotspots():
    """GET /stats/hotspots[?limit=]  ->  celdas mas densas con sus especies dominantes."""
    limit = parse_limit(request.args, default=20, maximum=200)
    docs = _db().agg_hotspots.find({}).sort("rank", 1).limit(limit)
    return _listing(docs)


@bp.get("/stats/grid")
def stats_grid():
    """GET /stats/grid[?limit=][&min_lat=&max_lat=&min_lng=&max_lng=]  ->  celdas de la grilla por conteo."""
    limit = parse_limit(request.args, default=50, maximum=500)
    flt = {}
    for lo, hi, field in (("min_lat", "max_lat", "lat_c"), ("min_lng", "max_lng", "lng_c")):
        rng = {}
        for key, op in ((lo, "$gte"), (hi, "$lte")):
            raw = request.args.get(key)
            if raw not in (None, ""):
                try:
                    rng[op] = float(raw)
                except ValueError:
                    raise ValidationError(f"'{key}' debe ser numerico")
        if rng:
            flt[field] = rng
    docs = _db().agg_grid.find(flt).sort("count", -1).limit(limit).max_time_ms(MAX_TIME_MS)
    return _listing(docs)


@bp.get("/stats/time")
def stats_time():
    """GET /stats/time?granularity=hour|dow|day  ->  conteos por hora local, dia de la semana (0=lunes) o fecha."""
    g = request.args.get("granularity", "hour")
    if g not in ("hour", "dow", "day"):
        raise ValidationError("'granularity' debe ser hour, dow o day")
    docs = _db().agg_time.find({"granularity": g}).sort("key", 1)
    return _listing(docs, extra={"granularity": g})


@bp.get("/stats/species")
def stats_species():
    """GET /stats/species[?limit=]  ->  especies por numero de avistamientos."""
    limit = parse_limit(request.args, default=20, maximum=151)
    return _listing(_db().agg_species.find({}).sort("count", -1).limit(limit))


@bp.get("/stats/species/<int:pokemon_id>/hours")
def stats_species_hours(pokemon_id):
    """GET /stats/species/<id>/hours  ->  a que horas locales aparece una especie."""
    if not 1 <= pokemon_id <= 151:
        raise ValidationError("el id de la especie debe estar entre 1 y 151")
    docs = _db().agg_species_hour.find({"pokemonId": pokemon_id}).sort("local_hour", 1)
    cat = _catalog().get(pokemon_id, {})
    return _listing(docs, extra={"pokemonId": pokemon_id, "name": cat.get("name")})


def _listing(docs, extra=None):
    results = [_strip(d) for d in docs]
    body = dict(extra or {})
    body.update(count=len(results), results=results)
    return jsonify(body)
