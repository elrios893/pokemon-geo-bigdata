"""Constructores de consultas geoespaciales de MongoDB. Funciones puras (sin acceso a la base).

Siempre reciben parametros: nunca hay coordenadas, radios ni poligonos fijos en el codigo.
Recordatorio: GeoJSON usa el orden [longitud, latitud].
"""


def point(lng, lat):
    return {"type": "Point", "coordinates": [lng, lat]}


def species_clause(pokemon_id):
    """Un id -> igualdad; una lista de ids (filtro por tipo) -> $in."""
    return {"$in": list(pokemon_id)} if isinstance(pokemon_id, (list, tuple, set)) else pokemon_id


def near_filter(lng, lat, radius_m, pokemon_id=None):
    """$near: documentos dentro de radius_m metros, ordenados de mas cerca a mas lejos."""
    q = {"location": {"$near": {"$geometry": point(lng, lat), "$maxDistance": radius_m}}}
    if pokemon_id is not None:
        q["pokemonId"] = species_clause(pokemon_id)
    return q


def within_filter(polygon, pokemon_id=None):
    """$geoWithin: documentos dentro de un poligono GeoJSON (sin orden)."""
    q = {"location": {"$geoWithin": {"$geometry": polygon}}}
    if pokemon_id is not None:
        q["pokemonId"] = species_clause(pokemon_id)
    return q


def geonear_pipeline(lng, lat, radius_m, pokemon_id=None, limit=50):
    """$geoNear (agregacion): como $near pero devuelve la distancia calculada en `distance_m`."""
    stage = {
        "near": point(lng, lat),
        "distanceField": "distance_m",
        "maxDistance": radius_m,
        "spherical": True,
    }
    if pokemon_id is not None:
        stage["query"] = {"pokemonId": species_clause(pokemon_id)}
    return [{"$geoNear": stage}, {"$limit": limit}]
