import pytest

from app import queries
from app.validation import (MAX_POLYGON_VERTICES, MAX_RADIUS_M, ValidationError, parse_lat_lng,
                            parse_limit, parse_pokemon_id, parse_polygon, parse_radius)

RING = [[-74.02, 40.70], [-73.93, 40.70], [-73.93, 40.80], [-74.02, 40.80], [-74.02, 40.70]]


# ---------- validacion ----------
def test_lat_lng_validos():
    assert parse_lat_lng({"lat": "40.7", "lng": "-73.9"}) == (40.7, -73.9)


@pytest.mark.parametrize("args", [
    {}, {"lat": "40"}, {"lng": "-73"}, {"lat": "x", "lng": "1"}, {"lat": "91", "lng": "0"},
    {"lat": "-91", "lng": "0"}, {"lat": "0", "lng": "181"}, {"lat": "nan", "lng": "0"}, {"lat": "inf", "lng": "0"},
])
def test_lat_lng_invalidos(args):
    with pytest.raises(ValidationError):
        parse_lat_lng(args)


def test_radio():
    assert parse_radius({"radius": "500"}) == 500.0
    assert parse_radius({}, default=300) == 300.0
    for bad in ({}, {"radius": "0"}, {"radius": "-5"}, {"radius": str(MAX_RADIUS_M + 1)}, {"radius": "abc"}):
        with pytest.raises(ValidationError):
            parse_radius(bad)


def test_limit_y_pokemon_id():
    assert parse_limit({}) == 50 and parse_limit({"limit": "7"}) == 7
    assert parse_pokemon_id({}) is None and parse_pokemon_id({"pokemonId": "16"}) == 16
    for bad in ("0", "-1", "5000", "x", "1.5"):
        with pytest.raises(ValidationError):
            parse_limit({"limit": bad})
    for bad in ("0", "152", "x"):
        with pytest.raises(ValidationError):
            parse_pokemon_id({"pokemonId": bad})


def test_poligono_valido_y_feature():
    geom = {"type": "Polygon", "coordinates": [RING]}
    assert parse_polygon(geom)["coordinates"][0][0] == [-74.02, 40.70]
    assert parse_polygon({"type": "Feature", "properties": {}, "geometry": geom})["type"] == "Polygon"


@pytest.mark.parametrize("body", [
    None, [], "x", {"type": "Point", "coordinates": [0, 0]}, {"type": "Polygon"}, {"type": "Polygon", "coordinates": []},
    {"type": "Polygon", "coordinates": [RING[:3]]},                              # < 4 posiciones
    {"type": "Polygon", "coordinates": [RING[:-1] + [[0, 0]]]},                  # anillo abierto
    {"type": "Polygon", "coordinates": [[[200, 0], [1, 1], [2, 2], [200, 0]]]},  # lng fuera de rango
    {"type": "Polygon", "coordinates": [[[0, 95], [1, 1], [2, 2], [0, 95]]]},    # lat fuera de rango
    {"type": "Polygon", "coordinates": [[["a", 0], [1, 1], [2, 2], ["a", 0]]]},  # no numerico
    {"type": "Polygon", "coordinates": [[[1, 2, 3], [1, 1], [2, 2], [1, 2, 3]]]},  # posicion 3D
])
def test_poligono_invalido(body):
    with pytest.raises(ValidationError):
        parse_polygon(body)


def test_poligono_demasiados_vertices():
    ring = [[i / 10000, i / 10000] for i in range(MAX_POLYGON_VERTICES + 5)]
    ring.append(ring[0])
    with pytest.raises(ValidationError):
        parse_polygon({"type": "Polygon", "coordinates": [ring]})


# ---------- consultas ----------
def test_near_usa_orden_lng_lat_y_radio():
    q = queries.near_filter(lng=-73.9855, lat=40.758, radius_m=500)
    near = q["location"]["$near"]
    assert near["$geometry"] == {"type": "Point", "coordinates": [-73.9855, 40.758]}
    assert near["$maxDistance"] == 500 and "pokemonId" not in q


def test_near_con_especie():
    assert queries.near_filter(1, 2, 10, pokemon_id=16)["pokemonId"] == 16


def test_within_geowithin():
    poly = {"type": "Polygon", "coordinates": [RING]}
    assert queries.within_filter(poly)["location"]["$geoWithin"]["$geometry"] == poly
    assert queries.within_filter(poly, 19)["pokemonId"] == 19


def test_geonear_pipeline():
    p = queries.geonear_pipeline(-73.98, 40.75, 300, pokemon_id=16, limit=3)
    stage = p[0]["$geoNear"]
    assert stage["near"]["coordinates"] == [-73.98, 40.75]
    assert stage["distanceField"] == "distance_m" and stage["spherical"] is True
    assert stage["maxDistance"] == 300 and stage["query"] == {"pokemonId": 16}
    assert p[1] == {"$limit": 3}
    assert "query" not in queries.geonear_pipeline(1, 2, 10)[0]["$geoNear"]
