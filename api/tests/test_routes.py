import pytest

from app import create_app, queries
from conftest import FakeCollection, FakeDB

POLY = {"type": "Polygon", "coordinates": [[[-74.02, 40.70], [-73.93, 40.70], [-73.93, 40.80],
                                              [-74.02, 40.80], [-74.02, 40.70]]]}


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.get_json() == {"status": "ok", "mongo": "up"}


def test_near_ok_y_consulta_exacta(client, db):
    r = client.get("/near?lat=40.758&lng=-73.9855&radius=500&pokemonId=16&limit=5")
    body = r.get_json()
    assert r.status_code == 200 and body["count"] == 1
    rec = body["results"][0]
    assert rec["name"] == "pidgey" and rec["types"] == ["normal", "flying"]   # enriquecido con el catalogo
    assert rec["appeared_utc"] == "2016-09-21T12:26:25Z"
    assert rec["location"]["coordinates"] == [-73.97, 40.77]
    call = db.spawns.calls[-1]
    assert call["filter"] == queries.near_filter(-73.9855, 40.758, 500.0, 16)     # parametros del usuario, no fijos
    assert call["limit"] == 5 and call["max_time_ms"] > 0


@pytest.mark.parametrize("qs", [
    "", "lat=40.7", "lat=40.7&lng=-73.9", "lat=91&lng=0&radius=10", "lat=0&lng=181&radius=10",
    "lat=40&lng=-73&radius=0", "lat=40&lng=-73&radius=999999", "lat=x&lng=1&radius=1",
    "lat=40&lng=-73&radius=10&pokemonId=999", "lat=40&lng=-73&radius=10&limit=100000",
])
def test_near_400(client, db, qs):
    r = client.get("/near?" + qs)
    assert r.status_code == 400 and "error" in r.get_json()
    assert db.spawns.calls == []          # una entrada invalida nunca llega a la base


def test_within_ok(client, db):
    r = client.post("/within?pokemonId=16", json=POLY)
    assert r.status_code == 200
    assert db.spawns.calls[-1]["filter"] == queries.within_filter(POLY, 16)
    assert r.get_json()["truncated"] is False


def test_within_acepta_feature(client):
    assert client.post("/within", json={"type": "Feature", "geometry": POLY, "properties": {}}).status_code == 200


def test_within_trunca_con_limit(client, db):
    db.spawns.docs = db.spawns.docs * 3
    r = client.post("/within?limit=2", json=POLY).get_json()
    assert r["count"] == 2 and r["truncated"] is True


def test_within_total_solo_si_se_pide(client, db):
    assert "total" not in client.post("/within", json=POLY).get_json()
    r = client.post("/within?count=true&pokemonId=16", json=POLY).get_json()
    assert r["total"] == 1234 and r["count"] == 1
    assert db.spawns.calls[-1]["count_filter"] == queries.within_filter(POLY, 16)
    assert client.post("/within?count=quizas", json=POLY).status_code == 400


@pytest.mark.parametrize("kw", [
    {"json": {"type": "Point", "coordinates": [0, 0]}}, {"json": [1, 2]}, {"data": "no es json"},
    {"data": "{}", "content_type": "application/json"}, {},
])
def test_within_400(client, db, kw):
    r = client.post("/within", **kw)
    assert r.status_code == 400 and db.spawns.calls == []


def test_geonear_devuelve_distancia(client, db):
    r = client.get("/geonear?lat=40.758&lng=-73.9855&radius=300&limit=3")
    assert r.status_code == 200 and r.get_json()["results"][0]["distance_m"] == 37.0
    call = db.spawns.calls[-1]
    assert call["pipeline"] == queries.geonear_pipeline(-73.9855, 40.758, 300.0, None, 3)


def test_geonear_400(client):
    assert client.get("/geonear?lat=40&lng=-73").status_code == 400


def test_stats_hotspots_y_grid(client, db):
    assert client.get("/stats/hotspots?limit=5").get_json()["results"][0]["rank"] == 1
    assert db.agg_hotspots.calls[-1]["sort"] == ("rank", 1)
    r = client.get("/stats/grid?min_lat=40&max_lat=41&limit=10")
    assert r.status_code == 200
    assert db.agg_grid.calls[-1]["filter"] == {"lat_c": {"$gte": 40.0, "$lte": 41.0}}
    assert client.get("/stats/grid?min_lat=abc").status_code == 400


def test_stats_time(client, db):
    assert client.get("/stats/time?granularity=hour").get_json()["results"][0]["count"] == 63238
    assert db.agg_time.calls[-1]["filter"] == {"granularity": "hour"}
    assert client.get("/stats/time?granularity=year").status_code == 400
    assert client.get("/stats/time").status_code == 200       # por defecto: hora


def test_stats_species_y_horas(client):
    assert client.get("/stats/species?limit=3").get_json()["count"] == 1
    r = client.get("/stats/species/16/hours").get_json()
    assert r["name"] == "pidgey" and r["results"][0]["local_hour"] == 7
    assert client.get("/stats/species/999/hours").status_code == 400


def test_404_y_405_son_json(client):
    r = client.get("/no-existe")
    assert r.status_code == 404 and "error" in r.get_json()
    r = client.get("/within")                                  # /within solo acepta POST
    assert r.status_code == 405 and "error" in r.get_json()


def test_error_de_mongo_se_convierte_en_400():
    class OperationFailure(Exception):
        pass

    db = FakeDB()
    db.spawns = FakeCollection(error=OperationFailure("Loop is not valid"))
    app = create_app(db=db)
    app.testing = False
    r = app.test_client().post("/within", json=POLY)
    assert r.status_code == 400 and "rechazada" in r.get_json()["error"]


def test_error_inesperado_es_500_sin_filtrar_detalles():
    db = FakeDB()
    db.spawns = FakeCollection(error=RuntimeError("secreto interno"))
    app = create_app(db=db)
    app.testing = False
    r = app.test_client().get("/near?lat=1&lng=1&radius=10")
    assert r.status_code == 500 and "secreto" not in r.get_data(as_text=True)


@pytest.mark.parametrize("path", [
    "/static/app.js", "/static/app.css", "/static/vendor/leaflet.js", "/static/vendor/leaflet-heat.js",
    "/static/vendor/leaflet.draw.js", "/static/vendor/fonts/geist-latin-400.woff2",
])
def test_interfaz_web_y_estaticos(client, path):
    page = client.get("/")
    assert page.status_code == 200 and b"leaflet" in page.data and page.mimetype == "text/html"
    assert client.get(path).status_code == 200      # la UI no depende de CDN: todo viaja en la imagen
