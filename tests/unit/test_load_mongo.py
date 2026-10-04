import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))

from load_mongo import to_document  # noqa: E402

REC = {
    "id": "57e27f5d4856fa0012d30faa",
    "pokemonId": 134,
    "source": "POKERADAR",
    "lng": -73.972765,
    "lat": 40.77907,
    "appeared_utc": datetime(2016, 9, 21, 12, 26, 25),
    "local_date": "2016-09-21",
    "local_hour": 7,
    "local_dow": 2,
}


def test_location_es_geojson_point_con_orden_lng_lat():
    doc = to_document(REC)
    assert doc["location"] == {"type": "Point", "coordinates": [-73.972765, 40.77907]}


def test_id_original_como__id():
    assert to_document(REC)["_id"] == "57e27f5d4856fa0012d30faa"


def test_campos_y_tipos():
    doc = to_document(REC)
    assert set(doc) == {"_id", "pokemonId", "source", "location", "appeared_utc",
                        "local_date", "local_hour", "local_dow"}
    assert isinstance(doc["pokemonId"], int) and isinstance(doc["local_hour"], int)
    assert isinstance(doc["location"]["coordinates"][0], float)
    assert doc["appeared_utc"] == datetime(2016, 9, 21, 12, 26, 25)


def test_no_incluye_hora_local_ambigua_ni_localTime():
    doc = to_document(REC)
    assert "localTime" not in doc and "appeared_local" not in doc


def test_acepta_tipos_numpy_como_enteros_y_flotantes_python():
    import numpy as np
    rec = dict(REC, pokemonId=np.int16(134), lng=np.float64(-73.97), lat=np.float64(40.77),
               local_hour=np.int8(7), local_dow=np.int8(2))
    doc = to_document(rec)
    assert type(doc["pokemonId"]) is int and type(doc["local_hour"]) is int
    assert type(doc["location"]["coordinates"][0]) is float
