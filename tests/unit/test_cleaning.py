import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))

from cleaning import clean_record, local_time  # noqa: E402


def doc(**over):
    base = {
        "localTime": "07:26:25",
        "source": "POKERADAR",
        "appearedOn": {"$date": "2016-09-21T12:26:25.000Z"},
        "pokemonId": 134,
        "_id": {"$oid": "57e27f5d4856fa0012d30faa"},
        "location": {"coordinates": [-73.972765, 40.77907], "type": "Point"},
        "__v": 0,
    }
    base.update(over)
    return base


def test_registro_valido():
    rec, why = clean_record(doc())
    assert why is None
    assert rec["pokemonId"] == 134
    assert rec["lng"] == -73.972765 and rec["lat"] == 40.77907
    assert rec["appeared_utc"] == datetime(2016, 9, 21, 12, 26, 25)
    assert rec["id"] == "57e27f5d4856fa0012d30faa"


def test_hora_local_se_deriva_de_utc_y_longitud():
    # Nueva York: lng -73.97 -> -295.9 min ~ -4h56 (solar). localTime del dataset NO se usa.
    rec, _ = clean_record(doc())
    assert rec["appeared_local"] == datetime(2016, 9, 21, 7, 30, 25)
    assert rec["local_hour"] == 7
    assert rec["local_dow"] == 2  # miercoles


def test_local_time_cruza_medianoche():
    assert local_time(datetime(2016, 9, 21, 1, 0), -90.0) == datetime(2016, 9, 20, 19, 0)


def test_sin_pokemon_id():
    d = doc()
    del d["pokemonId"]
    assert clean_record(d) == (None, "sin_pokemonId")


def test_pokemon_id_fuera_de_rango():
    assert clean_record(doc(pokemonId=152)) == (None, "sin_pokemonId")
    assert clean_record(doc(pokemonId=0)) == (None, "sin_pokemonId")
    assert clean_record(doc(pokemonId=True)) == (None, "sin_pokemonId")


def test_coordenadas_invalidas():
    casos = [
        {},                                                                   # sin location
        {"location": {"type": "Point", "coordinates": [0, 0]}},               # (0,0)
        {"location": {"type": "Point", "coordinates": [200, 10]}},            # lng fuera de rango
        {"location": {"type": "Point", "coordinates": [10, 95]}},             # lat fuera de rango
        {"location": {"type": "Point", "coordinates": [None, 10]}},           # nulo
        {"location": {"type": "Point", "coordinates": ["a", 10]}},            # no numerico
        {"location": {"type": "Point", "coordinates": [1]}},                  # longitud 1
        {"location": {"type": "Polygon", "coordinates": [1, 2]}},             # no es Point
        {"location": {"type": "Point", "coordinates": [float("nan"), 10]}},   # NaN
    ]
    for over in casos:
        d = doc()
        d.pop("location")
        d.update(over)
        assert clean_record(d) == (None, "coordenadas_invalidas"), over


def test_fecha_invalida():
    assert clean_record(doc(appearedOn={"$date": "no-es-fecha"})) == (None, "fecha_invalida")
    assert clean_record(doc(appearedOn={})) == (None, "fecha_invalida")
    assert clean_record(doc(appearedOn=None)) == (None, "fecha_invalida")


def test_fecha_con_milisegundos():
    rec, why = clean_record(doc(appearedOn={"$date": "2016-07-13T07:31:36.171Z"}))
    assert why is None and rec["appeared_utc"].year == 2016 and rec["appeared_utc"].month == 7


def test_orden_de_descarte_coordenadas_primero():
    d = doc(pokemonId=999)
    d["location"] = {"type": "Point", "coordinates": [0, 0]}
    assert clean_record(d) == (None, "coordenadas_invalidas")
