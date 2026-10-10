import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app  # noqa: E402


class FakeCursor:
    def __init__(self, docs, log):
        self.docs, self.log = list(docs), log

    def sort(self, key, direction):
        self.log["sort"] = (key, direction)
        return self

    def limit(self, n):
        self.log["limit"] = n
        self.docs = self.docs[:n]
        return self

    def max_time_ms(self, ms):
        self.log["max_time_ms"] = ms
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeCollection:
    def __init__(self, docs=None, error=None):
        self.docs, self.error, self.calls = docs or [], error, []

    def find(self, flt=None, *a, **k):
        if self.error:
            raise self.error
        log = {"filter": flt}
        self.calls.append(log)
        return FakeCursor(self.docs, log)

    def count_documents(self, flt, **k):
        if self.error:
            raise self.error
        self.calls.append({"count_filter": flt, **k})
        return 1234

    def create_index(self, keys, **k):
        self.calls.append({"create_index": keys})

    def aggregate(self, pipeline, **k):
        if self.error:
            raise self.error
        self.calls.append({"pipeline": pipeline, **k})
        return iter(self.docs)


class FakeAdmin:
    def command(self, name):
        return {"ok": 1}


class FakeClient:
    admin = FakeAdmin()


class FakeDB:
    client = FakeClient()

    def __init__(self):
        self.pokemon_catalog = FakeCollection([
            {"pokemonId": 16, "name": "pidgey", "types": ["normal", "flying"]},
            {"pokemonId": 19, "name": "rattata", "types": ["normal"]},
        ])
        self.spawns = FakeCollection([
            {"_id": "a1", "pokemonId": 16, "source": "POKERADAR",
             "location": {"type": "Point", "coordinates": [-73.97, 40.77]},
             "appeared_utc": datetime(2016, 9, 21, 12, 26, 25),
             "local_date": "2016-09-21", "local_hour": 7, "local_dow": 2, "distance_m": 37.04},
        ])
        self.agg_hotspots = FakeCollection([{"_id": 1, "rank": 1, "cell_id": "-7398_4076", "count": 2839}])
        self.agg_grid = FakeCollection([{"_id": "c", "cell_id": "c", "count": 10, "lat_c": 40.7, "lng_c": -74.0}])
        self.agg_time = FakeCollection([{"_id": "hour:07", "granularity": "hour", "key": "07", "count": 63238}])
        self.agg_species = FakeCollection([{"_id": 16, "pokemonId": 16, "name": "pidgey", "count": 5}])
        self.agg_species_hour = FakeCollection([{"_id": "16:7", "pokemonId": 16, "local_hour": 7, "count": 3}])


@pytest.fixture
def db():
    return FakeDB()


@pytest.fixture
def client(db):
    app = create_app(db=db)
    app.testing = True
    return app.test_client()
