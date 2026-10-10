import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))

from ensure_data import needs_load  # noqa: E402

IDX = ["_id_", "location_2dsphere", "pokemonId_1"]


def test_no_recarga_si_los_datos_estan_completos():
    assert needs_load(2_239_470, IDX, 151) is False


def test_carga_si_la_base_esta_vacia():
    assert needs_load(0, [], 0) is True


def test_carga_si_la_carga_anterior_quedo_a_medias():
    # sin indice 2dsphere: load_mongo.py lo crea al final, asi que la carga no termino
    assert needs_load(1_200_000, ["_id_"], 151) is True


def test_carga_si_falta_el_catalogo():
    assert needs_load(2_239_470, IDX, 0) is True


def test_force_recarga_siempre():
    assert needs_load(2_239_470, IDX, 151, force=True) is True
