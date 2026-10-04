import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ingest"))
pytest.importorskip("distributed")

from clean import process_chunk  # noqa: E402


def write_lines(path, n):
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({
                "source": "X", "appearedOn": {"$date": "2016-09-21T12:26:25.000Z"}, "pokemonId": 1 + i % 100,
                "_id": {"$oid": f"{i:024x}"}, "location": {"coordinates": [-73.0 - i / 1e6, 40.0], "type": "Point"},
            }) + "\n")


def test_stride_toma_una_de_cada_k_en_todo_el_archivo(tmp_path):
    f = tmp_path / "p.json"
    write_lines(f, 20)
    ids = []
    for start, end in ((0, 8), (8, 16), (16, 20)):      # particiones multiplo de K, como en clean.py
        df, _ = process_chunk(str(f), start, end, 4)
        ids += list(df["id"])
    assert ids == [f"{i:024x}" for i in range(0, 20, 4)]


def test_stride_1_lee_todo(tmp_path):
    f = tmp_path / "p.json"
    write_lines(f, 7)
    df, stats = process_chunk(str(f), 0, 7)
    assert len(df) == 7 and stats["leidas"] == 7
