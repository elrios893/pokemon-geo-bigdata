"""Construye data/catalog/pokemon_catalog.json consultando PokeAPI una sola vez.

- Solo los ids 1..151 (los que aparecen en el dataset).
- Peticiones secuenciales con pausa, reintentos con backoff exponencial ante 429/5xx.
- Caché en disco: si un id ya fue descargado no se vuelve a pedir.
- Se guardan solo los campos necesarios (unos pocos KB en total).
"""
import json
import sys
import time
from pathlib import Path

import requests

API_URL = "https://pokeapi.co/api/v2/pokemon/{id}"
FIRST_ID, LAST_ID = 1, 151
PAUSE_SECONDS = 0.3
MAX_RETRIES = 5
HEADERS = {"User-Agent": "bigdata-geoespacial-iue/1.0 (proyecto academico)"}

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "data" / "cache" / "pokeapi"
OUT_FILE = ROOT / "data" / "catalog" / "pokemon_catalog.json"


def fetch(pokemon_id: int) -> dict:
    """Devuelve la respuesta cruda de PokeAPI, usando caché en disco."""
    cache_file = CACHE_DIR / f"{pokemon_id}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(API_URL.format(id=pokemon_id), headers=HEADERS, timeout=(10, 30))
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
            wait = 2 ** attempt
            print(f"  id {pokemon_id}: {type(exc).__name__}, reintento en {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if resp.status_code == 200:
            data = resp.json()
            cache_file.write_text(json.dumps(data), encoding="utf-8")
            time.sleep(PAUSE_SECONDS)
            return data
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  id {pokemon_id}: HTTP {resp.status_code}, reintento en {wait:.0f}s", file=sys.stderr)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"PokeAPI no respondio para id {pokemon_id} tras {MAX_RETRIES} intentos")


def slim(raw: dict) -> dict:
    stats = {s["stat"]["name"]: s["base_stat"] for s in raw["stats"]}
    return {
        "pokemonId": raw["id"],
        "name": raw["name"],
        "types": [t["type"]["name"] for t in sorted(raw["types"], key=lambda t: t["slot"])],
        "height": raw["height"],  # decimetros
        "weight": raw["weight"],  # hectogramos
        "base_experience": raw.get("base_experience"),
        "stats": {
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special_attack": stats.get("special-attack"),
            "special_defense": stats.get("special-defense"),
            "speed": stats.get("speed"),
        },
    }


def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    catalog = []
    for pid in range(FIRST_ID, LAST_ID + 1):
        catalog.append(slim(fetch(pid)))
        if pid % 25 == 0 or pid == LAST_ID:
            print(f"{pid}/{LAST_ID} listos")
    OUT_FILE.write_text(json.dumps(catalog, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Catalogo escrito en {OUT_FILE} ({len(catalog)} pokemon)")


if __name__ == "__main__":
    main()
