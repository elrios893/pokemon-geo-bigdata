"""Descarga y descomprime los 4 archivos de Pokemon GO ("Catch Them All") en data/raw/.

Los archivos son adjuntos de un hilo de discusion de Kaggle (dataset "Predict'em All", autor
semioniy), alojados como ZIP publicos en storage.googleapis.com. No requieren token.

- Idempotente: si el .json ya existe y coincide en tamano con el del ZIP, no se vuelve a bajar.
- Robusto: reintentos con backoff ante errores de red; el ZIP se baja a un .part y se renombra
  solo cuando esta completo y valido.
- Verificable: se comprueba la integridad del ZIP y que el tamano extraido coincida con el declarado.

Uso:  python ingest/download.py [--dest data/raw] [--keep-zip]
"""
import argparse
import os
import sys
import time
import zipfile
from pathlib import Path

import requests

BASE = "https://storage.googleapis.com/kaggle-forum-message-attachments/138703"
# El id del adjunto no sigue el orden de las partes (5004, 5006, 5005, 5007).
PARTS = {
    "catchemall_part1": f"{BASE}/5004/catchemall_part1.zip",
    "catchemall_part2": f"{BASE}/5006/catchemall_part2.zip",
    "catchemall_part3": f"{BASE}/5005/catchemall_part3.zip",
    "catchemall_part4": f"{BASE}/5007/catchemall_part4.zip",
}
MAX_RETRIES = 5
HEADERS = {"User-Agent": "bigdata-geoespacial-iue/1.0 (proyecto academico)"}
CHUNK = 1024 * 1024


def log(msg):
    print(msg, flush=True)


def download_zip(url, target):
    """Baja url a target (via .part). Reintenta ante errores de red."""
    part = target.with_suffix(target.suffix + ".part")
    for attempt in range(MAX_RETRIES):
        try:
            with requests.get(url, headers=HEADERS, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", 0))
                got = 0
                with open(part, "wb") as fh:
                    for chunk in r.iter_content(CHUNK):
                        fh.write(chunk)
                        got += len(chunk)
            if expected and got != expected:
                raise IOError(f"descarga incompleta: {got} de {expected} bytes")
            part.replace(target)
            return
        except (requests.RequestException, IOError) as exc:
            wait = 2 ** attempt
            log(f"  intento {attempt + 1}/{MAX_RETRIES} fallo ({type(exc).__name__}: {exc}); reintento en {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"no se pudo descargar {url} tras {MAX_RETRIES} intentos")


def fetch_part(name, url, dest, keep_zip):
    json_path = dest / f"{name}.json"
    zip_path = dest / f"{name}.zip"

    # Cache: si ya hay un JSON y existe el ZIP para comparar tamano, o el JSON es plausible, se reutiliza.
    if json_path.exists() and json_path.stat().st_size > 0:
        if zip_path.exists():
            with zipfile.ZipFile(zip_path) as zf:
                declared = {i.filename: i.file_size for i in zf.infolist()}
            if declared.get(json_path.name) == json_path.stat().st_size:
                log(f"[{name}] ya existe y coincide con el ZIP; se omite")
                return json_path
        else:
            log(f"[{name}] ya existe ({json_path.stat().st_size / 1e6:.0f} MB); se omite (usa --force para rehacer)")
            return json_path

    log(f"[{name}] descargando {url}")
    t0 = time.time()
    download_zip(url, zip_path)
    log(f"[{name}] ZIP: {zip_path.stat().st_size / 1e6:.0f} MB en {time.time() - t0:.0f}s")

    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP corrupto, entrada danada: {bad}")
        members = [i for i in zf.infolist() if not i.is_dir()]
        if len(members) != 1:
            raise RuntimeError(f"se esperaba 1 archivo en {zip_path.name}, hay {len(members)}")
        info = members[0]
        tmp = dest / (name + ".json.part")
        with zf.open(info) as src, open(tmp, "wb") as out:
            while True:
                chunk = src.read(CHUNK)
                if not chunk:
                    break
                out.write(chunk)
        if tmp.stat().st_size != info.file_size:
            raise RuntimeError(f"tamano extraido {tmp.stat().st_size} != declarado {info.file_size}")
        tmp.replace(json_path)
    log(f"[{name}] extraido: {json_path.name} ({json_path.stat().st_size / 1e6:.0f} MB)")
    if not keep_zip:
        zip_path.unlink()
    return json_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", default=os.environ.get("RAW_DIR", "data/raw"))
    ap.add_argument("--keep-zip", action="store_true")
    ap.add_argument("--force", action="store_true", help="rehacer aunque ya existan los JSON")
    args = ap.parse_args()

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)
    if args.force:
        for name in PARTS:
            (dest / f"{name}.json").unlink(missing_ok=True)

    paths = [fetch_part(n, u, dest, args.keep_zip) for n, u in PARTS.items()]
    total = sum(p.stat().st_size for p in paths)
    log(f"Listo: {len(paths)} archivos, {total / 1e9:.2f} GB en {dest}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log(f"ERROR: {exc}")
        sys.exit(1)
