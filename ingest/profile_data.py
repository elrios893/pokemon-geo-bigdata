"""Perfilado de solo lectura de los 4 NDJSON de Pokemon GO ("Catch Them All").

Mide la calidad de los datos para fundamentar las reglas de limpieza (docs/PLAN_DE_TRABAJO.md §7)
y valida el criterio de muestreo (primeras N filas de cada parte). Solo usa la biblioteca estandar.

Uso:  python ingest/profile_data.py "<carpeta con catchemall_part*.json>" [--sample 500000]
Salida: resumen por pantalla y data/profile.json (ignorado por git).
"""
import argparse
import json
import math
import time
from collections import Counter
from pathlib import Path

MIN_ID, MAX_ID = 1, 151


def classify_coords(doc):
    """Devuelve (categoria, lng, lat). categoria == 'ok' si es utilizable."""
    loc = doc.get("location")
    if not isinstance(loc, dict):
        return "sin_location", None, None
    if loc.get("type") != "Point":
        return "tipo_no_point", None, None
    c = loc.get("coordinates")
    if not isinstance(c, list) or len(c) != 2:
        return "coordinates_malformado", None, None
    lng, lat = c
    if isinstance(lng, bool) or isinstance(lat, bool) or not all(isinstance(v, (int, float)) for v in (lng, lat)):
        return "no_numerico_o_nulo", None, None
    if math.isnan(lng) or math.isnan(lat):
        return "nan", None, None
    if abs(lat) > 90 or abs(lng) > 180:
        return "fuera_de_rango", lng, lat
    if lat == 0 and lng == 0:
        return "cero_cero", lng, lat
    return "ok", lng, lat


def offset_minutes(local_time, iso_utc):
    """Desfase local-UTC en minutos, normalizado a [-12h, +14h]; None si no se puede calcular."""
    try:
        lh, lm = int(local_time[0:2]), int(local_time[3:5])
        uh, um = int(iso_utc[11:13]), int(iso_utc[14:16])
    except (TypeError, ValueError, IndexError):
        return None
    diff = ((lh * 60 + lm) - (uh * 60 + um)) % 1440
    return diff - 1440 if diff > 14 * 60 else diff


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--sample", type=int, default=500_000, help="filas por parte para validar el muestreo")
    args = ap.parse_args()

    files = sorted(Path(args.folder).glob("catchemall_part*.json"))
    if not files:
        raise SystemExit("No se encontraron catchemall_part*.json")

    seen_ids, seen_events = set(), set()          # duplicados globales
    sample_events = set()                          # duplicados dentro de la muestra
    glob_dups_id = glob_dups_event = 0
    out = {"files": {}, "sample_per_part": args.sample}
    t0 = time.time()

    for f in files:
        st = {
            "rows": 0, "blank": 0, "json_error": 0, "json_error_examples": [],
            "coords": Counter(), "id_invalid": 0, "id_counts": Counter(),
            "source": Counter(), "keys": Counter(), "no_date": 0, "bad_date": 0,
            "date_min": None, "date_max": None, "days": Counter(), "offset": Counter(),
            "dup_id": 0, "dup_event": 0,
            "lat_min": 90.0, "lat_max": -90.0, "lng_min": 180.0, "lng_max": -180.0,
            "cells": Counter(),
            "sample": {"rows": 0, "coords_ok": 0, "dup_event": 0, "date_min": None, "date_max": None},
        }
        with open(f, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    st["blank"] += 1
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    st["json_error"] += 1
                    if len(st["json_error_examples"]) < 3:
                        st["json_error_examples"].append(line[:120])
                    continue
                st["rows"] += 1
                n = st["rows"]
                in_sample = n <= args.sample

                st["keys"][",".join(sorted(d.keys()))] += 1
                st["source"][str(d.get("source"))] += 1

                pid = d.get("pokemonId")
                if isinstance(pid, int) and not isinstance(pid, bool) and MIN_ID <= pid <= MAX_ID:
                    st["id_counts"][pid] += 1
                else:
                    st["id_invalid"] += 1

                cat, lng, lat = classify_coords(d)
                st["coords"][cat] += 1
                if cat == "ok":
                    st["lat_min"] = min(st["lat_min"], lat); st["lat_max"] = max(st["lat_max"], lat)
                    st["lng_min"] = min(st["lng_min"], lng); st["lng_max"] = max(st["lng_max"], lng)
                    st["cells"][(math.floor(lat), math.floor(lng))] += 1

                date = None
                ao = d.get("appearedOn")
                if isinstance(ao, dict) and isinstance(ao.get("$date"), str):
                    date = ao["$date"]
                    if len(date) < 19 or date[4] != "-":
                        st["bad_date"] += 1
                        date = None
                else:
                    st["no_date"] += 1
                if date:
                    if st["date_min"] is None or date < st["date_min"]:
                        st["date_min"] = date
                    if st["date_max"] is None or date > st["date_max"]:
                        st["date_max"] = date
                    st["days"][date[:10]] += 1
                    off = offset_minutes(d.get("localTime"), date)
                    st["offset"]["sin_dato" if off is None else off] += 1

                oid = (d.get("_id") or {}).get("$oid") if isinstance(d.get("_id"), dict) else None
                if oid is not None:
                    if oid in seen_ids:
                        st["dup_id"] += 1
                    else:
                        seen_ids.add(oid)

                ev = hash((pid, lng, lat, date))
                if ev in seen_events:
                    st["dup_event"] += 1
                else:
                    seen_events.add(ev)

                if in_sample:
                    sm = st["sample"]
                    sm["rows"] += 1
                    if cat == "ok":
                        sm["coords_ok"] += 1
                    if ev in sample_events:
                        sm["dup_event"] += 1
                    else:
                        sample_events.add(ev)
                    if date:
                        if sm["date_min"] is None or date < sm["date_min"]:
                            sm["date_min"] = date
                        if sm["date_max"] is None or date > sm["date_max"]:
                            sm["date_max"] = date

        print(f"[{time.time() - t0:6.0f}s] {f.name}: {st['rows']:,} filas", flush=True)
        out["files"][f.name] = st

    # ---- resumen ----
    tot = lambda k: sum(s[k] for s in out["files"].values())
    rows = tot("rows")
    coords = Counter()
    for s in out["files"].values():
        coords.update(s["coords"])
    print("\n=== RESUMEN ===")
    print(f"Filas validas: {rows:,} | JSON invalidos: {tot('json_error'):,} | lineas vacias: {tot('blank'):,}")
    print("Coordenadas:", {k: f"{v:,} ({v / rows:.3%})" for k, v in coords.most_common()})
    print(f"pokemonId fuera de 1..151: {tot('id_invalid'):,} | sin fecha: {tot('no_date'):,} | fecha mal formada: {tot('bad_date'):,}")
    print(f"Duplicados por _id (entre/dentro de partes): {tot('dup_id'):,}")
    print(f"Duplicados por evento (pokemonId, lng, lat, fecha): {tot('dup_event'):,} ({tot('dup_event') / rows:.3%})")
    for name, s in out["files"].items():
        print(f"\n{name}: {s['rows']:,} filas | fechas {s['date_min']} -> {s['date_max']}")
        print("  source:", dict(s["source"].most_common(6)))
        print("  claves distintas:", len(s["keys"]), "|", dict(list(s["keys"].most_common(3))))
        sm = s["sample"]
        print(f"  muestra primeras {sm['rows']:,}: coords ok {sm['coords_ok']:,}, fechas {sm['date_min']} -> {sm['date_max']}")
    src = Counter()
    days = Counter()
    cells = Counter()
    offs = Counter()
    ids = Counter()
    for s in out["files"].values():
        src.update(s["source"]); days.update(s["days"]); cells.update(s["cells"]); offs.update(s["offset"]); ids.update(s["id_counts"])
    print("\nSource global:", dict(src.most_common()))
    print("Dias:", dict(sorted(days.items())))
    print("Desfase local-UTC (min) mas comunes:", dict(offs.most_common(8)))
    odd = sum(v for k, v in offs.items() if k != "sin_dato" and k % 15 != 0)
    print(f"Desfases que no son multiplo de 15 min: {odd:,}")
    print("Top 12 celdas de 1 grado (lat,lng):", [(k, v) for k, v in cells.most_common(12)])
    print(f"Celdas de 1 grado con datos: {len(cells):,}")
    print(f"pokemonId distintos: {len(ids)} | top 5: {ids.most_common(5)} | menos frecuentes: {ids.most_common()[-3:]}")
    sm_rows = sum(s["sample"]["rows"] for s in out["files"].values())
    sm_dup = sum(s["sample"]["dup_event"] for s in out["files"].values())
    sm_ok = sum(s["sample"]["coords_ok"] for s in out["files"].values())
    print(f"\nMUESTRA ({args.sample:,} por parte = {sm_rows:,}): coords ok {sm_ok:,}; duplicados {sm_dup:,}; "
          f"util estimada tras limpieza ~ {sm_ok - sm_dup:,} (minimo exigido 1,000,000)")

    # serializable
    def ser(o):
        if isinstance(o, Counter):
            return {str(k): v for k, v in o.items()}
        return o
    dump = {"sample_per_part": out["sample_per_part"], "files": {}}
    for name, s in out["files"].items():
        dump["files"][name] = {k: ser(v) for k, v in s.items()}
    p = Path(__file__).resolve().parent.parent / "data" / "profile.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(dump, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nDetalle completo en {p}  ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
