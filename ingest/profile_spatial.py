"""Perfilado espacial de solo lectura sobre la POBLACION COMPLETA (no solo la muestra).

Pregunta: los avistamientos caen en pocos puntos de aparicion repetidos (spawn points). Cuanto, y que
relacion tienen entre si los avistamientos que comparten coordenada exacta?

Mide, para tres vistas de los mismos datos:
  completo  : las ~8.9 M filas (tras quitar filas sin pokemonId y duplicados exactos)
  prefijo   : las primeras N filas de cada parte (criterio de muestreo actual de clean.py)
  salto     : una de cada K filas (muestreo sistematico, cubre toda la linea de tiempo)

Uso:  python profile_spatial.py [--raw /data/raw] [--prefix 500000] [--stride 4] [--out /data/spatial_profile.json]
Solo requiere pandas; el criterio de punto es la coordenada exacta redondeada a 1e-6 grados (~0.1 m).
"""
import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

SCALE = 1_000_000
BIG = 361 * SCALE + 1


def load(raw_dir):
    cols = {"line": [], "pid": [], "lng": [], "lat": [], "ts": [], "src": []}
    srcs = {}
    for f in sorted(Path(raw_dir).glob("catchemall_part*.json")):
        with open(f, "rb") as fh:
            for i, line in enumerate(fh):
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                pid = d.get("pokemonId")
                if pid is None:
                    continue
                lng, lat = d["location"]["coordinates"]
                cols["line"].append(i)
                cols["pid"].append(int(pid))
                cols["lng"].append(lng)
                cols["lat"].append(lat)
                cols["ts"].append(d["appearedOn"]["$date"])
                cols["src"].append(srcs.setdefault(d.get("source"), len(srcs)))
        print(f"  leido {f.name}", flush=True)
    df = pd.DataFrame(cols)
    df["pid"] = df["pid"].astype("int16")
    df["src"] = df["src"].astype("int8")
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None)
    df["tsec"] = (df["ts"].astype("int64") // 10**9).astype("int64")
    df["key"] = (np.rint(df["lng"] * SCALE).astype("int64") + 180 * SCALE) * BIG + (
        np.rint(df["lat"] * SCALE).astype("int64") + 90 * SCALE)
    df = df.drop_duplicates(subset=["pid", "key", "tsec"]).reset_index(drop=True)
    return df, {v: k for k, v in srcs.items()}


def view_stats(df, water_ids, label):
    out = {"vista": label, "avistamientos": int(len(df))}
    n_by_key = df.groupby("key").size()
    out["puntos_distintos"] = int(len(n_by_key))
    out["avist_por_punto_medio"] = round(len(df) / len(n_by_key), 3)
    rep = df["key"].map(n_by_key)
    out["pct_avist_en_puntos_repetidos"] = round(100 * float((rep > 1).mean()), 1)
    out["pct_puntos_vistos_una_vez"] = round(100 * float((n_by_key == 1).mean()), 1)
    bins = [0, 1, 2, 5, 10, 50, 10**9]
    names = ["1", "2", "3-5", "6-10", "11-50", ">50"]
    h = pd.cut(n_by_key, bins=bins, labels=names).value_counts().reindex(names)
    out["puntos_por_nro_de_avistamientos"] = {k: int(v) for k, v in h.items()}
    out["max_avist_en_un_punto"] = int(n_by_key.max())

    # pares consecutivos en el tiempo dentro del mismo punto
    s = df.sort_values(["key", "tsec"], kind="mergesort")
    same = s["key"].values[1:] == s["key"].values[:-1]
    cur, nxt = s.iloc[:-1][same], s.iloc[1:][same]
    gap_h = (nxt["tsec"].values - cur["tsec"].values) / 3600.0
    same_sp = nxt["pid"].values == cur["pid"].values
    out["pares_consecutivos"] = int(same.sum())
    if same.sum() == 0:
        return out
    p = df["pid"].value_counts(normalize=True)
    out["base_misma_especie_al_azar"] = round(float((p ** 2).sum()), 4)
    out["misma_especie_en_par_consecutivo"] = round(float(same_sp.mean()), 4)
    q = np.quantile(gap_h, [0.1, 0.25, 0.5, 0.75, 0.9])
    out["brecha_horas_cuantiles_10_25_50_75_90"] = [round(float(x), 2) for x in q]
    out["pct_brecha_menor_15min"] = round(100 * float((gap_h <= 0.25).mean()), 2)
    out["pct_brecha_menor_1h"] = round(100 * float((gap_h <= 1).mean()), 2)
    out["pct_brecha_menor_24h"] = round(100 * float((gap_h <= 24).mean()), 2)
    # misma especie segun la brecha
    edges = [(-1, 0.25, "<=15min"), (0.25, 1, "15min-1h"), (1, 24, "1h-24h"), (24, 24 * 7, "1-7d"), (24 * 7, 1e9, ">7d")]
    por_brecha = {}
    for lo, hi, name in edges:
        m = (gap_h > lo) & (gap_h <= hi)
        if m.sum():
            por_brecha[name] = {"n": int(m.sum()), "misma_especie": round(float(same_sp[m].mean()), 4)}
    out["misma_especie_por_brecha"] = por_brecha
    # duplicado semantico: misma especie, mismo punto, <= 15 min (un spawn dura <= 15 min)
    near_dup = same_sp & (gap_h <= 0.25)
    out["near_dup_misma_especie_15min"] = int(near_dup.sum())
    out["pct_near_dup_sobre_avistamientos"] = round(100 * float(near_dup.sum()) / len(df), 3)
    # agua -> agua en el mismo punto
    w_cur = cur["pid"].isin(water_ids).values
    w_nxt = nxt["pid"].isin(water_ids).values
    out["agua_global"] = round(float(df["pid"].isin(water_ids).mean()), 4)
    out["P_siguiente_agua_dado_actual_agua_mismo_punto"] = round(float(w_nxt[w_cur].mean()), 4)
    out["P_siguiente_agua_dado_actual_no_agua_mismo_punto"] = round(float(w_nxt[~w_cur].mean()), 4)
    # mismas fuentes entre pares consecutivos
    out["pct_par_misma_fuente"] = round(100 * float((nxt["src"].values == cur["src"].values).mean()), 1)
    # mezcla de especies por punto repetido
    g = df[df["key"].map(n_by_key) >= 2].groupby("key")["pid"].agg(["size", "nunique"])
    out["puntos_repetidos"] = int(len(g))
    out["pct_puntos_repetidos_monoespecie"] = round(100 * float((g["nunique"] == 1).mean()), 1)
    out["especies_distintas_por_punto_repetido_media"] = round(float(g["nunique"].mean()), 2)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="/data/raw")
    ap.add_argument("--catalog", default="/data/catalog/pokemon_catalog.json")
    ap.add_argument("--prefix", type=int, default=500_000)
    ap.add_argument("--stride", type=int, default=4)
    ap.add_argument("--out", default="/data/spatial_profile.json")
    a = ap.parse_args()
    t0 = time.time()
    cat = json.loads(Path(a.catalog).read_text(encoding="utf-8"))
    water_ids = {c["pokemonId"] for c in cat if "water" in c["types"]}

    df, srcs = load(a.raw)
    print(f"{len(df):,} filas utiles en {time.time() - t0:.0f}s", flush=True)
    res = {
        "fuentes": srcs,
        "completo": view_stats(df, water_ids, "completo"),
        "prefijo": view_stats(df[df["line"] < a.prefix], water_ids, f"primeras {a.prefix} de cada parte"),
        "salto": view_stats(df[df["line"] % a.stride == 0], water_ids, f"una de cada {a.stride}"),
    }
    # cobertura temporal de cada vista (el prefijo concentra la linea de tiempo?)
    for k, sub in (("completo", df), ("prefijo", df[df["line"] < a.prefix]), ("salto", df[df["line"] % a.stride == 0])):
        res[k]["rango_fechas"] = [str(sub["ts"].min()), str(sub["ts"].max())]
        res[k]["dias_distintos"] = int(sub["ts"].dt.normalize().nunique())
        dow = sub["ts"].dt.dayofweek.value_counts(normalize=True).sort_index()
        res[k]["pct_por_dia_semana_UTC_lun_a_dom"] = [round(100 * float(x), 1) for x in dow.reindex(range(7), fill_value=0)]
    Path(a.out).write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(res, ensure_ascii=False, indent=2))
    print(f"total {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
