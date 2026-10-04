"""Resume data/benchmark/results.jsonl: tabla CSV/Markdown y graficas en benchmark/results/.

Verifica primero que Dask y Spark produjeron el MISMO resultado (celdas, total, especies por celda) en cada tamano;
si no, aborta: comparar tiempos de calculos distintos no tiene sentido.

Uso (en el host, requiere matplotlib):  python benchmark/report.py
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "benchmark" / "results.jsonl"
OUT = ROOT / "benchmark" / "results"
OUT.mkdir(parents=True, exist_ok=True)

runs, mem = defaultdict(list), {}
checks = defaultdict(set)
for line in SRC.read_text(encoding="utf-8").splitlines():
    r = json.loads(line)
    key = (r["engine"], r["cores"], r["files"])
    if r.get("kind") == "memoria":
        mem[key] = r["peak_rss_mb"]
    elif not r["warmup"]:
        runs[key].append(r["seconds"])
        checks[r["files"]].add((r["cells"], r["total"], r["species_sum"]))
        checks[("rows", r["files"])] = r["total"]

for files in (2, 4, 8):
    if len(checks[files]) != 1:
        sys.exit(f"Resultados distintos entre motores/corridas para {files} archivos: {checks[files]}")

rows_by_files = {f: checks[("rows", f)] for f in (2, 4, 8)}
table = []
for (engine, cores, files), secs in sorted(runs.items(), key=lambda kv: (kv[0][2], kv[0][0], kv[0][1])):
    table.append({"motor": engine, "nucleos": cores, "workers": cores // 2, "archivos": files, "filas": rows_by_files[files],
                  "corridas": len(secs), "mediana_s": round(statistics.median(secs), 2), "min_s": min(secs), "max_s": max(secs),
                  "filas_por_s": round(rows_by_files[files] / statistics.median(secs)),
                  "pico_rss_mb": mem.get((engine, cores, files))})

cols = list(table[0])
(OUT / "results.csv").write_text("\n".join([",".join(cols)] + [",".join(str(r[c]) for c in cols) for r in table]) + "\n", encoding="utf-8")
md = ["| Motor | Núcleos | Filas | Mediana (s) | Mín–máx (s) | Filas/s | Pico RSS (MB) |", "|---|---|---|---|---|---|---|"]
for r in table:
    md.append(f"| {r['motor']} | {r['nucleos']} | {r['filas']:,} | {r['mediana_s']} | {r['min_s']}–{r['max_s']} | {r['filas_por_s']:,} | {r['pico_rss_mb']} |")
(OUT / "results.md").write_text("\n".join(md) + "\n", encoding="utf-8")
print("\n".join(md))

# ---- graficas ----
colors = {"dask": "#1f77b4", "spark": "#ff7f0e"}
fig, ax = plt.subplots(1, 2, figsize=(11, 4))
for engine in ("dask", "spark"):
    for cores, ls in ((2, "--"), (4, "-")):
        pts = [(rows_by_files[f], statistics.median(runs[(engine, cores, f)])) for f in (2, 4, 8) if (engine, cores, f) in runs]
        ax[0].plot([p[0] / 1e6 for p in pts], [p[1] for p in pts], ls, marker="o", color=colors[engine], label=f"{engine} · {cores} núcleos")
ax[0].set_xlabel("Filas (millones)")
ax[0].set_ylabel("Tiempo, mediana (s)")
ax[0].set_title("Tiempo de agregación por grilla")
ax[0].grid(alpha=.3)
ax[0].legend()
labels, vals, cl = [], [], []
for engine in ("dask", "spark"):
    for cores in (2, 4):
        if (engine, cores, 8) in mem:
            labels.append(f"{engine}\n{cores} núcleos")
            vals.append(mem[(engine, cores, 8)])
            cl.append(colors[engine])
ax[1].bar(labels, vals, color=cl)
for i, v in enumerate(vals):
    ax[1].text(i, v, f"{v:.0f}", ha="center", va="bottom")
ax[1].set_ylabel("Pico de memoria RSS (MB)")
ax[1].set_title(f"Memoria con {rows_by_files[8]/1e6:.2f} M filas")
fig.tight_layout()
fig.savefig(OUT / "benchmark.png", dpi=140)
print("Listo:", OUT)
