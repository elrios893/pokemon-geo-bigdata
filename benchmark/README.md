# Benchmark Dask vs Spark

**Operación (idéntica en ambos):** sobre el Parquet limpio (`data/clean/pokemon_spawns`), asignar cada avistamiento a una
celda de grilla `(floor(lng/0.01), floor(lat/0.01))` y contar, por celda, avistamientos y especies distintas.
El resultado completo vuelve al driver. Se verifica que ambos motores producen **el mismo resultado**
(celdas, total y especies por celda) antes de comparar tiempos (`report.py` aborta si no coincide).

**Configuraciones:** 1 worker (2 núcleos) y 2 workers (4 núcleos) en cada motor; 0,56 / 1,12 / 2,24 M de filas
(2, 4 y 8 archivos Parquet). Dask: 2 hilos y 1 GB por worker. Spark: ejecutores de 2 núcleos y 768 MB.
Cada combinación: workers/aplicación reiniciados (memoria limpia), 1 corrida de calentamiento (no cuenta) y 3 medidas.
**Tiempo:** de pared, solo el cálculo (excluye arranque del cluster/sesión). **Memoria:** pico de RSS sumado de los
procesos que participan (Dask: muestreo cada 0,2 s de los workers; Spark: métricas `ProcessTreeJVMRSSMemory` de los ejecutores).

Reproducir: `benchmark/run_bench.sh --reset` y `python benchmark/report.py` (≈ 15 min). Datos crudos: `results/results.jsonl`.

## Resultados (2.239.470 filas)

| Motor | Núcleos | Mediana (s) | Filas/s | Pico RSS (MB) |
|---|---|---|---|---|
| Dask | 2 | 3,54 | 632.440 | 298 |
| Dask | 4 | 2,11 | 1.062.367 | 537 |
| Spark | 2 | 4,03 | 555.700 | 682 |
| Spark | 4 | 4,56 | 491.112 | 1.239 |

Tabla completa en `results/results.md`; gráfica en `results/benchmark.png`.

## Lectura

- A este tamaño **Dask fue más rápido y usó menos memoria**: 1,1× más rápido que Spark con 2 núcleos y 2,2× con 4; casi la mitad de la memoria.
- **Dask escala con los núcleos** (3,54 s → 2,11 s, 1,7×). **Spark no mejoró** al pasar de 2 a 4 núcleos (4,03 s → 4,56 s) en este volumen, y consume el doble de memoria con dos ejecutores. No se midió la causa; lo esperable es que, con ~2 M de filas, pesen más la planificación, el shuffle y la serialización que el cálculo.
- **Calentamiento:** la primera corrida de Spark tardó ~11 s (JVM, planificación, caché de archivos) frente a 2,6–4 s en Dask. No se cuenta, pero importa si cada consulta arranca en frío.
- Con tiempos de 1 a 5 s, las diferencias pequeñas (p. ej. 1,88 s frente a 1,90 s) están dentro del ruido; el rango mín–máx está en la tabla.

## Limitaciones (decirlas en la defensa)

- Un solo equipo con 12 CPU virtuales y 8 GB para todos los contenedores: es un cluster simulado; no se midió red real entre máquinas.
- Solo hasta 2,24 M filas. **No se puede concluir** cómo se comportarían con las 8,9 M filas completas, donde Spark suele ganar terreno; es una hipótesis sin medir.
- La agregación `nunique` nativa del groupby de Dask (con conjuntos de Python, patrón `dd.Aggregation` de la documentación) fue ~12× más lenta (13 s frente a 1,1 s con 0,56 M filas). Se usó la forma equivalente a la que hace Spark internamente (deduplicar (celda, especie) y contar), con el mismo resultado. Un benchmark ingenuo habría favorecido a Spark.
- El Parquet limpio guarda timestamps en nanosegundos y Spark 3.5 no los lee; el benchmark de Spark usa un esquema explícito con solo las 3 columnas necesarias.

## Fuentes

- Dask `dd.Aggregation`: https://docs.dask.org/en/stable/generated/dask.dataframe.Aggregation.html
- Métricas de ejecutores de Spark (`ProcessTreeJVMRSSMemory`): https://spark.apache.org/docs/3.5.3/monitoring.html
