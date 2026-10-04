# Plan de trabajo — Big Data Geoespacial

> Asignatura: Big Data · Docente: Andrés Felipe Hernández Marulanda
> Publicación: 24 sep 2026 · **Entrega y sustentación: 9 oct 2026**
> Modalidad de equipo: **individual**
> Estado del documento: vivo — se actualiza a medida que avanza el proyecto.

## 1. Objetivo y alcance

Construir un sistema completo (no piezas sueltas) que:

1. Descargue automáticamente un dataset geoespacial desde Kaggle (vía API, con token como credencial, nunca en el repo).
2. Lo limpie y particione con **Dask**.
3. Lo almacene en **MongoDB** como GeoJSON con índice `2dsphere`.
4. Lo procese de forma distribuida con **Spark** (agregaciones espaciales y temporales).
5. Lo consulte por ubicación (`$near`, `$geoWithin`, `$geoNear`) con parámetros variables.
6. Lo exponga mediante una **API Flask**.
7. Se despliegue solo mediante **Jenkins** (Jenkinsfile versionado) cada vez que cambia el código en `main`, con pruebas que bloqueen el despliegue si fallan.
8. Corra completo en **Docker Compose** y se pueda levantar en otra máquina sin pasos manuales ocultos.

**Criterio de "terminado":** en un equipo limpio, con Docker instalado y las variables de entorno configuradas, `docker compose up -d` (más la carga inicial de datos) deja el sistema operativo sin intervención manual adicional.

## 2. Datasets

### 2.1 Dataset principal — Pokémon GO "Catch Them All" (estado: definido, pendiente de reportar/confirmar con el docente)

- **Contenido:** avistamientos (spawns) de Pokémon GO con ubicaciones reales de todo el mundo, recolectados de apps de rastreo colaborativo en 2016. Rango real (perfilado, §2.8): **13 jul – 2 oct 2016**, pero ~94 % de las filas caen entre el **21 y el 26 de septiembre**.
- **Tamaño (verificado localmente):** 4 archivos NDJSON de ~520 MB c/u (~2 GB), **~8,96 M de registros** (2.242.211 + 2.242.111 + 2.242.211 + 2.238.311 líneas).
- **Formato:** un documento por línea, export de `mongoexport`; la ubicación **ya es GeoJSON**:
  ```json
  {"localTime":"07:26:25","source":"POKESNIPER","appearedOn":{"$date":"2016-09-21T12:26:25.000Z"},
   "pokemonId":134,"_id":{"$oid":"57e27f5d4856fa0012d30faa"},
   "location":{"coordinates":[-73.972765,40.77907],"type":"Point"},"__v":0}
  ```
- **Campos:** `pokemonId` (1–151), `appearedOn` (UTC), `localTime` (**no fiable como hora local**, ver §2.8), `source` (POKESNIPER, POKERADAR, …), `location` (Point, `[lng, lat]`), `_id`, `__v`.
- **Origen y descarga:** los archivos están alojados en Kaggle como adjuntos de un hilo de discusión del dataset *Predict'em All* (`semioniy/predictemall`, discusión 24246), **no** como dataset descargable con la API. Enlaces públicos directos (verificados, HTTP 200, ~225 MB comprimidos en total):
  - part1: `https://storage.googleapis.com/kaggle-forum-message-attachments/138703/5004/catchemall_part1.zip`
  - part2: `https://storage.googleapis.com/kaggle-forum-message-attachments/138703/5006/catchemall_part2.zip`
  - part3: `https://storage.googleapis.com/kaggle-forum-message-attachments/138703/5005/catchemall_part3.zip`
  - part4: `https://storage.googleapis.com/kaggle-forum-message-attachments/138703/5007/catchemall_part4.zip`
- **Estrategia de ingesta:** el pipeline descarga y descomprime por URL (`requests` en streaming), solo si el archivo no existe en `data/` (caché local, fuera de git). No requiere token. Se documenta en el README y el informe la procedencia (Kaggle, adjuntos del hilo) y la autoría del dataset original, y se registra como desviación frente a "descarga con la API de Kaggle" (ver §2.4).
- **Variable `SAMPLE_ROWS`:** ver §2.6 (muestra de trabajo de 2.000.000, mínimo exigido 1.000.000).
- **Particularidades a tratar en la limpieza (§7):** concentración temporal en pocos días (el análisis por mes/año aporta poco; el foco es **hora y día**), 3.993 filas sin `pokemonId`, duplicados exactos (0,13 %), `localTime` poco fiable y fechas en formato extendido de Mongo (`$date`, `$oid`). Resultados completos del perfilado en §2.8.

### 2.2 Extensión opcional — CS:GO Competitive Matchmaking Damage (Fase F8)

- Kaggle: `skihikingkevin/csgo-matchmaking-damage` (archivos `mm_*` ≈ 955k eventos de daño; `esea_master_dmg_demos.part1/part2.csv` ≈ 1,1 GB).
- Cada fila: mapa, ronda, arma, daño y posición del atacante y de la víctima (`att_pos_x/y`, `vic_pos_x/y`).
- **Limitación:** las posiciones son coordenadas del mapa del juego (unidades de Source Engine, ≈ ±3000, origen distinto por mapa), no lat/lng. Se normalizarían por mapa a una caja lon/lat sintética con transformación lineal para poder usar `2dsphere`; las distancias serían "metros ficticios". Cada mapa es un espacio distinto (campo `map`).
- **Alcance:** solo si F0–F7 están completas y verificadas antes del 9 oct. Adaptador propio (colección `csgo_damage`, 2 endpoints `/csgo/near` y `/csgo/stats`). Si no alcanza, se documenta como trabajo futuro. **No se muestra en la sustentación si queda a medias.**
- Este dataset sí se puede descargar con la API oficial de Kaggle (token como credencial de Jenkins).

### 2.3 Respaldo — Chicago Crime

`currie32/crimes-in-chicago` (~7,9 M filas, ~1,9 GB, 4 CSV, 2001–2017; lat/lng y fecha). Solo se usa si el docente rechaza el dataset principal. Descarga con la API de Kaggle.

### 2.4 Consultas pendientes al docente

1. ¿Se acepta el dataset Pokémon GO "Catch Them All", descargado por URL directa de los adjuntos de Kaggle y no por `kaggle datasets download`? (Reportarlo también para garantizar unicidad.)
2. ¿Se acepta CS:GO con coordenadas de juego normalizadas a lon/lat sintéticas, como extensión?

### 2.6 Muestra de trabajo (decisión de alcance)

- **Motivo:** el dataset completo (~8,96 M) más MongoDB, imágenes Docker y volúmenes no caben cómodamente en el disco del equipo (~8 GB libres) y alarga innecesariamente la ingesta. El enunciado permite trabajar con una muestra de **≥ 1.000.000 de registros** si la descarga completa queda automatizada.
- **Muestra objetivo:** `SAMPLE_ROWS = 2.000.000` (500.000 por cada parte). Margen de seguridad: tras limpieza y deduplicación deben quedar **≥ 1.000.000** registros; si no, se sube `SAMPLE_ROWS`.
- **Criterio de muestreo:** primeras N filas de cada parte, porque las partes siguen aproximadamente el orden de inserción (el perfilado muestra que cada parte contiene unas pocas filas tempranas de julio, pero el grueso avanza de part1 ≈ 21–22 sep a part4 ≈ 25–26 sep). No son estrictamente cronológicas, pero la muestra cubre 13 jul – 25 sep y conserva la dispersión geográfica (cientos de ciudades) sin cargar todo. El criterio queda documentado y es reproducible (no aleatorio).
- La descarga y descompresión de los 4 ZIP completos siguen automatizadas; solo la **carga a MongoDB** se limita con `SAMPLE_ROWS` (valor `0` = todo).

### 2.7 Enriquecimiento con PokeAPI

- Cada registro trae `pokemonId` (1–151). Se enriquece con nombre, tipos, estadísticas base, altura y peso desde `https://pokeapi.co/api/v2/pokemon/{id}`.
- **Respeto de límites:** solo hay 151 ids distintos, así que se hacen **151 peticiones una única vez** (script `ingest/build_pokemon_catalog.py`), secuenciales, con pausa (~0,3 s), `User-Agent` identificable, reintentos con backoff exponencial ante 429/5xx y **caché en disco**. El resultado se guarda como `data/catalog/pokemon_catalog.json` (solo los campos necesarios, unos pocos KB) y se versiona en el repo para no depender de la API en tiempo de ejecución ni en Jenkins.
- **Nunca** se consulta PokeAPI por cada registro ni desde la API Flask en cada petición.
- El catálogo se carga en MongoDB (`pokemon_catalog`) y se une por `pokemonId`: Spark lo usa para agregar por tipo/especie (p. ej. hotspots de tipo agua) y la API devuelve `name` y `types` junto con cada avistamiento.
- Si PokeAPI no está disponible al reconstruir el catálogo, el pipeline usa el JSON versionado.
### 2.8 Resultados del perfilado (4 archivos, 8.964.844 filas, script `ingest/profile_data.py`)

| Aspecto | Hallazgo | Implicación |
|---|---|---|
| Formato | 0 JSON inválidos, 0 líneas vacías; solo 2 variantes de claves (la 2.ª sin `pokemonId`: 3.993 filas) | Lectura por líneas segura |
| Coordenadas | 100 % válidas (rango, tipo, `(0,0)`); lat −54,8 a 68,9; lng −159,8 a 175,3 | El filtro de coordenadas no elimina nada |
| `_id` | 0 repetidos | No hay solapamiento entre partes |
| Duplicados por evento | 11.940 (0,133 %); 1.356 en la muestra de 2 M | Deduplicar |
| Fechas (UTC) | 13 jul – 2 oct 2016; ~94 % entre el 21 y el 26 sep (pico 24 sep: 2,28 M) | Agregar por hora y día; mes/año aporta poco |
| Fuente (`source`) | POKERADAR 90,6 %; SKIPLAGGED 7,0 %; POKECREW 2,0 %; resto < 0,3 % | Datos de apps colaborativas: sesgo hacia ciudades con muchos usuarios |
| Geografía | 3.313 celdas de 1°; mayores: Nueva York/Filadelfia, Reino Unido, Toronto, Ámsterdam, Bahía de San Francisco | Hotspots dominados por cobertura de las apps |
| `pokemonId` | 145 de 151 distintos; ausentes 132 (Ditto), 144–146 (aves legendarias), 150–151 (Mewtwo, Mew) | Esperable: no aparecían como salvajes |
| `localTime` | Desfases múltiplos de 15 min, pero **no son hora local real** (ver §7, regla 5) | Derivar la hora local de UTC y longitud |
| Muestra de 2 M | 2.000.000 con coordenadas válidas, 1.356 duplicados; fechas 13 jul – 25 sep | ≈ 1,997 M útiles tras limpiar, por encima del mínimo de 1 M |

### 2.9 Perfilado espacial: puntos de aparición repetidos (script `ingest/profile_spatial.py`)

Se midió sobre la población completa (8.944.697 filas útiles, sin duplicados exactos) y se comparó con dos formas de muestrear ~2 M de filas: las **primeras 500.000 de cada parte** (criterio de §2.6) y **una de cada 4 filas** (muestreo sistemático). Punto = coordenada exacta (redondeada a 1e-6°, ≈ 0,1 m).

| Medida | Completo | Primeras 500 k/parte | Una de cada 4 |
|---|---|---|---|
| Avistamientos | 8.944.697 | 1.995.110 | 2.236.173 |
| Puntos distintos | 3.148.518 | 1.176.511 | 1.301.602 |
| Avistamientos por punto (media) | 2,84 | 1,70 | 1,72 |
| % de avistamientos en puntos vistos > 1 vez | **81,2 %** | 57,3 % | 58,0 % |
| % de puntos vistos una sola vez | 53,4 % | 72,4 % | 72,1 % |
| Máximo en un solo punto | 277 | 88 | 72 |
| Días distintos cubiertos | 82 | 75 | 81 |
| % viernes (UTC) | 7,4 | **0,7** | 7,4 |

Relación entre avistamientos que comparten coordenada (pares consecutivos en el tiempo dentro del mismo punto, población completa, 5,8 M pares):

- **Los puntos son de aparición, no de eventos únicos:** la misma especie se repite en el siguiente avistamiento del punto el 25,1 % de las veces, frente al 5,7 % que habría si la especie fuera independiente del punto (4,4×). Un punto de agua vuelve a dar agua el 66,0 % de las veces (contra 17,5 % global); un punto no de agua, solo el 8,5 %.
- **Ritmo:** mediana de 4 h entre observaciones del mismo punto; 86 % en menos de 24 h. Con brecha de 1–24 h la especie coincide el 29 %; con ≤ 15 min, 18 %.
- **Misma fuente:** el 99 % de los pares consecutivos viene de la misma app, es decir, los puntos repetidos reflejan que una app consulta repetidamente la misma zona.
- **Casi-duplicados reales:** misma especie, mismo punto, ≤ 15 min (un aparecimiento dura como máximo ese tiempo): 95.224 filas en la población (1,07 %); 0,23 % en la muestra sistemática.

**Decisiones**

1. **No se eliminan registros por compartir posición.** Son observaciones distintas (otro momento, a menudo otra especie) y su relación es justamente una propiedad del dataset (persistencia del tipo en cada punto). Borrarlos destruiría la señal que se analiza. Tampoco se añade una regla de ventana de 15 min: afectaría 0,2 % de la muestra.
2. **El porcentaje de posiciones repetidas no puede "bajarse" con el muestreo:** adelgazar la muestra reduce las coincidencias (57–58 % de avistamientos en puntos repetidos frente a 81 % real) pero no es una propiedad de los datos. Se documenta que la muestra **subestima** la repetición.
3. **Se cambia el criterio de muestreo de "primeras N filas" a "una de cada K filas"** (`SAMPLE_STRIDE`): conserva la distribución temporal del dataset completo (viernes 7,4 % como en la población, frente a 0,7 % antes) con el mismo tamaño y mismo costo. §2.6 queda reemplazado por este criterio una vez aplicado.
4. Los hotspots se interpretan junto con el número de **puntos distintos** por celda; `agg_grid` y `agg_hotspots` incorporarán `puntos_distintos` (conteo exacto de coordenadas distintas).

### 2.5 Diseño agnóstico al dataset

La ingesta se parametriza con un archivo de configuración (`config/datasets/<nombre>.yml`): fuente (URL o slug Kaggle), mapeo de columnas → esquema GeoJSON (`id`, `timestamp`, `location`, atributos), bounding box (si aplica) y reglas de limpieza. Añadir o cambiar un dataset no modifica la infraestructura, Jenkins ni la API base.
## 3. Stack tecnológico (versiones fijadas)

| Capa | Tecnología |
|---|---|
| Orquestación | Docker + Docker Compose v2 |
| Almacenamiento | MongoDB 7.0 — documentos GeoJSON `Point`, índice `2dsphere` |
| Ingesta / limpieza | Dask `distributed` (2024.x/2025.x) — 1 scheduler + ≥ 2 workers |
| Procesamiento distribuido | Apache Spark 3.5.x (imagen oficial `apache/spark`) — 1 master + ≥ 1 worker, con `org.mongodb.spark:mongo-spark-connector_2.12:10.4.x` |
| API | Python 3.11, Flask 3.x + gunicorn; mapa Leaflet opcional (no se califica aparte) |
| CI/CD | Jenkins LTS (jdk17), con Docker CLI disponible y socket montado; `Jenkinsfile` declarativo versionado en el repo |
| Control de versiones | GitHub, con webhook que dispare Jenkins en push/merge a `main` |

## 4. Librerías principales (Python)

- Ingesta/limpieza: `dask[distributed]`, `pandas`, `pyarrow`, `kaggle`, `pymongo`
- Procesamiento: `pyspark==3.5.*`
- Geoespacial/validación: `geojson`, `shapely`, `pygeohash` (opcional, para agregación por geohash)
- API: `flask`, `gunicorn`, `python-dotenv`
- Pruebas: `pytest`, `requests`
- Benchmark: `psutil` (memoria), `matplotlib` (gráficas comparativas)

Cada servicio mantiene su propio `requirements.txt` con versiones fijadas (pines exactos), para builds reproducibles.

## 5. Arquitectura y estructura del repositorio

```
docker-compose.yml        Jenkinsfile        README.md
.env.example              .gitignore

docs/
  PLAN_DE_TRABAJO.md
  arquitectura.(png|drawio)
  informe/                 (informe técnico final, ≤10 páginas)
  decisiones/              (ADRs cortos)

ingest/                    (Dask)
  Dockerfile
  download.py              (API de Kaggle)
  clean_load.py            (limpieza + carga por lotes a MongoDB)
  build_pokemon_catalog.py (PokeAPI: 151 ids, una vez, con caché)

processing/                (Spark)
  jobs/
    grid_agg.py            (conteo por celda de grilla / geohash)
    hourly_agg.py           (comportamiento por hora/día/mes)
    hotspots.py             (zonas de alta concentración)

api/                       (Flask)
  Dockerfile
  app/
    routes.py
    queries.py             ($near, $geoWithin, $geoNear)
  tests/

benchmark/
  dask_grid.py
  spark_grid.py
  run_bench.sh
  results/                 (CSV + gráficas tiempo/memoria)

tests/
  unit/                    (limpieza, GeoJSON, validación de parámetros)
  smoke/                   (pruebas contra la API ya levantada, usadas por Jenkins)
```

**Flujo de despliegue:** GitHub (push/merge a `main`) → webhook → Jenkins → `docker compose build && up` → pruebas (pytest + smoke) → si pasan, queda desplegado; si fallan, el pipeline se detiene antes del despliegue.

## 6. Fases de trabajo

| Fase | Fechas | Rama | Entregable verificable |
|---|---|---|---|
| F0 — Setup | 3 oct | `chore/setup` | Repo creado, este plan, `.gitignore` ✅ · dataset reportado al docente ⚠️ **PENDIENTE** |
| F1 — Infraestructura + Jenkins temprano | 3–4 oct | `feat/infra` | ✅ **COMPLETADA (4 oct)**: `docker-compose.yml` con todos los servicios obligatorios levantando; Jenkinsfile mínimo (checkout + validar compose + build) disparado por webhook (PR #1, #2; build #3 automático en verde) |
| F2 — Ingesta con Dask | 4–5 oct | `feat/ingest` | Catálogo PokeAPI ✅ · descarga por URL (ZIP → NDJSON), muestra de 2 M, limpieza justificada, carga por lotes a MongoDB, índice `2dsphere` · **servicio `ingest` de un solo uso en el compose** para que `docker compose up` cargue los datos sin pasos manuales |
| F3 — Procesamiento con Spark | 5 oct | `feat/spark` | Agregación por grilla/geohash, identificación de hotspots, comportamiento por hora/día/mes → colecciones `agg_grid`, `agg_hotspots`, `agg_time` |
| F4 — Consultas geoespaciales + API | 6 oct | `feat/api` | Endpoints: `GET /near` (`$near` por lat/lng/radio), `POST /within` (`$geoWithin`, polígono GeoJSON), `GET /stats/*` (resultados de Spark), `GET /geonear` (`$geoNear`), `GET /health` |
| F5 — CI/CD completo | 6–7 oct | `feat/ci` | Jenkinsfile con stages: checkout → build → pytest → levantar servicios → pruebas smoke contra la API → deploy (bloqueado si algo falla) |
| F6 — Benchmark Dask vs Spark | 7–8 oct | `feat/benchmark` | Misma operación pesada (agregación por grilla) ejecutada en Dask (2 vs 4 workers) y Spark (1 vs 2 workers); tiempo y memoria medidos y comparados |
| F7 — Cierre y documentación | 8 oct | `docs/final` | README "desde cero" verificado, informe técnico ≤10 páginas, ensayo de modificación en vivo, tag `v1.0.0` |
| F8 — *Opcional*: extensión CS:GO | solo si F0–F7 están completas | `feat/csgo` | Adaptador CS:GO (coordenadas normalizadas por mapa), colección `csgo_damage`, `/csgo/near` y `/csgo/stats`; si no alcanza, queda como trabajo futuro |
| **Entrega** | **9 oct** | — | Enlace al repositorio + informe técnico |

## 7. Reglas de limpieza (a justificar en el informe)

Basadas en el perfilado de los 4 archivos (§2.8); se ajustan por dataset mediante su archivo de configuración (§2.5). Filas afectadas sobre las 8.964.844 originales:

1. **Coordenadas nulas, no numéricas, fuera de rango (`|lat| ≤ 90`, `|lng| ≤ 180`) o `(0,0)`:** se implementa el filtro, pero el perfilado muestra **0 filas afectadas** (100 % de coordenadas válidas). Se mantiene como defensa y se reporta honestamente que no elimina datos.
2. **Descartar filas sin `pokemonId`:** **3.993 filas (0,045 %)** no tienen ese campo; sin él no se puede enriquecer ni agregar por especie. También se valida `pokemonId` ∈ [1, 151].
3. **Deduplicar por `(pokemonId, lng, lat, appearedOn)`:** **11.940 duplicados exactos (0,133 %)**. No hay `_id` repetidos, así que son el mismo evento reportado de nuevo, no un artefacto de las partes.
4. **Aplanar fechas extendidas de Mongo:** `appearedOn.$date` → `datetime` UTC; `_id.$oid` → `id` de texto.
5. **No usar `localTime` como hora local:** verificado que no lo es (Nueva York da UTC−5 en vez de UTC−4, California UTC−8 en vez de UTC−7, Reino Unido UTC+0 en vez de +1, Ámsterdam UTC+0 en vez de +2: ignora el horario de verano y en Europa ni acierta la zona). La **hora local se deriva de `appearedOn` (UTC) y la longitud** (aprox. solar, `lng/15` h), decisión documentada en el informe. `source` se conserva para comparar fuentes.
6. **Verificar** que `location` queda como GeoJSON `Point` válido con orden `[lng, lat]`.
7. **Conservar solo** las columnas necesarias para las consultas y agregaciones.

Tamaño esperado tras limpiar: ≈ 8,95 M sobre el conjunto completo y **≈ 1,997 M sobre la muestra de 2 M** (mínimo exigido: 1 M). Se registra el conteo real antes/después de cada regla para el informe.

## 8. Restricciones

- **Sin secretos en el repo.** Las credenciales de MongoDB viven en `.env` (solo se versiona `.env.example`) y en credenciales de Jenkins (`withCredentials`). El dataset principal se descarga por URL pública y no requiere token; si se usa un dataset descargado con la API de Kaggle (CS:GO, Chicago), su token se guarda como credencial *Secret file* en Jenkins y nunca en el repo.
- Todo corre en contenedores; el sistema se levanta con un solo comando.
- Healthchecks y `depends_on: condition: service_healthy` para evitar condiciones de carrera entre servicios.
- Las consultas geoespaciales siempre reciben parámetros (nunca valores fijos en el código); se valida la entrada en la API (400 si es inválida).
- Límites de memoria por servicio en `docker-compose.yml`; si el equipo no tiene recursos para el dataset completo, se usa una muestra de ≥ 1.000.000 de registros (con la descarga completa igualmente automatizada).
- Cualquier código de terceros usado se cita en el README/informe.
- Plazo fijo: 9 de octubre — no se reciben entregas posteriores.

## 9. Forma de trabajo y trazabilidad

- `main` protegida: solo recibe cambios vía Pull Request desde ramas `feat/*`, `fix/*`, `docs/*`, `chore/*`.
- El merge a `main` dispara el webhook → Jenkins.
- Un *issue* de GitHub por tarea, agrupados en un *milestone* por fase; cada PR referencia `Closes #n`.
- Mensajes de commit en formato Conventional Commits (`feat(api): ...`, `fix(ingest): ...`), commits pequeños y frecuentes (el historial de commits se revisa y debe reflejar trabajo real).
- Se etiqueta (`git tag`) el cierre de cada fase (`v0.1.0` … `v1.0.0`), con `CHANGELOG.md`.
- Decisiones técnicas relevantes se documentan como ADRs breves en `docs/decisiones/`.
- Definición de "hecho" por tarea: pasa `pytest`, pasa el pipeline de Jenkins, y queda documentada en el README si cambia la forma de uso del sistema.

## 10. Pruebas

- **Unitarias** (`tests/unit`): reglas de limpieza, construcción del GeoJSON, validación de parámetros de la API, construcción de las queries de Mongo.
- **Smoke** (`tests/smoke`): ejecutadas por Jenkins contra la API ya levantada — `/health`, `/near`, `/within`, `/stats/*`, `/geonear` responden 200 y con la forma esperada. Si fallan, el pipeline no despliega.

## 11. Entregables finales

- Repositorio de GitHub con:
  - README que explica cómo levantar el sistema desde cero (requisitos, variables de entorno, comandos, ejemplos `curl` de cada endpoint).
  - `docker-compose.yml` y `Jenkinsfile`.
- Informe técnico corto (≤ 10 páginas) con: diagrama de arquitectura, decisiones tomadas (y por qué), consultas geoespaciales implementadas, y el análisis comparativo Dask vs Spark con mediciones propias.

## 12. Preparación de la sustentación

Ensayo previo del escenario de "modificación en vivo": agregar un endpoint nuevo o una consulta sobre un polígono distinto (p. ej. otro `Community Area`) → commit → push → PR → merge → verificar que Jenkins construye y despliega la nueva versión, todo durante la sesión. Repasar cada componente del sistema para responder preguntas individuales sobre cualquier parte, no solo la propia.

## 13. Riesgos y mitigación

| Riesgo | Mitigación |
|---|---|
| Webhook de GitHub no llega a un Jenkins local | Exponer Jenkins con `ngrok` o `smee.io` |
| Límites/caída de PokeAPI | 151 peticiones únicas con pausa y backoff, catálogo versionado en el repo; nunca se llama por registro |
| Poco disco (~8 GB libres) | Muestra de 2 M, `data/` fuera de git, limpiar imágenes/volúmenes Docker no usados |
| Los enlaces de los adjuntos de Kaggle dejan de funcionar o cambian | Mantener la caché local en `data/`; guardar los ZIP también como respaldo propio (p. ej. dataset privado de Kaggle) y documentar el origen |
| El docente no acepta la descarga por URL o las coordenadas de CS:GO | Consulta temprana (§2.4); respaldo con Chicago Crime vía API de Kaggle |
| Tiempo insuficiente (6 días, trabajo individual) | F8 es opcional; se prioriza el sistema completo sobre Pokémon |
| ~9 M de registros pesan en memoria/carga | Particionado en Dask, inserts por lotes, límites de memoria y `SAMPLE_ROWS` |
| Memoria insuficiente para el dataset completo | Usar muestra ≥ 1M filas; límites de memoria por servicio en compose |
| Incompatibilidad de versiones Spark ↔ conector de Mongo | Fijar versiones exactas y pasarlas con `--packages` |
| Jenkins necesita ejecutar Docker | Montar el socket de Docker y añadir el usuario de Jenkins al grupo `docker` |
| El repo vive dentro de OneDrive | Evaluar clonar/trabajar fuera de la carpeta sincronizada por OneDrive para evitar bloqueos de archivos y volúmenes de Docker |

## 14. Próximos pasos inmediatos

1. **Reportar al docente** (urgente: el enunciado pide hacerlo *antes de empezar*) el dataset Pokémon GO "Catch Them All" y hacer las dos consultas de §2.4.
2. Antes de F2, correr un perfilado rápido sobre los 4 archivos (rango de fechas, distribución por `source`, coordenadas inválidas, duplicados) para fundamentar las reglas de limpieza con datos reales.
3. F2: ingesta con Dask, incluyendo un servicio `ingest` de un solo uso en el compose (criterio "un solo comando, sin pasos manuales ocultos").
4. F5 (anticipar el riesgo): resolver el montaje de volúmenes cuando Jenkins usa el Docker del host por socket, para poder desplegar en la sustentación.
5. Decidir si se protege `main` (obliga a trabajar siempre por PR).
6. Citar en README e informe toda fuente ajena (el enunciado trata como copia el código de otros equipos o repositorios públicos sin citar): origen del dataset, PokeAPI, `smee-client` y cualquier fragmento tomado de repositorios, con enlace y licencia.