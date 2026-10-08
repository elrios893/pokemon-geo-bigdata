# Cuaderno de sustentación

`sustentacion.ipynb` recorre el sistema con datos reales: arquitectura, limpieza, GeoJSON e índice `2dsphere`, las tres consultas
(`$near`, `$geoWithin`, `$geoNear`), los resultados de Spark con su verificación, la API, el benchmark Dask vs Spark y el pipeline de Jenkins.
Es solo material de apoyo para explicar; no forma parte del despliegue.

```bash
docker compose up -d && docker compose --profile webhook up -d   # stack levantado
pip install -r notebooks/requirements.txt
jupyter lab notebooks/sustentacion.ipynb
```

Las credenciales de MongoDB se leen del `.env` de la raíz (no se imprimen). `sustentacion_respaldo.html` es la misma guía ya ejecutada, sin código, para abrirla sin el stack.
