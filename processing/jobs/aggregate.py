"""Agregaciones espaciales y temporales con Spark sobre pokemon.spawns (MongoDB Spark Connector).

Lee la coleccion `spawns` y el catalogo de PokeAPI, y escribe colecciones nuevas:

  agg_grid          conteo por celda de grilla (GRID_DEG grados; 0.01 ~ 1.1 km). La suma de `count` = total de documentos.
  agg_hotspots      las HOTSPOT_TOP celdas mas densas, con sus 3 especies dominantes (nombre y tipos del catalogo)
  agg_time          conteos por hora local, dia de la semana y fecha local
  agg_species       conteo por especie (nombre, tipos)
  agg_species_hour  conteo por especie y hora local
  agg_type          conteo por tipo de pokemon

Variables de entorno: MONGO_URI (obligatoria), MONGO_DB, GRID_DEG, HOTSPOT_TOP.
La "hora local" es la derivada en la limpieza (UTC + lng*4 min), no el localTime del dataset.
"""
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

URI = os.environ["MONGO_URI"]
DB = os.environ.get("MONGO_DB", "pokemon")
GRID = float(os.environ.get("GRID_DEG", "0.01"))
TOP = int(os.environ.get("HOTSPOT_TOP", "200"))


def write(df, name):
    (df.write.format("mongodb").mode("overwrite")
       .option("database", DB).option("collection", name).save())
    print(f"  -> {name}: {df.count():,} documentos", flush=True)


def main():
    t0 = time.time()
    spark = (SparkSession.builder.appName("pokemon-aggregations")
             .config("spark.mongodb.read.connection.uri", URI)
             .config("spark.mongodb.write.connection.uri", URI)
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    read = lambda coll: spark.read.format("mongodb").option("database", DB).option("collection", coll).load()
    spawns = read("spawns").select(
        "pokemonId", "local_hour", "local_dow", "local_date",
        F.col("location.coordinates")[0].alias("lng"),
        F.col("location.coordinates")[1].alias("lat"),
    )
    catalog = read("pokemon_catalog").select("pokemonId", "name", "types")
    spawns.cache()
    total = spawns.count()
    print(f"Documentos leidos: {total:,} | grilla {GRID} grados | top {TOP} hotspots", flush=True)

    # ---- grilla ----
    cell = spawns.withColumn("cx", F.floor(F.col("lng") / GRID).cast("long")) \
                 .withColumn("cy", F.floor(F.col("lat") / GRID).cast("long")) \
                 .withColumn("cell_id", F.concat_ws("_", F.col("cx"), F.col("cy")))
    cell.cache()

    def center(df):
        return (df.withColumn("lng_c", F.round((F.col("cx") + 0.5) * GRID, 6))
                  .withColumn("lat_c", F.round((F.col("cy") + 0.5) * GRID, 6))
                  .withColumn("location", F.struct(F.lit("Point").alias("type"),
                                                   F.array("lng_c", "lat_c").alias("coordinates"))))

    grid = center(cell.groupBy("cell_id", "cx", "cy")
                      .agg(F.count("*").alias("count"), F.countDistinct("pokemonId").alias("species")))
    grid.cache()
    write(grid.select(F.col("cell_id").alias("_id"), "cell_id", "count", "species", "location", "lng_c", "lat_c"),
          "agg_grid")

    # ---- hotspots: top N celdas + 3 especies dominantes ----
    top = grid.orderBy(F.col("count").desc(), F.col("cell_id")).limit(TOP) \
              .withColumn("rank", F.row_number().over(Window.orderBy(F.col("count").desc(), F.col("cell_id"))))
    cs = (cell.join(top.select("cell_id"), "cell_id")
              .groupBy("cell_id", "pokemonId").agg(F.count("*").alias("n"))
              .join(catalog, "pokemonId")
              .withColumn("r", F.row_number().over(Window.partitionBy("cell_id").orderBy(F.col("n").desc(), "pokemonId")))
              .filter("r <= 3")
              .groupBy("cell_id")
              .agg(F.sort_array(F.collect_list(F.struct(F.col("n").alias("count"), "pokemonId", "name", "types")),
                                asc=False).alias("top_species")))
    hot = top.join(cs, "cell_id")
    write(hot.select(F.col("rank").alias("_id"), "rank", "cell_id", "count", "species", "location", "top_species"),
          "agg_hotspots")

    # ---- tiempo ----
    t_hour = spawns.groupBy(F.lpad(F.col("local_hour").cast("string"), 2, "0").alias("key")).count() \
                   .withColumn("granularity", F.lit("hour"))
    t_dow = spawns.groupBy(F.col("local_dow").cast("string").alias("key")).count() \
                  .withColumn("granularity", F.lit("dow"))
    t_day = spawns.groupBy(F.col("local_date").alias("key")).count() \
                  .withColumn("granularity", F.lit("day"))
    time_df = t_hour.unionByName(t_dow).unionByName(t_day) \
                    .withColumn("_id", F.concat_ws(":", "granularity", "key"))
    write(time_df.select("_id", "granularity", "key", "count"), "agg_time")

    # ---- especies y tipos ----
    sp = spawns.groupBy("pokemonId").agg(F.count("*").alias("count")).join(catalog, "pokemonId")
    write(sp.select(F.col("pokemonId").alias("_id"), "pokemonId", "name", "types", "count"), "agg_species")

    sph = spawns.groupBy("pokemonId", "local_hour").agg(F.count("*").alias("count"))
    write(sph.select(F.concat_ws(":", "pokemonId", "local_hour").alias("_id"), "pokemonId", "local_hour", "count"),
          "agg_species_hour")

    typ = sp.select(F.explode("types").alias("type"), "count").groupBy("type").agg(F.sum("count").alias("count"))
    write(typ.select(F.col("type").alias("_id"), "type", "count"), "agg_type")

    # ---- invariantes verificables ----
    s_grid = grid.agg(F.sum("count")).first()[0]
    s_hour = t_hour.agg(F.sum("count")).first()[0]
    s_species = sp.agg(F.sum("count")).first()[0]
    print(f"Invariantes: total={total:,} | suma grilla={s_grid:,} | suma horas={s_hour:,} | suma especies={s_species:,}", flush=True)
    assert total == s_grid == s_hour == s_species, "las agregaciones no suman el total de documentos"
    print(f"OK en {time.time() - t0:.0f}s", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
