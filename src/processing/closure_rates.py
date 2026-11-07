"""MapReduce de tasas de cierre y medicion de balanceo de carga."""
from __future__ import annotations

import json
import statistics
import time

from pyspark.sql import functions as F

from src.common import config
from src.common.geo import geohash_polygon
from src.common.metrics import tasa_cierre
from src.processing.common_spark import crear_spark

PARTICIONES = [2, 8, 32, 200]
REPETICIONES = 3


def mapreduce(rdd, clave: str, n: int | None = None) -> list:
    """(clave, (cierre, dias_negocio)) sumado con reduceByKey."""
    return (rdd.map(lambda r: (r[clave], (r["evento"], r["dias_negocio"])))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions=n)
            .collect())


def validar(df, resultado: list, clave: str) -> None:
    grupo = {r[clave]: (r["c"], r["d"]) for r in df.groupBy(clave).agg(
        F.sum("evento").alias("c"), F.sum("dias_negocio").alias("d")).collect()}
    if dict(resultado) != grupo:
        raise AssertionError(f"reduceByKey y groupBy no coinciden para {clave}")


def filas_por_particion(df) -> list[int]:
    return [r["count"] for r in df.groupBy(F.spark_partition_id().alias("p")).count()
            .orderBy("p").collect()]


def balanceo(spark, df, resenas) -> dict:
    medidas = {}
    for n in PARTICIONES:
        tiempos = []
        rdd = df.repartition(n).rdd.cache()
        rdd.count()
        for _ in range(REPETICIONES):
            inicio = time.perf_counter()
            mapreduce(rdd, "categoria_principal", n)
            tiempos.append(round(time.perf_counter() - inicio, 3))
        rdd.unpersist(blocking=True)
        medidas[n] = {"repeticiones": tiempos, "media": round(statistics.mean(tiempos), 3),
                      "desv": round(statistics.stdev(tiempos), 3)}
    # El sesgo se mide en las resenas: alli una ciudad o categoria concentra las filas
    filas = {"por_ciudad": filas_por_particion(resenas.repartition(8, "city")),
             "por_categoria": filas_por_particion(resenas.repartition(8, "categoria_principal")),
             "sin_clave": filas_por_particion(resenas.repartition(8))}
    return {"tiempos": medidas, "filas_por_particion": filas}


def main() -> None:
    spark = crear_spark("lifemap-tasas")
    df = (spark.read.format("mongodb").option("database", config.MONGO_DB)
          .option("collection", "businesses").load()
          .select(F.col("_id").alias("business_id"), "city", "categoria_principal", "geohash",
                  "evento", "dias_negocio").cache())
    resenas = (spark.read.parquet(f"{config.CURATED_DIR}/reviews")
               .select("business_id", "geohash", "sentimiento", "categoria_principal")
               .join(df.select("business_id", "city"), "business_id"))

    salida = []
    for clave, coleccion in (("categoria_principal", "closure_by_category"),
                             ("geohash", "closure_by_geohash")):
        resultado = mapreduce(df.rdd, clave)
        validar(df, resultado, clave)
        salida.append((clave, coleccion, resultado))

    negativas = {r["geohash"]: r["pct"] for r in resenas.groupBy("geohash").agg(
        (F.avg((F.col("sentimiento") == "negativo").cast("double")) * 100).alias("pct")).collect()}

    for clave, coleccion, resultado in salida:
        filas = []
        for valor, (cierres, dias) in resultado:
            doc = {"_id": valor, "clave": valor, "cierres": int(cierres), "dias_negocio": int(dias),
                   "tasa_cierre_pct": tasa_cierre(cierres, dias)}
            if clave == "geohash":
                doc["pct_negativas"] = negativas.get(valor)
                doc["geometry"] = json.dumps(geohash_polygon(valor))
            filas.append(doc)
        documentos = spark.createDataFrame(filas)
        if clave == "geohash":
            documentos = documentos.withColumn("geometry", F.from_json(
                "geometry", "type STRING, coordinates ARRAY<ARRAY<ARRAY<DOUBLE>>>"))
        (documentos.write.format("mongodb").mode("overwrite")
         .option("database", config.MONGO_DB).option("collection", coleccion).save())

    medidas = balanceo(spark, df, resenas)
    with open(f"{config.DATA_DIR}/balanceo.json", "w", encoding="utf-8") as fh:
        json.dump(medidas, fh, indent=2)
    print(json.dumps(medidas, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
