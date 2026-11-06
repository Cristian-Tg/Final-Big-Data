"""Sesion de Spark contra el cluster, con el conector de MongoDB."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common import config


def crear_spark(nombre: str, particiones: int = 8) -> SparkSession:
    return (SparkSession.builder.appName(nombre)
            .master(config.SPARK_MASTER_URL)
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1500m"))
            .config("spark.sql.shuffle.partitions", str(particiones))
            .config("spark.mongodb.read.connection.uri", config.MONGO_URI)
            .config("spark.mongodb.write.connection.uri", config.MONGO_URI)
            .getOrCreate())
