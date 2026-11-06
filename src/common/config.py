"""Configuracion central: todo sale de variables de entorno."""
from __future__ import annotations

import os

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/lifemap")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")

DATA_DIR = os.environ.get("DATA_DIR", "/data")
CURATED_DIR = f"{DATA_DIR}/curated"
MODEL_DIR = f"{DATA_DIR}/models"

KAGGLE_DATASET = os.environ.get("KAGGLE_DATASET", "yelp-dataset/yelp-dataset")
BUSINESS_FILE = "yelp_academic_dataset_business.json"
REVIEW_FILE = "yelp_academic_dataset_review.json"

# Alcance del equipo: area metropolitana (estados) y familia de categorias
METRO_STATES = [s.strip() for s in os.environ.get("METRO_STATES", "PA").split(",")]
CATEGORY_FAMILY = [c.strip() for c in
                   os.environ.get("CATEGORY_FAMILY", "Restaurants,Food").split(",")]

GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "5"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
DASK_SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
DASK_BLOCKSIZE = os.environ.get("DASK_BLOCKSIZE", "32MB")

SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_MASTER_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080/json/")
DASK_HEALTH_URL = os.environ.get("DASK_HEALTH_URL", "http://dask-scheduler:8787/health")

# 100 anos-negocio: con una tasa cercana al 5 % se esperan unos 5 cierres, lo
# minimo para que una sola falla no domine el resultado (ver docs/informe.md)
MIN_DIAS_NEGOCIO = int(os.environ.get("MIN_DIAS_NEGOCIO", "36500"))

GIT_COMMIT = os.environ.get("GIT_COMMIT", "desconocido")
