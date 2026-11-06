"""Conexion a MongoDB e indices del modelo de datos."""
from __future__ import annotations

import time

from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient
from pymongo.errors import PyMongoError

from src.common import config


def get_db(retries: int = 30, delay: float = 2.0, timeout_ms: int = 5000):
    """Base de datos de trabajo; reintenta porque Mongo puede estar arrancando."""
    error = None
    for _ in range(retries):
        try:
            client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=timeout_ms,
                                 tz_aware=True)
            client.admin.command("ping")
            return client[config.MONGO_DB]
        except PyMongoError as exc:
            error = exc
            time.sleep(delay)
    raise RuntimeError("No fue posible conectar a MongoDB") from error


def ensure_indexes(db) -> None:
    """Sin el indice 2dsphere, $near y $geoNear fallan."""
    db.businesses.create_index([("location", GEOSPHERE)], name="ix_location_2dsphere")
    db.businesses.create_index([("categoria_principal", ASCENDING)])
    db.businesses.create_index([("geohash", ASCENDING)])
    db.reviews.create_index([("business_id", ASCENDING), ("date", DESCENDING)])
    db.closure_by_category.create_index([("dias_negocio", DESCENDING)])
    db.closure_by_geohash.create_index([("dias_negocio", DESCENDING)])
    db.closure_by_geohash.create_index([("geometry", GEOSPHERE)])
