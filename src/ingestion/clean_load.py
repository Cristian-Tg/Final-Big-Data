"""Ingesta con Dask: lectura particionada, limpieza, union, vida y carga a MongoDB."""
from __future__ import annotations

import json
import os
import shutil
from collections import Counter

import dask
import dask.bag as db_bag
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from dask.utils import parse_bytes
from pymongo import InsertOne

from src.common import config
from src.common.geo import to_geojson_point
from src.common.metrics import etiqueta_sentimiento
from src.common.mongo import ensure_indexes, get_db
from src.ingestion.cleaning import leer_rango, limpiar_negocios, limpiar_resenas, vida_negocios

COL_NEG = ["business_id", "name", "city", "state", "latitude", "longitude", "stars",
           "review_count", "is_open", "categories"]
COL_RES = ["review_id", "business_id", "stars", "text", "date"]


def parsear(lineas: list[str], columnas: list[str]) -> pd.DataFrame:
    """Conserva solo las columnas usadas: el JSON completo no cabe en un worker."""
    filas = []
    for linea in lineas:
        registro = json.loads(linea)
        filas.append({c: registro.get(c) for c in columnas})
    return pd.DataFrame(filas, columns=columnas)


def particiones(ruta: str, columnas: list[str]) -> list:
    bloques = db_bag.read_text(ruta, blocksize=config.DASK_BLOCKSIZE).to_delayed()
    return [dask.delayed(parsear)(b, columnas) for b in bloques]


def particion_resenas(origen: str, inicio: int, fin: int, negocios: set[str],
                      negocio_info: pd.DataFrame, ruta: str) -> dict[str, int]:
    df, leidas = leer_rango(origen, inicio, fin, COL_RES, negocios)
    limpio, conteos = limpiar_resenas(df, negocios)
    conteos["leidas"] = leidas
    limpio = limpio.merge(negocio_info, on="business_id", how="inner")
    limpio["sentimiento"] = limpio["stars"].map(etiqueta_sentimiento)
    limpio.to_parquet(ruta, index=False, coerce_timestamps="us")
    return conteos


def cargar_resenas(ruta: str, duplicados: set[str]) -> int:
    db = get_db()
    df = pd.read_parquet(ruta)
    df = df[~df["review_id"].isin(duplicados)]
    ops = [InsertOne({"_id": r.review_id, "business_id": r.business_id, "stars": int(r.stars),
                      "text": r.text, "date": r.date.to_pydatetime(),
                      "sentimiento": r.sentimiento, "geohash": r.geohash})
           for r in df.itertuples(index=False)]
    for i in range(0, len(ops), config.BATCH_SIZE):
        db.reviews.bulk_write(ops[i:i + config.BATCH_SIZE], ordered=False)
    return len(ops)


def resumen_particion(ruta: str, duplicados: set[str]) -> pd.DataFrame:
    df = pd.read_parquet(ruta, columns=["review_id", "business_id", "date", "sentimiento"])
    df = df[~df["review_id"].isin(duplicados)]
    df = df.assign(neg=df.sentimiento == "negativo", pos=df.sentimiento == "positivo")
    return df.groupby("business_id").agg(first_review=("date", "min"), last_review=("date", "max"),
                                         n_reviews=("date", "size"), n_neg=("neg", "sum"),
                                         n_pos=("pos", "sum")).reset_index()


def documento_negocio(r) -> dict:
    return {"_id": r.business_id, "name": r.name, "city": r.city, "state": r.state,
            "location": to_geojson_point(r.latitude, r.longitude),
            "stars": float(r.stars), "review_count": int(r.review_count),
            "is_open": int(r.is_open), "categories": r.categories,
            "categoria_principal": r.categoria_principal, "geohash": r.geohash,
            "first_review": r.first_review.to_pydatetime(),
            "last_review": r.last_review.to_pydatetime(),
            "n_reviews": int(r.n_reviews), "n_neg": int(r.n_neg), "n_pos": int(r.n_pos),
            "sentimiento_medio": float((r.n_pos - r.n_neg) / r.n_reviews),
            "dias_negocio": int(r.dias_negocio), "dias_hasta_corte": int(r.dias_hasta_corte),
            "evento": int(r.evento), "censurado": bool(r.censurado)}


def main() -> None:
    Client(config.DASK_SCHEDULER)
    base = config.DATA_DIR
    stats: dict[str, dict] = {}

    negocios = pd.concat(dask.compute(*particiones(f"{base}/{config.BUSINESS_FILE}", COL_NEG)),
                         ignore_index=True)
    negocios, stats["negocios"] = limpiar_negocios(negocios)
    ids = set(negocios["business_id"])
    info = negocios[["business_id", "categoria_principal", "geohash"]]

    # Cada tarea lee su tramo del archivo y filtra ahi: los 5 GB nunca pasan por memoria
    shutil.rmtree(f"{config.CURATED_DIR}/reviews", ignore_errors=True)
    os.makedirs(f"{config.CURATED_DIR}/reviews")
    origen = f"{base}/{config.REVIEW_FILE}"
    tam = os.path.getsize(origen)
    paso = parse_bytes(config.DASK_BLOCKSIZE)
    inicios = list(range(0, tam, paso))
    rutas = [f"{config.CURATED_DIR}/reviews/part-{i:05d}.parquet" for i in range(len(inicios))]
    ids_d, info_d = dask.delayed(ids), dask.delayed(info)
    tareas = [dask.delayed(particion_resenas)(origen, ini, min(ini + paso, tam), ids_d, info_d, r)
              for ini, r in zip(inicios, rutas)]
    total = Counter()
    for conteo in dask.compute(*tareas):
        total.update(conteo)
    stats["resenas"] = dict(total)

    # Duplicados entre particiones: basta con la columna de identificadores
    leidas = dd.read_parquet(rutas, columns=["review_id"]).compute()
    duplicados = set(leidas.loc[leidas.duplicated(), "review_id"])
    stats["resenas"]["sin_duplicados"] = len(leidas) - len(duplicados)

    db = get_db()
    db.reviews.drop()
    db.businesses.drop()
    cargadas = sum(dask.compute(*[dask.delayed(cargar_resenas)(r, duplicados) for r in rutas]))

    partes = dask.compute(*[dask.delayed(resumen_particion)(r, duplicados) for r in rutas])
    resumen = (pd.concat(partes).groupby("business_id")
               .agg(first_review=("first_review", "min"), last_review=("last_review", "max"),
                    n_reviews=("n_reviews", "sum"), n_neg=("n_neg", "sum"),
                    n_pos=("n_pos", "sum")).reset_index())
    corte = resumen["last_review"].max()
    unidos = negocios.merge(resumen, on="business_id", how="inner")
    stats["negocios"]["con_resenas"] = len(unidos)
    vida = vida_negocios(unidos, corte)
    docs = [documento_negocio(r) for r in vida.itertuples(index=False)]
    for i in range(0, len(docs), config.BATCH_SIZE):
        db.businesses.insert_many(docs[i:i + config.BATCH_SIZE], ordered=False)
    ensure_indexes(db)

    vida.drop(columns=["categories"]).to_parquet(f"{config.CURATED_DIR}/businesses.parquet",
                                                 index=False, coerce_timestamps="us")
    stats["resenas"]["cargadas"] = cargadas
    stats["fecha_corte"] = str(corte.date())
    stats["cierres"] = int(vida["evento"].sum())
    with open(f"{base}/cleaning_stats.json", "w", encoding="utf-8") as fh:
        json.dump(stats, fh, indent=2)
    print(json.dumps(stats, indent=2), flush=True)


if __name__ == "__main__":
    main()
