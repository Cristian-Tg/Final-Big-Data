"""API de LifeMap."""
from __future__ import annotations

import json
import os
import re
import urllib.request

from flask import Flask, jsonify, redirect, request, send_from_directory
from pymongo.errors import OperationFailure, PyMongoError

from src.api import sentiment_model
from src.common import config
from src.common.geo import validate_polygon
from src.common.mongo import get_db

app = Flask(__name__)
LIMITE_MAX = 500
RADIO_MAX_M = 100_000
CAMPOS = {"name": 1, "city": 1, "stars": 1, "is_open": 1, "categories": 1,
          "categoria_principal": 1,
          "location": 1, "dias_negocio": 1, "evento": 1, "censurado": 1,
          "sentimiento_medio": 1}


class Invalido(ValueError):
    pass


@app.errorhandler(Invalido)
def _invalido(exc):
    return jsonify(error=str(exc)), 400


@app.errorhandler(OperationFailure)
def _consulta(exc):
    # MongoDB respondio y rechazo la consulta, por ejemplo un poligono que se cruza
    return jsonify(error="Consulta rechazada: revise los parametros"), 400


@app.errorhandler(PyMongoError)
def _mongo(exc):
    return jsonify(error="MongoDB no disponible"), 503


def db():
    return get_db(retries=2, delay=0.2, timeout_ms=1500)


def numero(nombre: str, minimo: float, maximo: float, defecto: float | None = None) -> float:
    crudo = request.args.get(nombre)
    if crudo is None:
        if defecto is None:
            raise Invalido(f"Falta el parametro {nombre}")
        return defecto
    try:
        valor = float(crudo)
    except ValueError as exc:
        raise Invalido(f"{nombre} debe ser numerico") from exc
    if not minimo <= valor <= maximo:
        raise Invalido(f"{nombre} debe estar entre {minimo} y {maximo}")
    return valor


def punto() -> dict:
    return {"type": "Point", "coordinates": [numero("lon", -180, 180), numero("lat", -90, 90)]}


def limpio(doc: dict) -> dict:
    doc["business_id"] = doc.pop("_id")
    return doc


def filtro_categoria() -> dict:
    categoria = request.args.get("category")
    if not categoria:
        return {}
    # El texto del usuario se busca literal, nunca como expresion regular
    return {"categories": {"$regex": re.escape(categoria), "$options": "i"}}


def sondear(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return "ok" if resp.status == 200 else "error"
    except OSError:
        return "caido"


ESTATICOS = os.path.join(os.path.dirname(__file__), "static")


@app.get("/")
def inicio():
    return redirect("/mapa")


@app.get("/mapa")
def mapa():
    return send_from_directory(ESTATICOS, "mapa.html")


@app.get("/health")
def health():
    try:
        db().command("ping")
        mongo = "ok"
    except (PyMongoError, RuntimeError):
        mongo = "caido"
    servicios = {"api": "ok", "mongodb": mongo, "spark": sondear(config.SPARK_MASTER_UI),
                 "dask": sondear(config.DASK_HEALTH_URL)}
    estado = "ok" if all(v == "ok" for v in servicios.values()) else "degradado"
    return jsonify(status=estado, services=servicios, commit=config.GIT_COMMIT)


@app.get("/businesses/near")
def near():
    radio = numero("radius_m", 1, RADIO_MAX_M, 1000)
    limite = int(numero("limit", 1, LIMITE_MAX, 50))
    consulta = {"location": {"$near": {"$geometry": punto(), "$maxDistance": radio}},
                **filtro_categoria()}
    docs = [limpio(d) for d in db().businesses.find(consulta, CAMPOS).limit(limite)]
    return jsonify(count=len(docs), results=docs)


@app.route("/businesses/within", methods=["GET", "POST"])
def within():
    if request.method == "POST":
        geometria = request.get_json(silent=True)
    else:
        try:
            geometria = json.loads(request.args.get("polygon", ""))
        except ValueError as exc:
            raise Invalido("polygon debe ser un GeoJSON valido") from exc
    if not isinstance(geometria, dict):
        raise Invalido("Se requiere un poligono GeoJSON")
    valido, mensaje = validate_polygon(geometria)
    if not valido:
        raise Invalido(mensaje)
    limite = int(numero("limit", 1, LIMITE_MAX, 100))
    consulta = {"location": {"$geoWithin": {"$geometry": geometria}}, **filtro_categoria()}
    docs = [limpio(d) for d in db().businesses.find(consulta, CAMPOS).limit(limite)]
    return jsonify(count=len(docs), results=docs)


@app.get("/zones/summary")
def zones():
    radio = numero("radius_m", 1, RADIO_MAX_M, 2000)
    tuberia = [
        {"$geoNear": {"near": punto(), "distanceField": "distancia_m", "maxDistance": radio,
                      "spherical": True, "query": filtro_categoria()}},
        {"$group": {"_id": None, "negocios": {"$sum": 1}, "cierres": {"$sum": "$evento"},
                    "dias_negocio": {"$sum": "$dias_negocio"},
                    "sentimiento_medio": {"$avg": "$sentimiento_medio"},
                    "estrellas": {"$avg": "$stars"}}},
        {"$project": {"_id": 0, "negocios": 1, "cierres": 1, "dias_negocio": 1,
                      "sentimiento_medio": 1, "estrellas": 1,
                      "tasa_cierre_pct": {"$cond": [
                          {"$gt": ["$dias_negocio", 0]},
                          {"$multiply": [{"$divide": ["$cierres",
                                                      {"$divide": ["$dias_negocio", 365]}]}, 100]},
                          None]}}},
    ]
    resultado = list(db().businesses.aggregate(tuberia))
    return jsonify(resultado[0] if resultado else {"negocios": 0})


@app.get("/stats/closure")
def closure():
    por = request.args.get("by", "category")
    colecciones = {"category": "closure_by_category", "geohash": "closure_by_geohash"}
    if por not in colecciones:
        raise Invalido("by debe ser category o geohash")
    minimo = int(numero("min_dias", 0, 1e9, config.MIN_DIAS_NEGOCIO))
    limite = int(numero("limit", 1, LIMITE_MAX, 50))
    docs = (db()[colecciones[por]].find({"dias_negocio": {"$gte": minimo}}, {"_id": 0})
            .sort("tasa_cierre_pct", -1).limit(limite))
    resultados = list(docs)
    return jsonify(by=por, min_dias=minimo, count=len(resultados), results=resultados)


@app.post("/predict")
def predict():
    cuerpo = request.get_json(silent=True) or {}
    texto = cuerpo.get("text")
    if not isinstance(texto, str) or not texto.strip():
        raise Invalido("Se requiere el campo text")
    modelo = sentiment_model.cargar()
    if modelo is None:
        return jsonify(error="Modelo no disponible"), 503
    return jsonify(sentiment_model.predecir(texto, modelo))
