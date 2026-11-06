"""Inferencia de sentimiento sin JVM.

Reproduce el PipelineModel de Spark: los mismos tokens, las mismas stopwords y
el hash de HashingTF (MurmurHash3 de 32 bits con semilla 42).
"""
from __future__ import annotations

import os
import re
from collections import Counter

import mmh3
import numpy as np

from src.api.stopwords import STOPWORDS
from src.common import config

TOKEN = re.compile(r"[a-z']+")
_modelo = None


def cargar(ruta: str | None = None):
    global _modelo
    ruta = ruta or f"{config.MODEL_DIR}/sentiment_v1.npz"
    if _modelo is None and os.path.exists(ruta):
        with np.load(ruta) as z:
            _modelo = {k: z[k] for k in z.files}
    return _modelo


def indices(texto: str, n_features: int) -> Counter:
    palabras = [t for t in TOKEN.findall(texto.lower()) if t not in STOPWORDS]
    return Counter(mmh3.hash(p.encode("utf-8"), 42, signed=True) % n_features for p in palabras)


def predecir(texto: str, modelo: dict) -> dict:
    n = int(modelo["n_features"])
    conteo = indices(texto, n)
    columnas = np.fromiter(conteo, dtype=int)
    valores = np.array([conteo[c] for c in conteo], dtype=float) * modelo["idf"][columnas]
    puntajes = modelo["coef"][:, columnas] @ valores + modelo["intercept"]
    prob = np.exp(puntajes - puntajes.max())
    prob /= prob.sum()
    clases = [str(c) for c in modelo["clases"]]
    i = int(prob.argmax())
    return {"sentimiento": clases[i], "probabilidad": round(float(prob[i]), 4),
            "probabilidades": {c: round(float(p), 4) for c, p in zip(clases, prob)}}
