"""Sentimiento: limpieza, stopwords, HashingTF/IDF, Naive Bayes vs. regresion logistica.

La particion es por negocio (fold = hash(business_id) % 10) para que un negocio
no quede en entrenamiento y prueba a la vez. El mejor modelo se guarda como
PipelineModel para la Entrega 2 y se exporta a numpy para que /predict no
necesite una JVM dentro de la API.
"""
from __future__ import annotations

import json
import os

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover, StringIndexer
from pyspark.sql import functions as F

from src.common import config
from src.processing.common_spark import crear_spark

N_FEATURES = 1 << 16
MUESTRA_ENTRENAMIENTO = float(os.environ.get("SENTIMENT_SAMPLE", "0.35"))


def etapas(clasificador):
    return Pipeline(stages=[
        StringIndexer(inputCol="sentimiento", outputCol="label"),
        RegexTokenizer(inputCol="text", outputCol="tokens", pattern="[a-z']+", gaps=False),
        StopWordsRemover(inputCol="tokens", outputCol="palabras"),
        HashingTF(inputCol="palabras", outputCol="tf", numFeatures=N_FEATURES),
        IDF(inputCol="tf", outputCol="tfidf", minDocFreq=5),
        clasificador,
    ])


def evaluar(pred, clases: list[str]) -> dict:
    filas = pred.groupBy("label", "prediction").count().collect()
    matriz = np.zeros((len(clases), len(clases)), dtype=int)
    for f in filas:
        matriz[int(f["label"]), int(f["prediction"])] = f["count"]
    por_clase, f1s = {}, []
    for i, clase in enumerate(clases):
        tp = matriz[i, i]
        precision = tp / max(matriz[:, i].sum(), 1)
        recall = tp / max(matriz[i, :].sum(), 1)
        f1 = 2 * precision * recall / max(precision + recall, 1e-9)
        por_clase[clase] = {"precision": round(float(precision), 4),
                            "recall": round(float(recall), 4), "f1": round(float(f1), 4),
                            "soporte": int(matriz[i, :].sum())}
        f1s.append(f1)
    return {"accuracy": round(float(np.trace(matriz) / matriz.sum()), 4),
            "f1_macro": round(float(np.mean(f1s)), 4), "por_clase": por_clase,
            "matriz": matriz.tolist(), "clases": clases}


def exportar(modelo, ruta: str) -> None:
    """Guarda idf, coeficientes e interceptos; la API aplica softmax sobre ellos."""
    idf = modelo.stages[4].idf.toArray()
    clasificador = modelo.stages[5]
    if hasattr(clasificador, "coefficientMatrix"):
        coef, inter = clasificador.coefficientMatrix.toArray(), clasificador.interceptVector.toArray()
    else:
        coef, inter = clasificador.theta.toArray(), clasificador.pi.toArray()
    np.savez_compressed(ruta, idf=idf, coef=coef, intercept=inter,
                        clases=np.array(modelo.stages[0].labels), n_features=N_FEATURES)


def main() -> None:
    spark = crear_spark("lifemap-sentimiento")
    df = (spark.read.parquet(f"{config.CURATED_DIR}/reviews")
          .dropDuplicates(["review_id"]).select("business_id", "text", "sentimiento")
          .withColumn("text", F.lower("text"))
          .withColumn("fold", F.abs(F.hash("business_id")) % 10))
    train = df.filter("fold < 8").sample(fraction=MUESTRA_ENTRENAMIENTO, seed=42)
    test = df.filter("fold >= 8")

    distribucion = {f["sentimiento"]: f["count"] for f in df.groupBy("sentimiento").count().collect()}
    resultados: dict = {"distribucion": distribucion, "n_train": train.count(), "n_test": test.count()}
    cruce = (train.select("business_id").distinct()
             .join(test.select("business_id").distinct(), "business_id").count())
    resultados["negocios_en_ambos_lados"] = cruce

    candidatos = {
        "naive_bayes": NaiveBayes(featuresCol="tfidf", modelType="multinomial", smoothing=1.0),
        "regresion_logistica": LogisticRegression(featuresCol="tfidf", maxIter=30, regParam=0.01,
                                                  elasticNetParam=0.0, family="multinomial"),
    }
    mejor, mejor_f1 = None, -1.0
    for nombre, clasificador in candidatos.items():
        modelo = etapas(clasificador).fit(train)
        clases = list(modelo.stages[0].labels)
        pred = modelo.transform(test).select("label", "prediction")
        metricas = evaluar(pred, clases)
        resultados[nombre] = metricas
        print(nombre, json.dumps(metricas), flush=True)
        if metricas["f1_macro"] > mejor_f1:
            mejor, mejor_f1 = (nombre, modelo), metricas["f1_macro"]

    os.makedirs(config.MODEL_DIR, exist_ok=True)
    mejor[1].write().overwrite().save(f"{config.MODEL_DIR}/sentiment_v1")
    exportar(mejor[1], f"{config.MODEL_DIR}/sentiment_v1.npz")
    resultados["modelo_guardado"] = mejor[0]
    with open(f"{config.DATA_DIR}/sentiment_metrics.json", "w", encoding="utf-8") as fh:
        json.dump(resultados, fh, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
