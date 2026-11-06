import json

import mongomock
import numpy as np
import pandas as pd
import pytest

from src.api import app as api
from src.api import sentiment_model
from src.common.metrics import etiqueta_sentimiento, tasa_cierre
from src.ingestion.cleaning import categoria_principal, leer_rango, limpiar_negocios, limpiar_resenas, vida_negocios


def test_tasa_cierre_formula():
    assert tasa_cierre(20, 365_000) == pytest.approx(3.0)
    assert tasa_cierre(1, 0) is None


@pytest.mark.parametrize("estrellas,esperado", [(1, "negativo"), (2, "negativo"), (3, "neutral"),
                                                (4, "positivo"), (5, "positivo")])
def test_etiqueta(estrellas, esperado):
    assert etiqueta_sentimiento(estrellas) == esperado


def test_categoria_principal_ignora_genericas():
    familia = ["Restaurants", "Food"]
    assert categoria_principal("Restaurants, Pizza", familia) == "Pizza"
    assert categoria_principal("Food", familia) == "Food"
    assert categoria_principal("Hair Salons", familia) is None


def test_limpiar_negocios_descarta_coordenadas_y_duplicados():
    df = pd.DataFrame({"business_id": ["a", "a", "b", "c"], "state": ["PA", "PA", "PA", "TX"],
                       "categories": ["Pizza, Food"] * 4, "latitude": [40.0, 40.0, 95.0, 40.0],
                       "longitude": [-75.0, -75.0, -75.0, -75.0], "is_open": [1, 1, 0, 1]})
    limpio, conteos = limpiar_negocios(df)
    assert list(limpio.business_id) == ["a"]
    assert conteos == {"leidos": 4, "en_el_area": 3, "en_la_familia": 3,
                       "coordenadas_validas": 2, "sin_duplicados": 1}


def test_limpiar_resenas_quita_sin_texto():
    df = pd.DataFrame({"review_id": ["1", "2", "3"], "business_id": ["a", "a", "z"],
                       "stars": [5.0, 1.0, 3.0], "text": ["bueno", "  ", "ok"],
                       "date": ["2020-01-01"] * 3})
    limpio, conteos = limpiar_resenas(df, {"a"})
    assert list(limpio.review_id) == ["1"]
    assert conteos["con_texto"] == 1


def test_leer_rango_cuenta_cada_linea_una_vez(tmp_path):
    ruta = tmp_path / "r.json"
    lineas = [json.dumps({"business_id": "a" if i % 2 else "b", "text": "x" * (i % 7)})
              for i in range(50)]
    ruta.write_text("".join(linea + chr(10) for linea in lineas))
    tam = ruta.stat().st_size
    for paso in (1, 13, 64, tam):
        partes = [leer_rango(str(ruta), i, min(i + paso, tam), ["business_id"], {"a"})
                  for i in range(0, tam, paso)]
        assert sum(leidas for _, leidas in partes) == 50
        assert sum(len(df) for df, _ in partes) == 25


def test_vida_y_censura():
    corte = pd.Timestamp("2022-01-01")
    df = pd.DataFrame({"first_review": pd.to_datetime(["2020-01-01", "2021-06-01"]),
                       "last_review": pd.to_datetime(["2021-01-01", "2021-06-01"]),
                       "is_open": [0, 1]})
    v = vida_negocios(df, corte)
    assert list(v.dias_negocio) == [366, 1]
    assert list(v.evento) == [1, 0]
    assert list(v.censurado) == [False, True]


@pytest.fixture
def cliente(monkeypatch):
    base = mongomock.MongoClient().lifemap
    base.closure_by_category.insert_many([
        {"_id": "Pizza", "clave": "Pizza", "cierres": 5, "dias_negocio": 50_000, "tasa_cierre_pct": 3.65},
        {"_id": "Sushi", "clave": "Sushi", "cierres": 1, "dias_negocio": 100, "tasa_cierre_pct": 365.0}])
    monkeypatch.setattr(api, "db", lambda: base)
    return api.app.test_client()


def test_closure_respeta_minimo(cliente):
    datos = cliente.get("/stats/closure?min_dias=1000").get_json()
    assert [r["clave"] for r in datos["results"]] == ["Pizza"]
    assert cliente.get("/stats/closure?min_dias=0").get_json()["count"] == 2


@pytest.mark.parametrize("url", ["/businesses/near?lat=999&lon=0", "/businesses/near?lon=0",
                                 "/stats/closure?by=x", "/zones/summary?lat=a&lon=0",
                                 "/businesses/within?polygon=nada"])
def test_entradas_invalidas(cliente, url):
    assert cliente.get(url).status_code == 400


def test_predict_exige_texto(cliente):
    assert cliente.post("/predict", json={}).status_code == 400


def test_predecir_con_modelo_pequeno():
    n = 32
    positivo = next(iter(sentiment_model.indices("delicious", n)))
    coef = np.zeros((2, n))
    coef[0, positivo] = 5
    modelo = {"n_features": np.array(n), "idf": np.ones(n), "coef": coef,
              "intercept": np.zeros(2), "clases": np.array(["positivo", "negativo"])}
    salida = sentiment_model.predecir("delicious", modelo)
    assert salida["sentimiento"] == "positivo"
    assert json.dumps(salida)
