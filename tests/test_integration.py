import math
import os

import pytest
import requests

BASE = os.environ.get("API_BASE_URL", "http://api:5000")
LAT, LON = 39.9526, -75.1652   # centro de Filadelfia

pytestmark = pytest.mark.integration


def distancia_m(lon: float, lat: float) -> float:
    a, b = math.radians(LAT), math.radians(lat)
    d = math.radians(lon - LON)
    h = math.sin((b - a) / 2) ** 2 + math.cos(a) * math.cos(b) * math.sin(d / 2) ** 2
    return 2 * 6_371_000 * math.asin(math.sqrt(h))


def test_health_trae_servicios_y_commit():
    datos = requests.get(f"{BASE}/health", timeout=10).json()
    assert {"api", "mongodb", "spark", "dask"} <= set(datos["services"])
    assert datos["status"] == "ok"
    assert len(datos["commit"]) == 40


def test_near_filtra_por_categoria():
    datos = requests.get(f"{BASE}/businesses/near", timeout=30, params={
        "lat": LAT, "lon": LON, "radius_m": 2000, "category": "Pizza"}).json()
    assert datos["count"] > 0
    assert all("pizza" in r["categories"].lower() for r in datos["results"])


def test_near_respeta_radio_y_orden():
    datos = requests.get(f"{BASE}/businesses/near", timeout=30, params={
        "lat": LAT, "lon": LON, "radius_m": 1000, "limit": 200}).json()
    distancias = [distancia_m(*r["location"]["coordinates"]) for r in datos["results"]]
    assert distancias and max(distancias) <= 1010
    assert distancias == sorted(distancias)


def test_categoria_con_simbolos_no_rompe():
    r = requests.get(f"{BASE}/businesses/near", timeout=30, params={
        "lat": LAT, "lon": LON, "category": "categoria-inexistente("})
    assert r.status_code == 200 and r.json()["count"] == 0


def test_poligono_que_se_cruza_es_400():
    r = requests.post(f"{BASE}/businesses/within", timeout=30, json={
        "type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 1], [1, 1], [0, 0]]]})
    assert r.status_code == 400


def test_within_con_poligono():
    poligono = {"type": "Polygon", "coordinates": [[
        [LON - .02, LAT - .02], [LON + .02, LAT - .02], [LON + .02, LAT + .02],
        [LON - .02, LAT + .02], [LON - .02, LAT - .02]]]}
    datos = requests.post(f"{BASE}/businesses/within", json=poligono, timeout=30).json()
    assert datos["count"] > 0
    for r in datos["results"]:
        lon, lat = r["location"]["coordinates"]
        assert abs(lon - LON) <= .02 and abs(lat - LAT) <= .02


def test_zones_summary():
    datos = requests.get(f"{BASE}/zones/summary", timeout=30,
                         params={"lat": LAT, "lon": LON, "radius_m": 2000}).json()
    assert datos["negocios"] > 0 and "tasa_cierre_pct" in datos


def test_stats_closure():
    datos = requests.get(f"{BASE}/stats/closure", timeout=30).json()
    assert datos["count"] > 0
    for r in datos["results"]:
        assert r["dias_negocio"] >= datos["min_dias"]
        esperado = r["cierres"] / (r["dias_negocio"] / 365) * 100
        assert abs(r["tasa_cierre_pct"] - esperado) < 1e-6


def test_predict():
    r = requests.post(f"{BASE}/predict", json={"text": "terrible food, awful and rude service"},
                      timeout=30)
    assert r.json()["sentimiento"] == "negativo"
    r = requests.post(f"{BASE}/predict", json={"text": "delicious food, amazing friendly staff"},
                      timeout=30)
    assert r.json()["sentimiento"] == "positivo"
