import os

import pytest
import requests

BASE = os.environ.get("API_BASE_URL", "http://api:5000")
LAT, LON = 39.9526, -75.1652   # centro de Filadelfia

pytestmark = pytest.mark.integration


def test_health_trae_servicios_y_commit():
    datos = requests.get(f"{BASE}/health", timeout=10).json()
    assert {"api", "mongodb", "spark", "dask"} <= set(datos["services"])
    assert datos["commit"]


def test_near_filtra_por_categoria():
    datos = requests.get(f"{BASE}/businesses/near", timeout=30, params={
        "lat": LAT, "lon": LON, "radius_m": 2000, "category": "Pizza"}).json()
    assert datos["count"] > 0


def test_within_con_poligono():
    poligono = {"type": "Polygon", "coordinates": [[
        [LON - .02, LAT - .02], [LON + .02, LAT - .02], [LON + .02, LAT + .02],
        [LON - .02, LAT + .02], [LON - .02, LAT - .02]]]}
    assert requests.post(f"{BASE}/businesses/within", json=poligono, timeout=30).json()["count"] > 0


def test_zones_summary():
    datos = requests.get(f"{BASE}/zones/summary", timeout=30,
                         params={"lat": LAT, "lon": LON, "radius_m": 2000}).json()
    assert datos["negocios"] > 0 and "tasa_cierre_pct" in datos


def test_stats_closure():
    assert requests.get(f"{BASE}/stats/closure", timeout=30).json()["count"] > 0


def test_predict():
    r = requests.post(f"{BASE}/predict", json={"text": "terrible food, awful and rude service"},
                      timeout=30)
    assert r.json()["sentimiento"] == "negativo"
