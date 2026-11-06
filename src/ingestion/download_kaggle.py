"""Descarga el Yelp Open Dataset con la API de Kaggle, en streaming."""
from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

import requests

from src.common import config

URL = "https://www.kaggle.com/api/v1/datasets/download/" + config.KAGGLE_DATASET


def credenciales() -> tuple[str, str]:
    user, key = os.environ.get("KAGGLE_USERNAME"), os.environ.get("KAGGLE_KEY")
    if user and key:
        return user, key
    ruta = Path(os.environ.get("KAGGLE_JSON", "/run/secrets/kaggle.json"))
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    return datos["username"], datos["key"]


def descargar(destino: Path) -> None:
    """Escribe por trozos: la libreria de Kaggle acumula el zip de 4 GB en memoria."""
    archivos = [destino / config.BUSINESS_FILE, destino / config.REVIEW_FILE]
    if all(a.is_file() for a in archivos):
        print("El dataset ya esta en el volumen; no se descarga", flush=True)
        return
    zip_path = destino / "yelp.zip"
    with requests.get(URL, auth=credenciales(), stream=True, timeout=(30, 600)) as resp:
        resp.raise_for_status()
        with zip_path.open("wb") as fh:
            for trozo in resp.iter_content(1 << 20):
                fh.write(trozo)
    with zipfile.ZipFile(zip_path) as zf:
        for archivo in archivos:
            zf.extract(archivo.name, destino)
    zip_path.unlink()


if __name__ == "__main__":
    descargar(Path(config.DATA_DIR))
