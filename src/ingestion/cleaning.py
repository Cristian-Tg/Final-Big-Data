"""Reglas de limpieza. Funciones puras de pandas para poder probarlas con pytest."""
from __future__ import annotations

import json

import pandas as pd

from src.common import config
from src.common.geo import geohash_encode, is_valid_point

GENERICAS = {"Restaurants", "Food"}


def categoria_principal(categorias: str | None, familia: list[str]) -> str | None:
    """Primera categoria especifica del negocio dentro de la familia del equipo."""
    lista = [c.strip() for c in (categorias or "").split(",") if c.strip()]
    if not set(lista) & set(familia):
        return None
    especificas = [c for c in lista if c not in GENERICAS]
    return especificas[0] if especificas else lista[0]


def limpiar_negocios(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Devuelve los negocios validos del area y el conteo despues de cada regla."""
    conteos = {"leidos": len(df)}
    df = df[df["state"].isin(config.METRO_STATES)]
    conteos["en_el_area"] = len(df)
    df = df.assign(categoria_principal=[
        categoria_principal(c, config.CATEGORY_FAMILY) for c in df["categories"]])
    df = df[df["categoria_principal"].notna()]
    conteos["en_la_familia"] = len(df)
    validos = [is_valid_point(la, lo) for la, lo in zip(df["latitude"], df["longitude"])]
    df = df[validos] if len(df) else df
    conteos["coordenadas_validas"] = len(df)
    df = df[df["is_open"].isin([0, 1])].drop_duplicates("business_id")
    conteos["sin_duplicados"] = len(df)
    df = df.assign(geohash=[geohash_encode(la, lo, config.GEOHASH_PRECISION)
                            for la, lo in zip(df["latitude"], df["longitude"])])
    return df, conteos


def leer_rango(ruta: str, inicio: int, fin: int, columnas: list[str],
               negocios: set[str]) -> tuple[pd.DataFrame, int]:
    """Lee las lineas que empiezan en [inicio, fin) y guarda solo las de los negocios dados.

    Filtrar mientras se lee evita tener en memoria el texto de las demas resenas.
    """
    filas, leidas = [], 0
    with open(ruta, "rb") as fh:
        posicion = inicio
        if inicio:
            fh.seek(inicio - 1)
            posicion = inicio - 1 + len(fh.readline())
        while posicion < fin:
            linea = fh.readline()
            if not linea:
                break
            posicion += len(linea)
            leidas += 1
            registro = json.loads(linea)
            if registro.get("business_id") in negocios:
                filas.append({c: registro.get(c) for c in columnas})
    return pd.DataFrame(filas, columns=columnas), leidas


def limpiar_resenas(df: pd.DataFrame, negocios: set[str]) -> tuple[pd.DataFrame, dict[str, int]]:
    """Filtra una particion de resenas; los duplicados entre particiones se quitan despues."""
    conteos = {"leidas": len(df)}
    df = df[df["business_id"].isin(negocios)]
    conteos["de_negocios_validos"] = len(df)
    df = df[df["text"].fillna("").str.strip().str.len() > 0]
    conteos["con_texto"] = len(df)
    fechas = pd.to_datetime(df["date"], errors="coerce")
    df = df.assign(date=fechas)
    df = df[df["date"].notna() & df["stars"].between(1, 5)]
    conteos["con_fecha_y_estrellas"] = len(df)
    return df, conteos


def vida_negocios(resumen: pd.DataFrame, corte: pd.Timestamp) -> pd.DataFrame:
    """Vida por negocio. Espera columnas first_review, last_review e is_open."""
    r = resumen.copy()
    # Un negocio con una sola resena tiene vida 0: cuenta 1 dia de exposicion
    r["dias_negocio"] = ((r["last_review"] - r["first_review"]).dt.days).clip(lower=1)
    r["evento"] = (r["is_open"] == 0).astype(int)
    r["censurado"] = r["is_open"] == 1
    r["dias_hasta_corte"] = (corte - r["first_review"]).dt.days.clip(lower=1)
    return r
