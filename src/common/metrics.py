"""Tasa de cierre anualizada: la AFR del ejercicio de discos con dias-negocio."""
from __future__ import annotations


def tasa_cierre(cierres: int, dias_negocio: float) -> float | None:
    """cierres / (dias_negocio / 365) * 100; None si no hay exposicion."""
    if dias_negocio <= 0:
        return None
    return cierres / (dias_negocio / 365) * 100


def etiqueta_sentimiento(estrellas: float) -> str:
    """1-2 negativo, 3 neutral, 4-5 positivo."""
    if estrellas <= 2:
        return "negativo"
    return "neutral" if estrellas == 3 else "positivo"
