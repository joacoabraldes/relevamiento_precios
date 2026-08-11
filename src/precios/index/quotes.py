"""Colapso de observaciones diarias a quotes mensuales.

Un **quote** es la unidad de medicion del indice: un producto, en una sucursal
concreta, de un comercio concreto. Su precio del mes es la **mediana** de sus
precios diarios — mediana y no promedio porque un solo precio mal cargado
desplaza el promedio y no mueve la mediana.

Esta tabla es el insumo directo del indice y **se conserva para siempre**,
incluso cuando el detalle diario vence por TTL: con los quotes se puede
recalcular la serie completa con una clasificacion corregida.

Dos decisiones que hacen falta para que esa promesa se sostenga:

1. **Se guardan TODOS los comercios**, incluidos los que hoy se excluyen del
   analisis (farmacias, estaciones de servicio). Filtrar aca seria irreversible
   una vez que venza el crudo; filtrar al calcular el indice no lo es.

2. **Se guardan TODOS los quotes**, incluidos los que no llegan al minimo de
   dias observados, junto con `n_dias`. Asi el umbral se puede cambiar mas
   adelante sin reprocesar nada. El filtro se aplica al calcular el indice.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from datetime import date
from pathlib import Path

import duckdb

from ..logs import log

# `sepa_1_comercio-sepa-15_2026-07-30_09-05-11.parquet` -> "15"
RE_COMERCIO = re.compile(r"comercio-sepa-(\d+)", re.IGNORECASE)

# Minimo de dias observados para que un quote entre al indice. No filtra la
# tabla: se guarda como bandera para poder recalcular con otro umbral.
MINIMO_DIAS_DEFAULT = 5


@dataclasses.dataclass
class ResumenQuotes:
    anio: int
    mes: int
    dias_disponibles: int
    observaciones: int = 0
    quotes: int = 0
    quotes_con_minimo: int = 0
    productos: int = 0
    comercios: int = 0

    @property
    def cobertura_minimo(self) -> float:
        return 100.0 * self.quotes_con_minimo / self.quotes if self.quotes else 0.0


def comercio_de_archivo(nombre: str) -> str | None:
    m = RE_COMERCIO.search(Path(nombre).name)
    return m.group(1) if m else None


SQL_QUOTES = """
COPY (
    SELECT
        {anio}                              AS anio,
        {mes}                               AS mes,
        id_comercio, id_bandera, id_sucursal, id_producto,
        any_value(es_ean)                   AS es_ean,
        median(precio_lista)                AS precio_lista_mediana,
        median(precio_efectivo)             AS precio_efectivo_mediana,
        count(*)                            AS n_dias,
        count(*) >= {minimo_dias}           AS cumple_minimo_dias,
        min(precio_lista)                   AS precio_lista_min,
        max(precio_lista)                   AS precio_lista_max,
        min(fecha)                          AS primera_fecha,
        max(fecha)                          AS ultima_fecha,
        any_value(provincia)                AS provincia
    FROM read_parquet([{archivos}])
    GROUP BY id_comercio, id_bandera, id_sucursal, id_producto
) TO '{destino}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""

# El catalogo permite volver a clasificar sin el detalle diario: los quotes solo
# tienen claves y precios, no descripciones. Sin esto quedarian precios que no
# se pueden reclasificar porque no se sabe que producto son.
SQL_CATALOGO = """
COPY (
    SELECT
        {anio} AS anio, {mes} AS mes,
        id_producto,
        any_value(es_ean)                   AS es_ean,
        arg_max(descripcion, n)             AS descripcion,
        arg_max(marca, n)                   AS marca,
        arg_max(cantidad_base, n)           AS cantidad_base,
        arg_max(unidad_base, n)             AS unidad_base,
        arg_max(unidad_presentacion_raw, n) AS unidad_presentacion_raw,
        sum(n)                              AS n_observaciones,
        count(DISTINCT id_comercio)         AS n_comercios,
        list(DISTINCT descripcion)[1:5]     AS descripciones_alternativas
    FROM (
        SELECT id_producto, es_ean, descripcion, marca, cantidad_base,
               unidad_base, unidad_presentacion_raw, id_comercio, count(*) AS n
        FROM read_parquet([{archivos}])
        WHERE id_producto IS NOT NULL
        GROUP BY ALL
    )
    GROUP BY id_producto
) TO '{destino}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""


def conectar(memoria: str, hilos: int, tmpdir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memoria}'")
    con.execute(f"SET threads={hilos}")
    # El colapso mensual agrupa cientos de millones de filas: sin directorio
    # temporal DuckDB no puede derramar a disco y muere por memoria.
    con.execute(f"SET temp_directory='{tmpdir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _lista_sql(archivos: list[Path]) -> str:
    return ", ".join(f"'{p.as_posix()}'" for p in archivos)


def colapsar_comercio(
    con: duckdb.DuckDBPyConnection,
    archivos: list[Path],
    anio: int,
    mes: int,
    destino_quotes: Path,
    destino_catalogo: Path,
    minimo_dias: int = MINIMO_DIAS_DEFAULT,
) -> tuple[int, int]:
    """Colapsa los dias de un comercio a quotes. Devuelve (quotes, con_minimo)."""
    lista = _lista_sql(archivos)
    con.execute(
        SQL_QUOTES.format(
            anio=anio, mes=mes, minimo_dias=minimo_dias,
            archivos=lista, destino=destino_quotes.as_posix(),
        )
    )
    con.execute(
        SQL_CATALOGO.format(
            anio=anio, mes=mes, archivos=lista,
            destino=destino_catalogo.as_posix(),
        )
    )
    n = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE cumple_minimo_dias) "
        f"FROM read_parquet('{destino_quotes.as_posix()}')"
    ).fetchone()
    return int(n[0]), int(n[1])


def dias_del_mes(fechas: list[date], anio: int, mes: int) -> list[date]:
    return sorted(f for f in fechas if f.year == anio and f.month == mes)
