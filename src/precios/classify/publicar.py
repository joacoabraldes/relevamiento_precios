"""Publicacion del mapeo producto -> categoria al bucket.

El repo de reporte necesita saber a que categoria elemental pertenece cada
id_producto: Jevons compara leche con leche y arroz con arroz, asi que sin ese
mapeo no hay indice elemental y no hay nada que agregar.

Esa informacion vive en dos archivos versionados de ESTE repo:

    config/mapeo_productos.csv   id_producto -> categoria      (revisado a mano)
    config/categorias.yaml       categoria   -> clase COICOP   (la taxonomia)

y hasta ahora no salia de aca. El otro repo quedaba sin forma de agrupar, o
tenia que copiarse los archivos y desincronizarse en silencio. La interfaz entre
los dos repos es el bucket, no el codigo: esto la completa.

Se publica **desnormalizado** —producto, categoria y clase COICOP en la misma
fila— para que el consumidor no tenga que replicar la taxonomia ni saber que la
jerarquia es division -> grupo -> clase -> categoria. Lee una tabla y joinea.

`generado_en` viaja en cada fila a proposito: permite detectar del lado del
consumidor que se esta calculando el indice con una clasificacion vieja, sin
tener que consultar el bucket por metadata aparte.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import duckdb

from .taxonomia import Taxonomia

# El orden es el de las columnas del Parquet publicado.
COLUMNAS = (
    "id_producto",
    "categoria",
    "categoria_nombre",
    "clase",
    "clase_nombre",
    "grupo",
    "grupo_nombre",
    "division",
    "division_nombre",
    "origen",
    "revisado",
    "generado_en",
)

DDL = """
CREATE OR REPLACE TABLE clasificacion (
    id_producto      VARCHAR,
    categoria        VARCHAR,
    categoria_nombre VARCHAR,
    clase            VARCHAR,
    clase_nombre     VARCHAR,
    grupo            VARCHAR,
    grupo_nombre     VARCHAR,
    division         VARCHAR,
    division_nombre  VARCHAR,
    origen           VARCHAR,
    revisado         BOOLEAN,
    generado_en      TIMESTAMP
)
"""

NOMBRE_ARCHIVO = "clasificacion.parquet"


def _grupo_de_clase(clase: str) -> str:
    """`01.1.5` -> `01.1`."""
    return clase.rsplit(".", 1)[0] if "." in clase else clase


def _division_de_clase(clase: str) -> str:
    """`01.1.5` -> `01`."""
    return clase.split(".", 1)[0]


def filas_clasificacion(
    mapeo_path: Path,
    taxonomia: Taxonomia,
    generado_en: dt.datetime | None = None,
) -> list[tuple]:
    """Une el mapeo revisado con la taxonomia y devuelve las filas a publicar.

    Falla si una categoria del CSV no existe en la taxonomia. Es a proposito: un
    producto que se cae del join desaparece del indice sin ruido, y el resultado
    sigue siendo un numero plausible. Mejor romper aca.
    """
    generado_en = generado_en or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    por_categoria = {r.codigo: r for r in taxonomia.reglas}

    filas: list[tuple] = []
    vistos: set[str] = set()
    with mapeo_path.open(encoding="utf-8", newline="") as fh:
        for n, fila in enumerate(csv.DictReader(fh), start=2):
            id_producto = (fila.get("id_producto") or "").strip()
            categoria = (fila.get("categoria") or "").strip()
            if not id_producto:
                raise ValueError(f"{mapeo_path}:{n}: fila sin id_producto")

            regla = por_categoria.get(categoria)
            if regla is None:
                raise ValueError(
                    f"{mapeo_path}:{n}: categoria {categoria!r} no existe en la "
                    f"taxonomia (producto {id_producto})"
                )

            # Un id_producto en dos categorias haria que el mismo precio entre
            # dos veces al indice, con pesos distintos. No puede pasar.
            if id_producto in vistos:
                raise ValueError(
                    f"{mapeo_path}:{n}: id_producto {id_producto} repetido"
                )
            vistos.add(id_producto)

            clase = regla.clase
            grupo = _grupo_de_clase(clase)
            division = _division_de_clase(clase)
            filas.append(
                (
                    id_producto,
                    categoria,
                    regla.nombre,
                    clase,
                    taxonomia.clases.get(clase, ""),
                    grupo,
                    taxonomia.grupos.get(grupo, ""),
                    division,
                    taxonomia.divisiones.get(division, ""),
                    (fila.get("origen") or "").strip(),
                    (fila.get("revisado") or "").strip().lower() in ("si", "sí", "true", "1"),
                    generado_en,
                )
            )

    if not filas:
        raise ValueError(f"{mapeo_path} no tiene ninguna fila")
    return filas


def escribir_parquet(filas: list[tuple], destino: Path) -> Path:
    """Escribe las filas como Parquet. Devuelve el path escrito."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(DDL)
        con.executemany(
            f"INSERT INTO clasificacion VALUES ({', '.join('?' * len(COLUMNAS))})",
            filas,
        )
        con.execute(
            f"COPY clasificacion TO '{destino.as_posix()}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return destino


def resumen(filas: list[tuple]) -> dict[str, int]:
    """Conteos para el log: productos, categorias, clases y cuantos revisados."""
    i_cat = COLUMNAS.index("categoria")
    i_clase = COLUMNAS.index("clase")
    i_rev = COLUMNAS.index("revisado")
    return {
        "productos": len(filas),
        "categorias": len({f[i_cat] for f in filas}),
        "clases": len({f[i_clase] for f in filas}),
        "revisados": sum(1 for f in filas if f[i_rev]),
    }
