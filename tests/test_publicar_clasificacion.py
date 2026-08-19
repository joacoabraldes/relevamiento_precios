"""Publicacion del mapeo producto -> categoria al bucket.

Esta tabla es la unica forma que tiene el repo de reporte de saber que producto
es cada id_producto. Si se cae una fila, el producto desaparece del indice sin
que nada falle: el numero sigue siendo plausible. Por eso los tests se centran
en que las fallas sean ruidosas.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from precios.classify import publicar as pub
from precios.classify.taxonomia import Taxonomia

RAIZ = Path(__file__).resolve().parents[1]

CABECERA = "id_producto,categoria,descripcion,marca,cantidad_base,unidad_base,n_observaciones,origen,revisado"


@pytest.fixture
def tax() -> Taxonomia:
    return Taxonomia.desde_yaml(RAIZ / "config" / "categorias.yaml")


def _csv(tmp_path: Path, filas: list[str]) -> Path:
    p = tmp_path / "mapeo.csv"
    p.write_text("\n".join([CABECERA, *filas]), encoding="utf-8")
    return p


def test_desnormaliza_la_jerarquia_completa(tmp_path, tax):
    """El consumidor lee una fila y tiene producto, categoria y clase COICOP."""
    path = _csv(tmp_path, ["7790001,almacen.arroz_largo_fino_1kg,ARROZ,M,1,kg,10,auto,no"])
    filas = pub.filas_clasificacion(path, tax)

    assert len(filas) == 1
    f = dict(zip(pub.COLUMNAS, filas[0]))
    assert f["id_producto"] == "7790001"
    assert f["categoria"] == "almacen.arroz_largo_fino_1kg"
    assert f["clase"] == "01.1.1"
    assert f["clase_nombre"] == "Pan y cereales"
    assert f["grupo"] == "01.1"
    assert f["division"] == "01"
    assert f["division_nombre"] == "Alimentos y bebidas no alcoholicas"


def test_una_categoria_inexistente_rompe_en_vez_de_perder_el_producto(tmp_path, tax):
    """El modo de falla peligroso es que la fila se caiga del join en silencio."""
    path = _csv(tmp_path, ["7790001,almacen.no_existe,ALGO,M,1,kg,10,auto,no"])
    with pytest.raises(ValueError, match="no existe en la taxonomia"):
        pub.filas_clasificacion(path, tax)


def test_un_producto_repetido_rompe(tmp_path, tax):
    """Entraria dos veces al indice, en dos categorias, con pesos distintos."""
    path = _csv(tmp_path, [
        "7790001,almacen.arroz_largo_fino_1kg,ARROZ,M,1,kg,10,auto,no",
        "7790001,almacen.sal_fina_500g,SAL,M,0.5,kg,10,auto,no",
    ])
    with pytest.raises(ValueError, match="repetido"):
        pub.filas_clasificacion(path, tax)


def test_revisado_se_publica_como_booleano(tmp_path, tax):
    path = _csv(tmp_path, [
        "1,almacen.sal_fina_500g,SAL,M,0.5,kg,10,auto,si",
        "2,almacen.sal_fina_500g,SAL,M,0.5,kg,10,auto,no",
    ])
    filas = pub.filas_clasificacion(path, tax)
    i = pub.COLUMNAS.index("revisado")
    assert [f[i] for f in filas] == [True, False]


def test_el_parquet_publicado_se_puede_leer_y_joinear(tmp_path, tax):
    """Round-trip: es exactamente lo que va a hacer el repo de reporte."""
    path = _csv(tmp_path, [
        "7790001,almacen.arroz_largo_fino_1kg,ARROZ,M,1,kg,10,auto,no",
        "7790002,lacteos.leche_entera_1l,LECHE,M,1,l,10,auto,no",
    ])
    filas = pub.filas_clasificacion(path, tax, generado_en=dt.datetime(2026, 8, 19))
    destino = pub.escribir_parquet(filas, tmp_path / "clasificacion.parquet")

    con = duckdb.connect()
    leido = con.execute(
        f"SELECT id_producto, clase FROM read_parquet('{destino.as_posix()}') "
        f"ORDER BY id_producto"
    ).fetchall()
    con.close()
    assert leido == [("7790001", "01.1.1"), ("7790002", "01.1.4")]


def test_los_comercios_excluidos_se_publican_con_el_motivo(tmp_path):
    """Dentro de un año nadie se acuerda de por que el comercio 23 no esta."""
    from precios.comercios import ListaComercios

    lista = ListaComercios.desde_yaml(RAIZ / "config" / "comercios.yaml")
    filas = pub.filas_comercios_excluidos(lista)

    assert {f[0] for f in filas} == set(lista.ids)
    assert all(f[1] for f in filas), "todos tienen nombre"
    assert all(f[2] for f in filas), "todos tienen motivo"

    destino = pub.escribir_comercios_parquet(filas, tmp_path / "com.parquet")
    con = duckdb.connect()
    leido = con.execute(
        f"SELECT id_comercio FROM read_parquet('{destino.as_posix()}') ORDER BY 1"
    ).fetchall()
    con.close()
    assert [r[0] for r in leido] == sorted(lista.ids)


def test_el_mapeo_real_del_repo_es_publicable(tax):
    """El CSV versionado tiene que pasar todas las validaciones de arriba."""
    filas = pub.filas_clasificacion(RAIZ / "config" / "mapeo_productos.csv", tax)
    res = pub.resumen(filas)
    assert res["productos"] > 900
    assert res["categorias"] == len(tax.reglas)
    # Las 6 clases COICOP del piloto.
    assert res["clases"] == 6
