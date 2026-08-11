"""Colapso a quotes mensuales.

Esta tabla se conserva para siempre y es lo que permite recalcular el indice
cuando el detalle diario venza por TTL. Los tests defienden esa promesa: que no
se pierda informacion que despues no se pueda reconstruir.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from precios.index import quotes as q


def _observaciones(tmp_path: Path, filas: list[dict]) -> Path:
    """Escribe un Parquet con la forma de staged/observaciones."""
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE o (
            fecha DATE, id_comercio VARCHAR, id_bandera VARCHAR, id_sucursal VARCHAR,
            provincia VARCHAR, provincia_iso VARCHAR, id_producto VARCHAR,
            es_ean BOOLEAN, descripcion VARCHAR, marca VARCHAR,
            cantidad_presentacion DOUBLE, unidad_presentacion VARCHAR,
            unidad_presentacion_raw VARCHAR, cantidad_base DOUBLE, unidad_base VARCHAR,
            precio_lista DOUBLE, precio_promo DOUBLE, precio_efectivo DOUBLE,
            precio_referencia DOUBLE, cantidad_referencia DOUBLE, unidad_referencia VARCHAR)"""
    )
    for f in filas:
        con.execute(
            "INSERT INTO o VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                f.get("fecha", date(2026, 8, 1)), f.get("id_comercio", "10"),
                f.get("id_bandera", "1"), f.get("id_sucursal", "7"),
                f.get("provincia", "CABA"), "AR-C", f.get("id_producto", "EAN1"),
                f.get("es_ean", True), f.get("descripcion", "LECHE ENTERA"),
                f.get("marca", "MARCA"), 1.0, "l", "L", 1.0, "l",
                f.get("precio_lista", 1000.0), f.get("precio_promo"),
                f.get("precio_efectivo", f.get("precio_lista", 1000.0)),
                1000.0, 1.0, "L",
            ],
        )
    destino = tmp_path / "obs.parquet"
    con.execute(f"COPY o TO '{destino.as_posix()}' (FORMAT PARQUET)")
    con.close()
    return destino


def _colapsar(tmp_path, filas, minimo_dias=5):
    obs = _observaciones(tmp_path, filas)
    con = q.conectar("2GB", 2, tmp_path)
    dq, dc = tmp_path / "q.parquet", tmp_path / "c.parquet"
    n, n_min = q.colapsar_comercio(con, [obs], 2026, 8, dq, dc, minimo_dias)
    quotes = con.execute(f"SELECT * FROM read_parquet('{dq.as_posix()}')").fetchdf()
    catalogo = con.execute(f"SELECT * FROM read_parquet('{dc.as_posix()}')").fetchdf()
    con.close()
    return quotes, catalogo, n, n_min


# --------------------------------------------------------------------------- #
# La mediana
# --------------------------------------------------------------------------- #


def test_usa_la_mediana_no_el_promedio(tmp_path):
    """Un precio mal cargado desplaza el promedio y no mueve la mediana.

    Precios: 1000, 1000, 1000, 1000, 99999 (el ultimo es un error de carga).
      mediana = 1000  (correcto)
      promedio = 20800 (arruinado)
    """
    filas = [
        {"fecha": date(2026, 8, d), "precio_lista": p}
        for d, p in zip(range(1, 6), [1000, 1000, 1000, 1000, 99999])
    ]
    quotes, _, _, _ = _colapsar(tmp_path, filas)
    assert len(quotes) == 1
    assert quotes.iloc[0]["precio_lista_mediana"] == pytest.approx(1000.0)


def test_mediana_con_cantidad_par_de_dias(tmp_path):
    filas = [
        {"fecha": date(2026, 8, d), "precio_lista": p}
        for d, p in zip(range(1, 5), [100, 200, 300, 400])
    ]
    quotes, _, _, _ = _colapsar(tmp_path, filas)
    assert quotes.iloc[0]["precio_lista_mediana"] == pytest.approx(250.0)


# --------------------------------------------------------------------------- #
# La clave del quote
# --------------------------------------------------------------------------- #


def test_la_clave_incluye_comercio_sucursal_y_producto(tmp_path):
    """Sin id_comercio, dos cadenas con la sucursal "7" colapsarian en un quote."""
    filas = [
        {"fecha": date(2026, 8, 1), "id_comercio": "10", "id_sucursal": "7",
         "id_producto": "EAN1", "precio_lista": 1000},
        {"fecha": date(2026, 8, 1), "id_comercio": "15", "id_sucursal": "7",
         "id_producto": "EAN1", "precio_lista": 2000},
    ]
    quotes, _, n, _ = _colapsar(tmp_path, filas)
    assert n == 2, "se mezclaron dos comercios distintos en un mismo quote"
    assert set(quotes["precio_lista_mediana"]) == {1000.0, 2000.0}


def test_un_producto_en_dos_sucursales_son_dos_quotes(tmp_path):
    filas = [
        {"fecha": date(2026, 8, 1), "id_sucursal": "7", "precio_lista": 1000},
        {"fecha": date(2026, 8, 1), "id_sucursal": "9", "precio_lista": 1200},
    ]
    _, _, n, _ = _colapsar(tmp_path, filas)
    assert n == 2


# --------------------------------------------------------------------------- #
# Lo que hace segura la promesa del TTL
# --------------------------------------------------------------------------- #


def test_no_filtra_los_quotes_por_debajo_del_minimo(tmp_path):
    """Se guardan igual, marcados. Asi el umbral se puede cambiar despues sin
    reprocesar — clave cuando el detalle diario ya venció por TTL."""
    filas = [{"fecha": date(2026, 8, 1), "id_producto": "POCO", "precio_lista": 500}]
    filas += [
        {"fecha": date(2026, 8, d), "id_producto": "MUCHO", "precio_lista": 900}
        for d in range(1, 11)
    ]
    quotes, _, n, n_min = _colapsar(tmp_path, filas, minimo_dias=5)
    assert n == 2, "el quote con pocos dias tiene que quedar guardado"
    assert n_min == 1
    por = dict(zip(quotes["id_producto"], quotes["cumple_minimo_dias"]))
    assert bool(por["MUCHO"]) is True
    assert bool(por["POCO"]) is False
    dias = dict(zip(quotes["id_producto"], quotes["n_dias"]))
    assert dias["POCO"] == 1 and dias["MUCHO"] == 10


def test_guarda_todos_los_comercios_incluidos_los_excluidos_del_analisis(tmp_path):
    """Farmacity y las estaciones de servicio se filtran al calcular el indice,
    no aca: filtrarlos en la tabla permanente seria irreversible tras el TTL."""
    filas = [
        {"fecha": date(2026, 8, 1), "id_comercio": "10", "precio_lista": 1000},
        {"fecha": date(2026, 8, 1), "id_comercio": "24", "precio_lista": 1100},  # Farmacity
        {"fecha": date(2026, 8, 1), "id_comercio": "23", "precio_lista": 1200},  # Axion
    ]
    quotes, _, n, _ = _colapsar(tmp_path, filas)
    assert n == 3
    assert set(quotes["id_comercio"]) == {"10", "24", "23"}


def test_conserva_min_max_y_rango_de_fechas(tmp_path):
    """Permite auditar y re-derivar sin el detalle diario."""
    filas = [
        {"fecha": date(2026, 8, d), "precio_lista": p}
        for d, p in zip([1, 5, 9], [800, 1000, 1500])
    ]
    quotes, _, _, _ = _colapsar(tmp_path, filas)
    f = quotes.iloc[0]
    assert f["precio_lista_min"] == pytest.approx(800.0)
    assert f["precio_lista_max"] == pytest.approx(1500.0)
    assert str(f["primera_fecha"])[:10] == "2026-08-01"
    assert str(f["ultima_fecha"])[:10] == "2026-08-09"


def test_las_dos_series_se_colapsan_por_separado(tmp_path):
    """Precio de lista y precio efectivo son series distintas y comparables."""
    filas = [
        {"fecha": date(2026, 8, d), "precio_lista": 1000, "precio_promo": 800,
         "precio_efectivo": 800}
        for d in range(1, 6)
    ]
    quotes, _, _, _ = _colapsar(tmp_path, filas)
    assert quotes.iloc[0]["precio_lista_mediana"] == pytest.approx(1000.0)
    assert quotes.iloc[0]["precio_efectivo_mediana"] == pytest.approx(800.0)


# --------------------------------------------------------------------------- #
# El catalogo de productos
# --------------------------------------------------------------------------- #


def test_el_catalogo_conserva_las_descripciones(tmp_path):
    """Sin esto quedarian precios que no se pueden reclasificar: los quotes solo
    tienen claves y precios, no dicen que producto es cada uno."""
    filas = [
        {"fecha": date(2026, 8, 1), "id_producto": "EAN1",
         "descripcion": "LECHE ENTERA SACHET", "marca": "LA SERENISIMA"},
    ]
    _, catalogo, _, _ = _colapsar(tmp_path, filas)
    assert len(catalogo) == 1
    f = catalogo.iloc[0]
    assert f["descripcion"] == "LECHE ENTERA SACHET"
    assert f["marca"] == "LA SERENISIMA"
    assert f["unidad_base"] == "l"
    assert f["cantidad_base"] == pytest.approx(1.0)


def test_el_catalogo_elige_la_descripcion_mas_frecuente(tmp_path):
    """El mismo EAN viene descripto distinto en cada cadena."""
    filas = [
        {"fecha": date(2026, 8, d), "id_comercio": "10", "id_producto": "EAN1",
         "descripcion": "LECHE ENTERA LA SERENISIMA SACHET 1L"} for d in range(1, 11)
    ]
    filas += [
        {"fecha": date(2026, 8, 1), "id_comercio": "15", "id_producto": "EAN1",
         "descripcion": "LECHE ENT LV"}
    ]
    _, catalogo, _, _ = _colapsar(tmp_path, filas)
    assert catalogo.iloc[0]["descripcion"] == "LECHE ENTERA LA SERENISIMA SACHET 1L"
    assert catalogo.iloc[0]["n_comercios"] == 2
    alternativas = list(catalogo.iloc[0]["descripciones_alternativas"])
    assert "LECHE ENT LV" in alternativas


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "nombre,esperado",
    [
        ("sepa_1_comercio-sepa-15_2026-07-30_09-05-11.parquet", "15"),
        ("staged/x/sepa_2_comercio-sepa-10_2026-08-01_01-05-08.parquet", "10"),
        ("cualquier-cosa.parquet", None),
    ],
)
def test_identifica_el_comercio_del_nombre_de_archivo(nombre, esperado):
    assert q.comercio_de_archivo(nombre) == esperado
