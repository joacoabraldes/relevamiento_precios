"""ETL diario end-to-end sobre ZIP sinteticos con las rarezas reales del formato."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from conftest import (
    CABECERA_COMERCIO,
    CABECERA_PRODUCTOS,
    CABECERA_SUCURSALES,
    armar_csv,
    armar_zip_comercio,
    armar_zip_diario,
)
from precios.normalize import etl as etl_mod

FECHA = date(2026, 8, 1)


def prod(
    id_comercio="21", bandera="1", sucursal="7", producto="7790070764010", ean="1",
    desc="LECHE ENTERA SACHET", cant="1", unidad="L", marca="LA SERENISIMA",
    precio="1500.00", p_ref="1500.00", c_ref="1", u_ref="L", promo1="", promo2="",
):
    return (f"{id_comercio}|{bandera}|{sucursal}|{producto}|{ean}|{desc}|{cant}|"
            f"{unidad}|{marca}|{precio}|{p_ref}|{c_ref}|{u_ref}|{promo1}||{promo2}|")


def suc(id_comercio="21", bandera="1", sucursal="7", prov="AR-C", loc="CABA"):
    return (f"{id_comercio}|{bandera}|{sucursal}|Suc Centro|Supermercado|Corrientes|"
            f"1234|-34.6|-58.4|||1043|{loc}|{prov}|08:00 a 21:00|08:00 a 21:00|"
            f"08:00 a 21:00|08:00 a 21:00|08:00 a 21:00|09:00 a 20:00|cerrado")


def com(id_comercio="21"):
    return (f"{id_comercio}|1|30123456787|Super SRL|El Super|http://x.ar|"
            f"2026-08-01T02:00:00-03:00|1.0")


def correr(tmp_path, cfg, unidades, provincias, comercios) -> tuple:
    z = armar_zip_diario(tmp_path / "dia.zip", FECHA.isoformat(), comercios)
    salida = tmp_path / "out"
    resumen = etl_mod.procesar_zip(z, FECHA, cfg, salida, unidades, provincias)
    con = duckdb.connect()
    obs_dir = salida / "observaciones"
    archivos = list(obs_dir.glob("*.parquet"))
    obs = (
        con.execute(
            f"SELECT * FROM read_parquet('{obs_dir.as_posix()}/*.parquet')"
        ).fetchdf()
        if archivos
        else None
    )
    rech_dir = salida / "rechazados"
    rech = (
        con.execute(
            f"SELECT * FROM read_parquet('{rech_dir.as_posix()}/*.parquet')"
        ).fetchdf()
        if list(rech_dir.glob("*.parquet"))
        else None
    )
    return resumen, obs, rech


# --------------------------------------------------------------------------- #
# Formato
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bom", [True, False])
@pytest.mark.parametrize("crlf", [True, False])
@pytest.mark.parametrize(
    "pie",
    [
        "Última actualización: 2026-08-01T02:02:18-03:00",
        "Ultima actualización: 2026-08-01T09:06:46-03:00",  # un comercio la escribe sin tilde
        None,
    ],
)
def test_lee_todas_las_variantes_de_formato(tmp_path, cfg, unidades, provincias, bom, crlf, pie):
    """BOM inconsistente, CRLF/LF mezclados y la linea de pie con y sin tilde."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(), prod(producto="7790070764027")],
                  bom=bom, crlf=crlf, pie=pie)
    s = armar_csv(CABECERA_SUCURSALES, [suc()], bom=bom, crlf=crlf, pie=pie)
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-21_x.zip": armar_zip_comercio(p, s, armar_csv(CABECERA_COMERCIO, [com()]))},
    )
    assert resumen.comercios_ok == 1
    assert resumen.observaciones == 2
    # La linea de pie nunca puede colarse como observacion.
    assert obs["id_producto"].notna().all()
    assert not obs["id_producto"].astype(str).str.contains("ctualiza").any()


def test_el_pie_no_cuenta_como_rechazo(tmp_path, cfg, unidades, provincias):
    """No es un dato malo: es metadata. No debe ensuciar la tabla de rechazados."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod()])
    resumen, _, rech = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert resumen.rechazos == 0
    assert rech is None


# --------------------------------------------------------------------------- #
# Tipado y normalizacion
# --------------------------------------------------------------------------- #


def test_id_producto_conserva_ceros_a_la_izquierda(tmp_path, cfg, unidades, provincias):
    """Si se tipara como numero, 0000000060257 se convierte en 60257."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(producto="0000000060257")])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert obs.iloc[0]["id_producto"] == "0000000060257"


def test_descripcion_y_marca_normalizadas(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS,
                  [prod(desc="  leche   entera  sachet ", marca=" la serenisima ")])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert obs.iloc[0]["descripcion"] == "LECHE ENTERA SACHET"
    assert obs.iloc[0]["marca"] == "LA SERENISIMA"


@pytest.mark.parametrize(
    "crudo,esperado",
    [("1500.50", 1500.50), ("1500,50", 1500.50), ("1.500,50", 1500.50),
     ("1,500.50", 1500.50), ("  1500.50  ", 1500.50), ("1500", 1500.0)],
)
def test_unifica_separadores_decimales(tmp_path, cfg, unidades, provincias, crudo, esperado):
    p = armar_csv(CABECERA_PRODUCTOS, [prod(precio=crudo)])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert obs.iloc[0]["precio_lista"] == pytest.approx(esperado)


def test_provincia_se_decodifica_con_el_maestro(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS, [prod(sucursal="7"), prod(sucursal="9", producto="123")])
    s = armar_csv(CABECERA_SUCURSALES, [suc(sucursal="7", prov="AR-C"),
                                        suc(sucursal="9", prov="AR-X")])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias, {"c1.zip": armar_zip_comercio(p, s)}
    )
    por_suc = dict(zip(obs["id_sucursal"], obs["provincia"]))
    assert por_suc["7"] == "CABA"
    assert por_suc["9"] == "Córdoba"


def test_provincia_ausente_no_se_inventa(tmp_path, cfg, unidades, provincias):
    """36 de 849 sucursales reales vienen sin provincia."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(sucursal="7")])
    s = armar_csv(CABECERA_SUCURSALES, [suc(sucursal="7", prov="")])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias, {"c1.zip": armar_zip_comercio(p, s)}
    )
    assert obs.iloc[0]["provincia"] == "DESCONOCIDA"


def test_unidades_equivalentes_dan_la_misma_cantidad_base(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS, [
        prod(producto="A", cant="1", unidad="L"),
        prod(producto="B", cant="1000", unidad="ML"),
        prod(producto="C", cant="1", unidad="LT"),
    ])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    base = dict(zip(obs["id_producto"], obs["cantidad_base"]))
    assert base["A"] == base["B"] == base["C"] == pytest.approx(1.0)
    assert set(obs["unidad_base"]) == {"l"}


def test_unidad_desconocida_no_descarta_el_precio(tmp_path, cfg, unidades, provincias):
    """Una unidad rara no justifica tirar una observacion de precio valida."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(unidad="BANANAS")])
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert resumen.observaciones == 1
    assert resumen.unidad_desconocida == 1
    assert obs.iloc[0]["precio_lista"] == pytest.approx(1500.0)
    assert obs.iloc[0]["cantidad_base"] is None or str(obs.iloc[0]["cantidad_base"]) == "nan"
    assert obs.iloc[0]["unidad_presentacion_raw"] == "BANANAS"


def test_precio_efectivo_usa_promo_cuando_hay(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS, [
        prod(producto="A", precio="1000.00", promo1="800.00"),
        prod(producto="B", precio="1000.00", promo1=""),
    ])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    ef = dict(zip(obs["id_producto"], obs["precio_efectivo"]))
    assert ef["A"] == pytest.approx(800.0)
    assert ef["B"] == pytest.approx(1000.0)


def test_flag_es_ean(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS, [prod(producto="A", ean="1"), prod(producto="B", ean="0")])
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    flags = dict(zip(obs["id_producto"], obs["es_ean"]))
    assert flags["A"] is True or flags["A"] == True  # noqa: E712
    assert flags["B"] is False or flags["B"] == False  # noqa: E712


# --------------------------------------------------------------------------- #
# Rechazos
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "precio,motivo",
    [("0", etl_mod.MOTIVO_PRECIO_NO_POS),
     ("0.00", etl_mod.MOTIVO_PRECIO_NO_POS),
     ("-5", etl_mod.MOTIVO_PRECIO_NO_POS),
     ("", etl_mod.MOTIVO_PRECIO_NULO),
     ("N/D", etl_mod.MOTIVO_PRECIO_NO_NUM)],
)
def test_precios_invalidos_van_a_rechazados_con_motivo(
    tmp_path, cfg, unidades, provincias, precio, motivo
):
    """Un precio 0 es un dato ausente disfrazado, no un precio bajo."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(producto="MALO", precio=precio),
                                       prod(producto="BUENO", precio="1500")])
    resumen, obs, rech = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert resumen.observaciones == 1
    assert resumen.rechazos == 1
    assert list(obs["id_producto"]) == ["BUENO"]
    assert rech.iloc[0]["motivo_rechazo"] == motivo
    assert rech.iloc[0]["id_producto"] == "MALO"


def test_nada_se_descarta_en_silencio(tmp_path, cfg, unidades, provincias):
    """observaciones + rechazos + duplicados == filas de datos del archivo."""
    filas = [prod(producto=f"P{i}", precio="1500") for i in range(5)]
    filas += [prod(producto="Z1", precio="0"), prod(producto="Z2", precio="")]
    filas += [prod(producto="P0", precio="1600")]  # duplicado de P0
    p = armar_csv(CABECERA_PRODUCTOS, filas)
    resumen, _, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert resumen.observaciones + resumen.rechazos + resumen.duplicados == len(filas)


# --------------------------------------------------------------------------- #
# Deduplicacion
# --------------------------------------------------------------------------- #


def test_dedup_se_queda_con_el_ultimo(tmp_path, cfg, unidades, provincias):
    p = armar_csv(CABECERA_PRODUCTOS, [
        prod(producto="X", precio="1000"),
        prod(producto="X", precio="2000"),
        prod(producto="X", precio="3000"),   # este gana
    ])
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))},
    )
    assert resumen.observaciones == 1
    assert resumen.duplicados == 2
    assert obs.iloc[0]["precio_lista"] == pytest.approx(3000.0)


def test_mismo_producto_y_sucursal_en_comercios_distintos_no_se_mezclan(
    tmp_path, cfg, unidades, provincias
):
    """El bug que tenia el spec: id_sucursal NO es unico entre comercios.

    Medido sobre datos reales: (id_sucursal, id_producto) da 33.146 claves
    duplicadas; agregando id_comercio da 0.
    """
    p1 = armar_csv(CABECERA_PRODUCTOS, [prod(id_comercio="21", sucursal="7",
                                             producto="MISMO", precio="1000")])
    p2 = armar_csv(CABECERA_PRODUCTOS, [prod(id_comercio="10", sucursal="7",
                                             producto="MISMO", precio="2000")])
    s1 = armar_csv(CABECERA_SUCURSALES, [suc(id_comercio="21", sucursal="7", prov="AR-C")])
    s2 = armar_csv(CABECERA_SUCURSALES, [suc(id_comercio="10", sucursal="7", prov="AR-B")])
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"c21.zip": armar_zip_comercio(p1, s1), "c10.zip": armar_zip_comercio(p2, s2)},
    )
    assert resumen.observaciones == 2, "se colapsaron dos quotes distintos"
    assert resumen.duplicados == 0
    assert set(obs["precio_lista"]) == {1000.0, 2000.0}
    assert set(obs["provincia"]) == {"CABA", "Buenos Aires"}


# --------------------------------------------------------------------------- #
# Robustez
# --------------------------------------------------------------------------- #


def test_un_comercio_roto_no_tumba_el_dia(tmp_path, cfg, unidades, provincias):
    """Pasa de verdad: comercio-sepa-36 publico un ZIP de 0 bytes el 2026-08-01."""
    bueno = armar_zip_comercio(
        armar_csv(CABECERA_PRODUCTOS, [prod()]),
        armar_csv(CABECERA_SUCURSALES, [suc()]),
    )
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {
            "sepa_2_comercio-sepa-36_x.zip": b"",              # vacio
            "sepa_2_comercio-sepa-99_x.zip": b"no soy un zip",  # corrupto
            "sepa_1_comercio-sepa-21_x.zip": bueno,
        },
    )
    assert resumen.comercios_ok == 1
    assert resumen.comercios_sin_datos == 2
    assert resumen.observaciones == 1
    # Un ZIP vacio en el origen es anomalia de la fuente, no falla del pipeline:
    # pasa seguido y no debe disparar la misma alerta que un bug nuestro.
    assert resumen.hubo_anomalias
    assert not resumen.hubo_fallas


def test_comercio_sin_sucursales_csv_sigue_produciendo_precios(
    tmp_path, cfg, unidades, provincias
):
    p = armar_csv(CABECERA_PRODUCTOS, [prod()])
    resumen, obs, _ = correr(
        tmp_path, cfg, unidades, provincias, {"c1.zip": armar_zip_comercio(p)}
    )
    assert resumen.observaciones == 1
    assert obs.iloc[0]["provincia"] == "DESCONOCIDA"


def test_reprocesar_es_idempotente(tmp_path, cfg, unidades, provincias):
    """Correr dos veces el mismo dia da exactamente el mismo resultado."""
    p = armar_csv(CABECERA_PRODUCTOS, [prod(producto=f"P{i}") for i in range(10)])
    comercios = {"c1.zip": armar_zip_comercio(p, armar_csv(CABECERA_SUCURSALES, [suc()]))}
    z = armar_zip_diario(tmp_path / "dia.zip", FECHA.isoformat(), comercios)
    salida = tmp_path / "out"

    r1 = etl_mod.procesar_zip(z, FECHA, cfg, salida, unidades, provincias)
    con = duckdb.connect()
    q = f"SELECT * FROM read_parquet('{(salida / 'observaciones').as_posix()}/*.parquet') ORDER BY id_producto"
    df1 = con.execute(q).fetchdf()
    archivos1 = sorted(x.name for x in (salida / "observaciones").glob("*.parquet"))

    r2 = etl_mod.procesar_zip(z, FECHA, cfg, salida, unidades, provincias)
    df2 = con.execute(q).fetchdf()
    archivos2 = sorted(x.name for x in (salida / "observaciones").glob("*.parquet"))

    assert r1.observaciones == r2.observaciones
    assert archivos1 == archivos2, "la particion acumulo archivos en vez de reescribirse"
    assert df1.equals(df2)


# --------------------------------------------------------------------------- #
# Dimensiones de sucursal y comercio
#
# Todo esto ya venia en los CSV de SEPA y se tiraba. El nombre de bandera existe
# para que los reportes no muestren "comercio 20"; el codigo postal y las
# coordenadas, para poder separar GBA de Pampeana, que pesan 78,9% del IPC y hoy
# van juntas.
# --------------------------------------------------------------------------- #


def test_el_nombre_de_bandera_llega_a_la_observacion(tmp_path, cfg, unidades, provincias):
    """SEPA numera informantes; nadie sabe que es el "comercio 21"."""
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-21_x.zip": armar_zip_comercio(
            armar_csv(CABECERA_PRODUCTOS, [prod()]),
            armar_csv(CABECERA_SUCURSALES, [suc()]),
            armar_csv(CABECERA_COMERCIO, [com()]))},
    )
    assert obs["comercio_nombre"].tolist() == ["El Super"]
    assert obs["comercio_razon_social"].tolist() == ["Super SRL"]


def test_cada_bandera_lleva_su_propio_nombre(tmp_path, cfg, unidades, provincias):
    """Una empresa opera varios formatos: Cencosud es Vea, Disco y Jumbo.

    El nombre se resuelve por (id_comercio, id_bandera), no por comercio, o
    todas las sucursales de Cencosud dirian lo mismo.
    """
    filas_com = [
        "9|1|30123456787|Cencosud S.A.|Vea|http://x.ar|2026-08-01T02:00:00-03:00|1.0",
        "9|2|30123456787|Cencosud S.A.|Jumbo|http://x.ar|2026-08-01T02:00:00-03:00|1.0",
    ]
    productos = [
        prod(id_comercio="9", bandera="1", sucursal="1"),
        prod(id_comercio="9", bandera="2", sucursal="2"),
    ]
    sucursales = [
        suc(id_comercio="9", bandera="1", sucursal="1"),
        suc(id_comercio="9", bandera="2", sucursal="2"),
    ]
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-9_x.zip": armar_zip_comercio(
            armar_csv(CABECERA_PRODUCTOS, productos),
            armar_csv(CABECERA_SUCURSALES, sucursales),
            armar_csv(CABECERA_COMERCIO, filas_com))},
    )
    por_bandera = dict(zip(obs["id_bandera"], obs["comercio_nombre"]))
    assert por_bandera == {"1": "Vea", "2": "Jumbo"}


def test_localidad_codigo_postal_y_coordenadas_llegan(tmp_path, cfg, unidades, provincias):
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-21_x.zip": armar_zip_comercio(
            armar_csv(CABECERA_PRODUCTOS, [prod()]),
            armar_csv(CABECERA_SUCURSALES, [suc()]),
            armar_csv(CABECERA_COMERCIO, [com()]))},
    )
    assert obs["localidad"].tolist() == ["CABA"]
    assert obs["codigo_postal"].tolist() == ["1043"]
    assert obs["latitud"].tolist() == [-34.6]
    assert obs["longitud"].tolist() == [-58.4]


def test_sin_comercio_csv_el_dia_se_procesa_igual(tmp_path, cfg, unidades, provincias):
    """Un ZIP incompleto no puede tumbar el dia: pasa todos los dias.

    Se pierde el nombre, no la observacion. El precio es el dato irrecuperable.
    """
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-21_x.zip": armar_zip_comercio(
            armar_csv(CABECERA_PRODUCTOS, [prod()]),
            armar_csv(CABECERA_SUCURSALES, [suc()]),
            None)},
    )
    assert obs is not None and len(obs) == 1
    assert obs["comercio_nombre"].isna().all()
    assert obs["precio_lista"].tolist() == [1500.0]


def test_una_coordenada_ilegible_no_rompe_la_observacion(tmp_path, cfg, unidades, provincias):
    """TRY_CAST: una latitud basura deja NULL y sigue, no aborta el comercio."""
    rota = (f"21|1|7|Suc Centro|Supermercado|Corrientes|1234|N/D|-58.4|||1043|"
            f"CABA|AR-C|08:00 a 21:00|08:00 a 21:00|08:00 a 21:00|08:00 a 21:00|"
            f"08:00 a 21:00|09:00 a 20:00|cerrado")
    _, obs, _ = correr(
        tmp_path, cfg, unidades, provincias,
        {"sepa_1_comercio-sepa-21_x.zip": armar_zip_comercio(
            armar_csv(CABECERA_PRODUCTOS, [prod()]),
            armar_csv(CABECERA_SUCURSALES, [rota]),
            armar_csv(CABECERA_COMERCIO, [com()]))},
    )
    assert len(obs) == 1
    assert obs["latitud"].isna().all()
    assert obs["longitud"].tolist() == [-58.4]
