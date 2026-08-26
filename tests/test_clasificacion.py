"""Clasificacion de productos: taxonomia, reglas y propuestas.

Varios tests son regresiones de falsos positivos reales detectados corriendo
sobre los datos del 2026-07-30.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from precios.classify.proponer import (
    AMBIGUA_ENTRE_CATEGORIAS,
    ASIGNADA,
    Variante,
    proponer,
)
from precios.classify.taxonomia import Taxonomia, normalizar_texto

RAIZ = Path(__file__).resolve().parents[1]


@pytest.fixture
def tax() -> Taxonomia:
    return Taxonomia.desde_yaml(RAIZ / "config" / "categorias.yaml")


def _prod(descripciones, cantidad=1.0, unidad="kg"):
    """Catalogo de un solo producto con N variantes."""
    if isinstance(descripciones, str):
        descripciones = [descripciones]
    if not isinstance(cantidad, list):
        cantidad = [cantidad] * len(descripciones)
    return {
        "EAN": [
            Variante(d, "MARCA", c, unidad, 100)
            for d, c in zip(descripciones, cantidad)
        ]
    }


# --------------------------------------------------------------------------- #
# Normalizacion de texto
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "crudo,esperado",
    [
        ("azúcar", "AZUCAR"),
        ("MAÑANITA", "MANANITA"),
        ("  leche   entera  ", "LECHE ENTERA"),
        ("Café", "CAFE"),
        (None, ""),
    ],
)
def test_normalizacion_de_texto(crudo, esperado):
    """Los comercios escriben AZUCAR y AZÚCAR indistintamente."""
    assert normalizar_texto(crudo) == esperado


# --------------------------------------------------------------------------- #
# Taxonomia
# --------------------------------------------------------------------------- #


def test_taxonomia_del_repo_es_valida(tax: Taxonomia):
    assert len(tax) >= 10, "el piloto pide 10-15 categorias elementales"
    for r in tax.reglas:
        assert r.clase in tax.clases, f"{r.codigo}: clase huerfana"
        assert r.patrones, f"{r.codigo}: sin patrones"


def test_categoria_con_clase_inexistente_falla(tmp_path):
    mala = tmp_path / "c.yaml"
    mala.write_text(
        "divisiones: {'01': {nombre: X, grupos: {'01.1': {nombre: Y, clases: "
        "{'01.1.1': {nombre: Z}}}}}}\n"
        "categorias:\n  x.y:\n    clase: '09.9.9'\n    nombre: X\n"
        "    patrones: ['\\bX\\b']\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="clase"):
        Taxonomia.desde_yaml(mala)


def test_regex_invalido_falla_al_cargar(tmp_path):
    mala = tmp_path / "c.yaml"
    mala.write_text(
        "divisiones: {'01': {nombre: X, grupos: {'01.1': {nombre: Y, clases: "
        "{'01.1.1': {nombre: Z}}}}}}\n"
        "categorias:\n  x.y:\n    clase: '01.1.1'\n    nombre: X\n"
        "    patrones: ['[sin cerrar']\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="regex"):
        Taxonomia.desde_yaml(mala)


# --------------------------------------------------------------------------- #
# El caso 000 vs 0000
# --------------------------------------------------------------------------- #


def test_harina_000_no_matchea_0000(tax: Taxonomia):
    """`\\b000\\b` no puede matchear "0000": es lo que separa las dos categorias."""
    p, _ = proponer(_prod("HARINA DE TRIGO 000"), tax)
    assert p[0].categoria == "almacen.harina_trigo_000_1kg"

    p, _ = proponer(_prod("HARINA 0000"), tax)
    assert p[0].categoria == "almacen.harina_trigo_0000_1kg"


def test_harina_000_y_0000_nunca_se_solapan(tax: Taxonomia):
    for desc in ["HARINA 000", "HARINA 0000", "HARINA DE TRIGO 000 C/VITAMINA"]:
        cats = tax.clasificar_producto([desc], [(1.0, "kg")])
        assert len(cats) <= 1, f"{desc!r} entro en {cats}"


# --------------------------------------------------------------------------- #
# Exclusiones evaluadas sobre TODAS las variantes
# --------------------------------------------------------------------------- #


def test_exclusion_en_una_variante_descarta_el_producto(tax: Taxonomia):
    """Regresion real (EAN 7791120037559).

    Una cadena describe el producto como "ARROZ ALA DORADO LARGO FINO" y otra
    como "ARROZ PARBOIL". Es arroz parboil: la exclusion de una descripcion
    tiene que pesar mas que el match de la otra.
    """
    catalogo = _prod(["ARROZ ALA DORADO LARGO FINO 00000", "ARROZ PARBOIL"])
    propuestas, resumen = proponer(catalogo, tax)
    assert propuestas == []
    assert resumen.productos_asignados == 0


def test_palmeritas_de_manteca_no_es_manteca(tax: Taxonomia):
    """Regresion real (EAN 7791696008014): reposteria que lleva manteca en el nombre."""
    catalogo = _prod(
        ["PALMERITAS DE MANTEC", "PALMERITAS MANTECA X 200 GR"],
        cantidad=0.2, unidad="kg",
    )
    assert proponer(catalogo, tax)[0] == []


def test_harina_para_pizza_no_es_harina_comun(tax: Taxonomia):
    """Regresion real (EAN 7792590001156)."""
    catalogo = _prod(
        ["HARINA FRACCIO PIZZA", "HARINA DE TRIGO 0000 P/PIZZA CASERITA"]
    )
    assert proponer(catalogo, tax)[0] == []


def test_manteca_de_verdad_sigue_entrando(tax: Taxonomia):
    p, _ = proponer(_prod("MANTECA X 200GR", cantidad=0.2), tax)
    assert p[0].categoria == "lacteos.manteca_200g"


# --------------------------------------------------------------------------- #
# Inclusiones permisivas: rescatan descripciones truncadas
# --------------------------------------------------------------------------- #


def test_una_variante_completa_rescata_a_las_truncadas(tax: Taxonomia):
    """Las descripciones vienen cortadas a ~20 caracteres en varias cadenas.

    "YOG FIRME VAINILLA" no matchea `\\bYOGUR\\b`, pero si otra cadena informa el
    nombre completo, el producto igual se clasifica.
    """
    catalogo = _prod(
        ["YOG FIRME VAINILLA", "YOGUR POTE FIRME VAINILLA YOGURISIMO"],
        cantidad=0.19,
    )
    p, _ = proponer(catalogo, tax)
    assert p[0].categoria == "lacteos.yogur_firme_pote"


def test_solo_variantes_truncadas_no_alcanza(tax: Taxonomia):
    """Si ninguna descripcion cumple los patrones, no se inventa la categoria."""
    assert proponer(_prod("YOG FIRM VAIN", cantidad=0.19), tax)[0] == []


def test_los_patrones_deben_cumplirse_en_una_misma_variante(tax: Taxonomia):
    """No se pueden ensamblar patrones tomando pedazos de descripciones distintas."""
    catalogo = _prod(["ARROZ INTEGRAL PREMIUM", "LARGO FINO SELECTO"])
    assert proponer(catalogo, tax)[0] == []


# --------------------------------------------------------------------------- #
# Presentacion
# --------------------------------------------------------------------------- #


def test_presentacion_fuera_de_rango_no_clasifica(tax: Taxonomia):
    """Las barritas de arroz de 20 g no son arroz de 1 kg."""
    assert proponer(_prod("ARROZ LARGO FINO", cantidad=0.02), tax)[0] == []


def test_una_presentacion_valida_alcanza(tax: Taxonomia):
    """Hay cadenas que informan "1 unidad" y pierden el peso real.

    Si otra cadena informa bien la presentacion, el producto se clasifica igual.
    """
    catalogo = {
        "EAN": [
            Variante("ARROZ LARGO FINO X 1 KG", "M", 1.0, "un", 500),
            Variante("ARROZ LARGO FINO", "M", 1.0, "kg", 100),
        ]
    }
    p, _ = proponer(catalogo, tax)
    assert p[0].categoria == "almacen.arroz_largo_fino_1kg"


def test_sin_cantidad_normalizada_no_se_adivina(tax: Taxonomia):
    catalogo = {"EAN": [Variante("ARROZ LARGO FINO", "M", None, None, 10)]}
    assert proponer(catalogo, tax)[0] == []


# --------------------------------------------------------------------------- #
# Ambiguedad
# --------------------------------------------------------------------------- #


def test_producto_en_dos_categorias_va_a_revision(tmp_path):
    """Un solapamiento de reglas se reporta, no se resuelve por orden de lista."""
    yaml = tmp_path / "c.yaml"
    yaml.write_text(
        "divisiones: {'01': {nombre: X, grupos: {'01.1': {nombre: Y, clases: "
        "{'01.1.1': {nombre: Z}}}}}}\n"
        "categorias:\n"
        "  a.uno:\n    clase: '01.1.1'\n    nombre: Uno\n    patrones: ['\\bLECHE\\b']\n"
        "  a.dos:\n    clase: '01.1.1'\n    nombre: Dos\n    patrones: ['\\bENTERA\\b']\n",
        encoding="utf-8",
    )
    tax = Taxonomia.desde_yaml(yaml)
    propuestas, resumen = proponer(_prod("LECHE ENTERA"), tax)
    assert propuestas[0].estado == AMBIGUA_ENTRE_CATEGORIAS
    assert propuestas[0].categoria is None
    assert propuestas[0].categorias_candidatas == ["a.dos", "a.uno"]
    assert resumen.ambiguos_entre_categorias == 1
    assert resumen.productos_asignados == 0


def test_la_taxonomia_del_repo_no_tiene_solapamientos(tax: Taxonomia):
    """Con descripciones reales del dataset, ninguna cae en dos categorias."""
    casos = [
        ("LECHE ENTERA 3% SACH", 1.0, "l"),
        ("LECHE DESC BOTELLA", 1.0, "l"),
        ("YOGUR FIRME VAINILLA", 0.19, "kg"),
        ("YOGUR BEBIBLE VAINIL", 0.9, "kg"),
        ("ACEITE DE GIRASOL", 0.9, "l"),
        ("ACEITE MEZCLA", 0.9, "l"),
        ("HARINA 000", 1.0, "kg"),
        ("HARINA 0000", 1.0, "kg"),
        ("YERBA MATE 4 FLEX", 0.5, "kg"),
        ("YERBA MATE 4FLEX", 1.0, "kg"),
        ("FIDEOS SPAGHETTI", 0.5, "kg"),
        ("AZUCAR COMUN TIPO A", 1.0, "kg"),
        ("SAL FINA PAQUETE", 0.5, "kg"),
        ("MANTECA X 200GR", 0.2, "kg"),
        ("ARROZ LARGO FINO", 1.0, "kg"),
    ]
    for desc, cant, unidad in casos:
        cats = tax.clasificar_producto([desc], [(cant, unidad)])
        assert len(cats) == 1, f"{desc!r} -> {cats}"


# --------------------------------------------------------------------------- #
# Falsos positivos del muestreo real
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "descripcion,cantidad,unidad",
    [
        ("BARRA ARROZ TRADICIO", 0.06, "kg"),
        ("ALFAJOR ARROZ DDL", 0.022, "kg"),
        ("GALLETAS AZUCARADAS", 0.139, "kg"),
        ("COPOS AZUCARADOS", 0.2, "kg"),
        ("CEREALES AZUCARADOS", 0.5, "kg"),
        ("CREMA DE LECHE", 0.2, "l"),
        ("LECHE INFANTIL 1", 0.2, "l"),
        ("LECHE CHOCOLATADA", 1.0, "l"),
        ("HARINA DE MANI", 0.45, "kg"),
        ("BARRA FRUT C/YOGUR", 0.025, "kg"),
        ("ARROZ C/LECHE CLASIC", 0.18, "kg"),
        ("ACEITE OLIVA EXT VIR", 0.9, "l"),
    ],
)
def test_falsos_positivos_conocidos_quedan_afuera(tax, descripcion, cantidad, unidad):
    """Todos aparecen en los datos reales al buscar por palabra suelta."""
    cats = tax.clasificar_producto([descripcion], [(cantidad, unidad)])
    assert cats == [], f"{descripcion!r} entro en {cats}"


# --------------------------------------------------------------------------- #
# Resumen
# --------------------------------------------------------------------------- #


def test_resumen_cuenta_productos_y_observaciones(tax: Taxonomia):
    catalogo = {
        "A": [Variante("ARROZ LARGO FINO", "M", 1.0, "kg", 300)],
        "B": [Variante("HARINA 000", "M", 1.0, "kg", 200)],
        "C": [Variante("ALGO QUE NO ENTRA", "M", 1.0, "kg", 500)],
    }
    _, r = proponer(catalogo, tax)
    assert r.productos_totales == 3
    assert r.obs_totales == 1000
    assert r.productos_asignados == 2
    assert r.obs_asignadas == 500
    assert r.cobertura_obs == pytest.approx(50.0)


# --------------------------------------------------------------------------- #
# Informantes excluidos
# --------------------------------------------------------------------------- #


def test_lista_de_comercios_del_repo_es_valida():
    from precios.comercios import ListaComercios

    lista = ListaComercios.desde_yaml(RAIZ / "config" / "comercios.yaml")
    assert len(lista) >= 1
    for i in lista.ids:
        d = lista.detalle(i)
        assert d is not None and d.nombre and d.motivo, f"{i}: falta nombre o motivo"


def test_filtro_sql_deja_afuera_a_los_excluidos(tmp_path):
    import duckdb

    from precios.comercios import ListaComercios

    y = tmp_path / "c.yaml"
    y.write_text(
        "excluidos:\n"
        "  '24': {nombre: Farmacity, motivo: farmacia}\n"
        "  '23': {nombre: Axion, motivo: estacion de servicio}\n",
        encoding="utf-8",
    )
    lista = ListaComercios.desde_yaml(y)
    assert lista.esta_excluido("24") and lista.esta_excluido(23)
    assert not lista.esta_excluido("10")

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE o AS SELECT * FROM (VALUES ('10'),('23'),('24'),('15')) t(id_comercio)"
    )
    quedan = con.execute(
        f"SELECT id_comercio FROM o WHERE {lista.filtro_sql()} ORDER BY 1"
    ).fetchall()
    assert [r[0] for r in quedan] == ["10", "15"]


def test_sin_archivo_de_exclusiones_no_filtra_nada(tmp_path):
    from precios.comercios import ListaComercios

    lista = ListaComercios.desde_yaml(tmp_path / "no-existe.yaml")
    assert len(lista) == 0
    assert lista.filtro_sql() == "TRUE"


# --------------------------------------------------------------------------- #
# El mapeo versionado conserva la revision humana
# --------------------------------------------------------------------------- #


def _propuesta(id_producto, categoria, n_obs=100):
    from precios.classify.proponer import ASIGNADA, Propuesta

    return Propuesta(
        id_producto=id_producto, categoria=categoria, estado=ASIGNADA,
        descripcion="ALGO", marca="M", cantidad_base=1.0, unidad_base="kg",
        n_obs=n_obs, n_comercios=3,
    )


def _leer(path):
    import csv

    with path.open(encoding="utf-8", newline="") as fh:
        return {f["id_producto"]: f for f in csv.DictReader(fh)}


def test_una_revision_a_mano_sobrevive_a_regenerar_el_mapeo(tmp_path):
    """EL bug: `clasificar` reescribia 'auto'/'no' en TODAS las filas.

    Con eso, sentarse a revisar productos era trabajo que se borraba solo la
    proxima vez que alguien corriera el comando.
    """
    from precios.classify.proponer import escribir_mapeo

    destino = tmp_path / "mapeo.csv"
    escribir_mapeo([_propuesta("111", "almacen.sal_fina_500g")], destino)
    assert _leer(destino)["111"]["revisado"] == "no"

    # Alguien lo revisa y ademas lo recategoriza.
    filas = destino.read_text(encoding="utf-8").replace(
        "almacen.sal_fina_500g,ALGO,M,1,kg,100,auto,no",
        "almacen.azucar_comun_1kg,ALGO,M,1,kg,100,manual,si",
    )
    destino.write_text(filas, encoding="utf-8")

    # La regla sigue proponiendo la categoria vieja: no puede ganarle a la persona.
    n, conservadas = escribir_mapeo(
        [_propuesta("111", "almacen.sal_fina_500g")], destino
    )
    fila = _leer(destino)["111"]
    assert fila["revisado"] == "si"
    assert fila["origen"] == "manual"
    assert fila["categoria"] == "almacen.azucar_comun_1kg"
    assert conservadas == 1


def test_un_producto_revisado_que_desaparece_del_catalogo_se_conserva(tmp_path):
    """Su EAN puede volver el mes que viene: no se pide revisarlo dos veces."""
    from precios.classify.proponer import escribir_mapeo

    destino = tmp_path / "mapeo.csv"
    escribir_mapeo([_propuesta("111", "almacen.sal_fina_500g")], destino)
    destino.write_text(
        destino.read_text(encoding="utf-8").replace(",auto,no", ",manual,si"),
        encoding="utf-8",
    )

    n, conservadas = escribir_mapeo([_propuesta("222", "almacen.azucar_comun_1kg")], destino)
    leido = _leer(destino)
    assert set(leido) == {"111", "222"}
    assert leido["111"]["revisado"] == "si"
    assert leido["222"]["revisado"] == "no"


def test_un_producto_sin_revisar_si_se_pisa_con_la_propuesta_nueva(tmp_path):
    """Lo que nadie miro se regenera: la regla manda mientras no haya decision."""
    from precios.classify.proponer import escribir_mapeo

    destino = tmp_path / "mapeo.csv"
    escribir_mapeo([_propuesta("111", "almacen.sal_fina_500g")], destino)
    escribir_mapeo([_propuesta("111", "almacen.azucar_comun_1kg")], destino)
    assert _leer(destino)["111"]["categoria"] == "almacen.azucar_comun_1kg"
