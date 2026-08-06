"""Normalizacion de unidades: lo que habilita comparar 1 L contra 1000 ML."""

from __future__ import annotations

import pytest

from precios.normalize.unidades import BASES, TablaUnidades


def test_codigos_reales_del_dataset_estan_cubiertos(unidades: TablaUnidades):
    """Los 15 codigos medidos sobre datos reales del 2026-08-01."""
    observados = ["UNI", "EA", "KG", "L", "KGM", "GR", "ML", "CC", "UN",
                  "M", "G", "GRM", "CM3", "M2", "LT"]
    faltantes = [c for c in observados if c not in unidades]
    assert not faltantes, f"unidades sin mapear: {faltantes}"


@pytest.mark.parametrize("crudo", ["kg", "KG", " kg ", "Kg.", "K G"])
def test_lookup_es_insensible_a_formato(unidades: TablaUnidades, crudo: str):
    u = unidades.buscar(crudo)
    assert u is not None and u.canonica == "kg"


@pytest.mark.parametrize(
    "cantidad,codigo,esperado_base,esperada_unidad",
    [
        (1, "L", 1.0, "l"),
        (1000, "ML", 1.0, "l"),        # 1000 ml == 1 l
        (1000, "CC", 1.0, "l"),        # cc y ml son lo mismo
        (1, "KG", 1.0, "kg"),
        (500, "GR", 0.5, "kg"),
        (500, "GRM", 0.5, "kg"),
        (1, "UNI", 1.0, "un"),
        (1, "EA", 1.0, "un"),          # "each", codigo en ingles
        (6, "UN", 6.0, "un"),
        (1, "DOC", 12.0, "un"),
    ],
)
def test_conversion_a_unidad_base(
    unidades: TablaUnidades, cantidad, codigo, esperado_base, esperada_unidad
):
    base, unidad_base, _ = unidades.convertir(cantidad, codigo)
    assert base == pytest.approx(esperado_base)
    assert unidad_base == esperada_unidad


def test_un_litro_y_mil_mililitros_colapsan_igual(unidades: TablaUnidades):
    """El caso que necesita la Etapa 2 para agrupar 'leche sachet 1L'.

    La unidad canonica sigue siendo distinta (l vs ml) porque conserva lo que
    informo el comercio; lo que tiene que coincidir es la cantidad base.
    """
    a = unidades.convertir(1, "L")
    b = unidades.convertir(1000, "ML")
    c = unidades.convertir(1, "LT")
    assert a[:2] == b[:2] == c[:2] == (1.0, "l")


def test_unidad_desconocida_no_inventa_conversion(unidades: TablaUnidades):
    assert unidades.convertir(5, "BANANAS") == (None, None, None)
    assert unidades.convertir(5, None) == (None, None, None)
    assert unidades.convertir(5, "") == (None, None, None)


def test_cantidad_nula_conserva_la_unidad(unidades: TablaUnidades):
    base, unidad_base, canonica = unidades.convertir(None, "KG")
    assert base is None and unidad_base == "kg" and canonica == "kg"


def test_todas_las_bases_declaradas_son_validas(unidades: TablaUnidades):
    for _, _, base, factor in unidades.como_filas():
        assert base in BASES
        assert factor > 0


def test_yaml_invalido_falla_al_cargar(tmp_path):
    mala = tmp_path / "u.yaml"
    mala.write_text(
        "unidades:\n  XX: {canonica: xx, base: parsecs, factor: 1.0}\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="base"):
        TablaUnidades.desde_yaml(mala)

    mala.write_text(
        "unidades:\n  XX: {canonica: xx, base: kg, factor: 0}\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="factor"):
        TablaUnidades.desde_yaml(mala)


def test_clave_sql_coincide_con_python(unidades: TablaUnidades):
    """La normalizacion de Python y la de SQL tienen que dar lo mismo.

    Si se desincronizan, codigos como 'GR.' dejan de matchear contra 'GR' y se
    pierde silenciosamente el 15% de las filas (paso de verdad).
    """
    import duckdb

    from precios.normalize.unidades import SQL_CLAVE_UNIDAD

    con = duckdb.connect()
    casos = ["GR.", "ml.", " UN. ", "Kg", "CC.", "uni.", "LT .", "", "EA"]
    for crudo in casos:
        esperado = TablaUnidades.clave(crudo)
        sql = SQL_CLAVE_UNIDAD.format(col="?")
        obtenido = con.execute(f"SELECT {sql}", [crudo]).fetchone()[0]
        assert obtenido == esperado, f"{crudo!r}: SQL={obtenido!r} Python={esperado!r}"


def test_codigos_reales_no_mapeados_quedan_cubiertos(unidades: TablaUnidades):
    """Los codigos que aparecieron al correr el ETL sobre el dia real 2026-08-01."""
    observados = ["GR.", "ML.", "CU", "UN.", "CMQ", "CC.", "LTR", "UNI.", "LT.",
                  "KGR", "MTR", "UNIDA", "MT.", "CJ", "MI", "PAR", "DM3", "PC",
                  "PIE", "DMQ", "CMT", "HJS", "METROS", "UD", "PCK", "CL"]
    faltantes = [c for c in observados if c not in unidades]
    assert not faltantes, f"unidades sin mapear: {faltantes}"
