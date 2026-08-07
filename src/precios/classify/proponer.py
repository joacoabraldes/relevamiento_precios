"""Propone asignaciones producto -> categoria a partir de los datos procesados.

El proceso es semiautomatico a proposito: esto **propone**, no decide. La salida
es un CSV que se revisa a mano y se versiona. Nada clasifica en runtime.

Un producto se identifica por `id_producto` (el EAN). Como el mismo EAN puede
venir descripto distinto en cada cadena ("LECHE ENT LARGA VIDA" en una,
"LECHE ENTERA SACHET 1L" en otra), se clasifica **cada variante por separado** y
despues se comparan: si todas coinciden, la asignacion es firme; si no, el
producto va a revision en vez de quedarse con la primera que matcheo.
"""

from __future__ import annotations

import csv
import dataclasses
import logging
from collections import defaultdict
from pathlib import Path

import duckdb

from ..comercios import ListaComercios
from ..logs import log
from .taxonomia import Taxonomia

# Estados posibles de una propuesta.
ASIGNADA = "asignada"
AMBIGUA_ENTRE_CATEGORIAS = "ambigua_entre_categorias"
AMBIGUA_ENTRE_VARIANTES = "ambigua_entre_variantes"


@dataclasses.dataclass
class Variante:
    """Una forma en que un comercio describe un producto."""

    descripcion: str
    marca: str | None
    cantidad_base: float | None
    unidad_base: str | None
    n_obs: int


@dataclasses.dataclass
class Propuesta:
    id_producto: str
    categoria: str | None
    estado: str
    descripcion: str
    marca: str | None
    cantidad_base: float | None
    unidad_base: str | None
    n_obs: int
    n_comercios: int
    categorias_candidatas: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ResumenClasificacion:
    productos_totales: int = 0
    obs_totales: int = 0
    productos_asignados: int = 0
    obs_asignadas: int = 0
    ambiguos_entre_categorias: int = 0
    ambiguos_entre_variantes: int = 0
    por_categoria: dict[str, tuple[int, int]] = dataclasses.field(default_factory=dict)

    @property
    def cobertura_productos(self) -> float:
        return 100.0 * self.productos_asignados / self.productos_totales if self.productos_totales else 0.0

    @property
    def cobertura_obs(self) -> float:
        return 100.0 * self.obs_asignadas / self.obs_totales if self.obs_totales else 0.0


SQL_CATALOGO = """
SELECT
    id_producto,
    descripcion,
    any_value(marca)                AS marca,
    cantidad_base,
    unidad_base,
    count(*)                        AS n_obs,
    count(DISTINCT id_comercio)     AS n_comercios
FROM read_parquet('{glob}')
WHERE es_ean AND id_producto IS NOT NULL AND descripcion IS NOT NULL
  AND {filtro_comercios}
GROUP BY id_producto, descripcion, cantidad_base, unidad_base
"""


def construir_catalogo(
    glob_parquet: str,
    memoria: str = "4GB",
    comercios: ListaComercios | None = None,
) -> dict[str, list[Variante]]:
    """Agrupa las observaciones en un catalogo de productos y sus variantes.

    Solo entran los productos con EAN real: un codigo interno de comercio
    (`es_ean = false`, ~5% de los casos) solo es unico dentro de esa cadena, asi
    que no se puede usar como clave global de producto.

    Los informantes que no son supermercados (farmacias, tiendas de estacion de
    servicio) quedan afuera segun config/comercios.yaml.
    """
    filtro = comercios.filtro_sql() if comercios else "TRUE"
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memoria}'")
    filas = con.execute(
        SQL_CATALOGO.format(glob=glob_parquet, filtro_comercios=filtro)
    ).fetchall()
    con.close()

    catalogo: dict[str, list[Variante]] = defaultdict(list)
    for id_prod, desc, marca, cant, unidad, n_obs, _n_com in filas:
        catalogo[id_prod].append(
            Variante(desc, marca, cant, unidad, n_obs)
        )
    return catalogo


def proponer(
    catalogo: dict[str, list[Variante]], taxonomia: Taxonomia
) -> tuple[list[Propuesta], ResumenClasificacion]:
    propuestas: list[Propuesta] = []
    resumen = ResumenClasificacion()

    for id_prod, variantes in catalogo.items():
        n_obs = sum(v.n_obs for v in variantes)
        resumen.productos_totales += 1
        resumen.obs_totales += n_obs

        descripciones = [v.descripcion for v in variantes]
        presentaciones = [(v.cantidad_base, v.unidad_base) for v in variantes]

        candidatas = taxonomia.clasificar_producto(descripciones, presentaciones)
        if not candidatas:
            continue

        if len(candidatas) > 1:
            categoria = None
            estado = AMBIGUA_ENTRE_CATEGORIAS
            resumen.ambiguos_entre_categorias += 1
            principal = max(variantes, key=lambda v: v.n_obs)
        else:
            categoria = candidatas[0]
            estado = ASIGNADA
            resumen.productos_asignados += 1
            resumen.obs_asignadas += n_obs
            n_p, n_o = resumen.por_categoria.get(categoria, (0, 0))
            resumen.por_categoria[categoria] = (n_p + 1, n_o + n_obs)
            # El representante tiene que ser una variante que efectivamente
            # matcheo la regla; si no, el CSV de revision muestra descripciones
            # que no explican por que el producto entro en la categoria.
            regla = taxonomia.por_codigo(categoria)
            coincidentes = [
                v for v in variantes
                if regla is not None and regla.coincide_texto(v.descripcion)
            ] or variantes
            principal = max(coincidentes, key=lambda v: v.n_obs)

        propuestas.append(
            Propuesta(
                id_producto=id_prod,
                categoria=categoria,
                estado=estado,
                descripcion=principal.descripcion,
                marca=principal.marca,
                cantidad_base=principal.cantidad_base,
                unidad_base=principal.unidad_base,
                n_obs=n_obs,
                n_comercios=len(variantes),
                categorias_candidatas=sorted(candidatas),
            )
        )

    propuestas.sort(key=lambda p: (-p.n_obs, p.id_producto))
    return propuestas, resumen


CAMPOS_MAPEO = (
    "id_producto",
    "categoria",
    "descripcion",
    "marca",
    "cantidad_base",
    "unidad_base",
    "n_observaciones",
    "origen",
    "revisado",
)


def escribir_mapeo(propuestas: list[Propuesta], destino: Path) -> int:
    """Escribe el mapeo versionable, solo con las asignaciones firmes.

    `revisado=no` marca que todavia nadie lo miro. La idea es que se vaya
    pasando a `si` a medida que se revisa, y que un cambio de categoria quede
    registrado en el diff.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    firmes = [p for p in propuestas if p.estado == ASIGNADA]
    firmes.sort(key=lambda p: (p.categoria or "", -p.n_obs))
    with destino.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(CAMPOS_MAPEO)
        for p in firmes:
            w.writerow([
                p.id_producto, p.categoria, p.descripcion, p.marca or "",
                "" if p.cantidad_base is None else f"{p.cantidad_base:g}",
                p.unidad_base or "", p.n_obs, "auto", "no",
            ])
    return len(firmes)


def escribir_revision(propuestas: list[Propuesta], destino: Path) -> int:
    """Productos que matchearon mas de una categoria: los tiene que resolver una persona."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    dudosos = [p for p in propuestas if p.estado != ASIGNADA]
    dudosos.sort(key=lambda p: -p.n_obs)
    with destino.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["id_producto", "estado", "categorias_candidatas", "descripcion",
                    "cantidad_base", "unidad_base", "n_observaciones"])
        for p in dudosos:
            w.writerow([
                p.id_producto, p.estado, "|".join(p.categorias_candidatas),
                p.descripcion,
                "" if p.cantidad_base is None else f"{p.cantidad_base:g}",
                p.unidad_base or "", p.n_obs,
            ])
    return len(dudosos)


def escribir_sin_clasificar(
    catalogo: dict[str, list[Variante]],
    propuestas: list[Propuesta],
    destino: Path,
    limite: int = 500,
) -> int:
    """Los productos de mas volumen que ninguna regla toco.

    No es un error: el piloto cubre 15 categorias a proposito. Sirve para decidir
    cual conviene agregar despues, ordenado por cuanto pesa en los datos.
    """
    vistos = {p.id_producto for p in propuestas}
    faltantes = []
    for id_prod, variantes in catalogo.items():
        if id_prod in vistos:
            continue
        principal = max(variantes, key=lambda v: v.n_obs)
        faltantes.append((sum(v.n_obs for v in variantes), id_prod, principal))
    faltantes.sort(reverse=True, key=lambda t: t[0])

    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["id_producto", "descripcion", "marca", "cantidad_base",
                    "unidad_base", "n_observaciones"])
        for n_obs, id_prod, v in faltantes[:limite]:
            w.writerow([
                id_prod, v.descripcion, v.marca or "",
                "" if v.cantidad_base is None else f"{v.cantidad_base:g}",
                v.unidad_base or "", n_obs,
            ])
    return len(faltantes)


def log_resumen(resumen: ResumenClasificacion, taxonomia: Taxonomia) -> None:
    log(
        logging.INFO,
        "clasificacion propuesta",
        productos=resumen.productos_totales,
        asignados=resumen.productos_asignados,
        cobertura_productos=f"{resumen.cobertura_productos:.2f}%",
        cobertura_observaciones=f"{resumen.cobertura_obs:.2f}%",
        ambiguos=resumen.ambiguos_entre_categorias + resumen.ambiguos_entre_variantes,
    )
