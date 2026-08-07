"""Taxonomia de categorias y motor de reglas.

La clasificacion es deliberadamente tonta y explicita: reglas de texto sobre la
descripcion mas restricciones de presentacion. Nada de similitud semantica ni
modelos. La razon es que el resultado tiene que ser auditable — si un producto
entra o sale de una categoria, se tiene que poder explicar en una linea y ver en
un diff de git.
"""

from __future__ import annotations

import dataclasses
import re
import unicodedata
from pathlib import Path

import yaml


def normalizar_texto(texto: str | None) -> str:
    """Mayusculas, sin acentos, espacios colapsados.

    Sin acentos porque los comercios son inconsistentes: aparece 'MAÑANITA' y
    'MANANITA', 'AZUCAR' y 'AZÚCAR', a veces en el mismo archivo.
    """
    if not texto:
        return ""
    sin_acentos = "".join(
        c
        for c in unicodedata.normalize("NFKD", texto)
        if not unicodedata.combining(c)
    )
    return re.sub(r"\s+", " ", sin_acentos.upper()).strip()


@dataclasses.dataclass(frozen=True)
class Regla:
    codigo: str
    clase: str
    nombre: str
    patrones: tuple[re.Pattern, ...]
    alguno: tuple[re.Pattern, ...]
    excluye: tuple[re.Pattern, ...]
    unidad_base: str | None
    cantidad_min: float | None
    cantidad_max: float | None

    def coincide_texto(self, descripcion: str) -> bool:
        d = normalizar_texto(descripcion)
        if not all(p.search(d) for p in self.patrones):
            return False
        if self.alguno and not any(p.search(d) for p in self.alguno):
            return False
        if any(p.search(d) for p in self.excluye):
            return False
        return True

    def coincide_presentacion(
        self, cantidad_base: float | None, unidad_base: str | None
    ) -> bool:
        if self.unidad_base is not None and unidad_base != self.unidad_base:
            return False
        if self.cantidad_min is not None or self.cantidad_max is not None:
            # Sin cantidad normalizada no podemos afirmar que la presentacion
            # entra en el rango: preferimos no clasificar antes que adivinar.
            if cantidad_base is None:
                return False
            if self.cantidad_min is not None and cantidad_base < self.cantidad_min:
                return False
            if self.cantidad_max is not None and cantidad_base > self.cantidad_max:
                return False
        return True

    def coincide(
        self, descripcion: str, cantidad_base: float | None, unidad_base: str | None
    ) -> bool:
        return self.coincide_presentacion(
            cantidad_base, unidad_base
        ) and self.coincide_texto(descripcion)

    def excluido_por(self, descripciones: list[str]) -> str | None:
        """Primer termino de exclusion que aparezca en CUALQUIER descripcion.

        Las exclusiones se evaluan sobre todas las variantes del producto, no
        sobre una sola. Un EAN es un producto fisico: si una cadena lo llama
        "ARROZ PARBOIL" y otra "ARROZ LARGO FINO", el producto es parboil y la
        segunda descripcion esta mal. Ver una exclusion una vez es evidencia
        fuerte; no verla en una descripcion truncada no prueba nada.
        """
        for d in descripciones:
            texto = normalizar_texto(d)
            for p in self.excluye:
                if p.search(texto):
                    return p.pattern
        return None

    def coincide_producto(
        self,
        descripciones: list[str],
        presentaciones: list[tuple[float | None, str | None]],
    ) -> bool:
        """Evalua la regla contra todas las variantes de un mismo producto.

        Asimetria deliberada:
          - exclusiones: alcanza con que UNA variante la dispare (estricto)
          - inclusiones: alcanza con que UNA variante cumpla TODOS los patrones
            (permisivo, para rescatar descripciones truncadas como "YOG FIRME")
          - presentacion: alcanza con que UNA variante entre en el rango, porque
            hay cadenas que informan "1 unidad" y pierden el peso real

        Los patrones de inclusion tienen que cumplirse dentro de UNA MISMA
        variante: si no, "ARROZ" de una descripcion y "LARGO FINO" de otra se
        combinarian en un match que ninguna de las dos justifica.
        """
        if self.excluido_por(descripciones):
            return False
        if not any(self.coincide_texto(d) for d in descripciones):
            return False
        return any(self.coincide_presentacion(c, u) for c, u in presentaciones)


@dataclasses.dataclass
class Taxonomia:
    reglas: tuple[Regla, ...]
    clases: dict[str, str]
    grupos: dict[str, str]
    divisiones: dict[str, str]

    @classmethod
    def desde_yaml(cls, path: Path) -> "Taxonomia":
        datos = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

        divisiones: dict[str, str] = {}
        grupos: dict[str, str] = {}
        clases: dict[str, str] = {}
        for cod_div, div in (datos.get("divisiones") or {}).items():
            divisiones[str(cod_div)] = div.get("nombre", "")
            for cod_gr, gr in (div.get("grupos") or {}).items():
                grupos[str(cod_gr)] = gr.get("nombre", "")
                for cod_cl, cl in (gr.get("clases") or {}).items():
                    clases[str(cod_cl)] = (cl or {}).get("nombre", "")

        reglas: list[Regla] = []
        for codigo, spec in (datos.get("categorias") or {}).items():
            clase = str(spec.get("clase", ""))
            if clase not in clases:
                raise ValueError(
                    f"categoria {codigo!r}: clase {clase!r} no existe en la taxonomia"
                )
            patrones = tuple(_compilar(p, codigo) for p in spec.get("patrones") or [])
            if not patrones:
                raise ValueError(f"categoria {codigo!r}: necesita al menos un patron")
            reglas.append(
                Regla(
                    codigo=str(codigo),
                    clase=clase,
                    nombre=spec.get("nombre", codigo),
                    patrones=patrones,
                    alguno=tuple(_compilar(p, codigo) for p in spec.get("alguno") or []),
                    excluye=tuple(
                        _compilar(p, codigo) for p in spec.get("excluye") or []
                    ),
                    unidad_base=spec.get("unidad_base"),
                    cantidad_min=_num(spec.get("cantidad_min")),
                    cantidad_max=_num(spec.get("cantidad_max")),
                )
            )
        if not reglas:
            raise ValueError(f"{path} no define ninguna categoria")

        for r in reglas:
            if (
                r.cantidad_min is not None
                and r.cantidad_max is not None
                and r.cantidad_min > r.cantidad_max
            ):
                raise ValueError(
                    f"categoria {r.codigo!r}: cantidad_min > cantidad_max"
                )
        return cls(tuple(reglas), clases, grupos, divisiones)

    def clasificar(
        self, descripcion: str, cantidad_base: float | None, unidad_base: str | None
    ) -> list[str]:
        """Categorias que coinciden con una unica descripcion + presentacion."""
        return [
            r.codigo
            for r in self.reglas
            if r.coincide(descripcion, cantidad_base, unidad_base)
        ]

    def clasificar_producto(
        self,
        descripciones: list[str],
        presentaciones: list[tuple[float | None, str | None]],
    ) -> list[str]:
        """Devuelve TODAS las categorias que coinciden con el producto.

        Devolver la lista completa (en vez de la primera) es a proposito: si un
        producto entra en dos categorias, la taxonomia tiene un solapamiento que
        hay que arreglar, y queremos verlo en vez de que quede tapado por el
        orden de las reglas.
        """
        return [
            r.codigo
            for r in self.reglas
            if r.coincide_producto(descripciones, presentaciones)
        ]

    def por_codigo(self, codigo: str) -> Regla | None:
        return next((r for r in self.reglas if r.codigo == codigo), None)

    def __len__(self) -> int:
        return len(self.reglas)


def _compilar(patron: str, codigo: str) -> re.Pattern:
    try:
        return re.compile(patron)
    except re.error as exc:
        raise ValueError(f"categoria {codigo!r}: regex invalido {patron!r}: {exc}") from exc


def _num(valor) -> float | None:
    return None if valor is None else float(valor)
