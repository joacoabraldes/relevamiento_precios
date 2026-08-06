"""Normalizacion de unidades de medida.

Los comercios informan la unidad con codigos libres (`UNI`, `EA`, `KGM`, `LT`,
`CC`...) en vez del conjunto chico que define el Anexo II. Este modulo lleva
todo a una unidad canonica y a una **unidad base** con su factor, para que la
Etapa 2 pueda comparar "1 L" contra "1000 ML" y darse cuenta de que son lo mismo.
"""

from __future__ import annotations

import dataclasses
import functools
from pathlib import Path

import yaml

# Unidades base admitidas. Todo lo demas se convierte a una de estas.
BASES = frozenset({"kg", "l", "un", "m", "m2"})

# Equivalente SQL de TablaUnidades.clave(), para normalizar del lado de DuckDB
# antes de joinear contra dim_unidades. `{col}` se reemplaza por la columna.
SQL_CLAVE_UNIDAD = "replace(replace(upper(trim({col})), '.', ''), ' ', '')"


@dataclasses.dataclass(frozen=True)
class Unidad:
    canonica: str
    base: str
    factor: float


class TablaUnidades:
    """Mapeo codigo informado -> unidad canonica + base + factor."""

    def __init__(self, mapa: dict[str, Unidad]) -> None:
        self._mapa = mapa

    @classmethod
    def desde_yaml(cls, path: Path) -> "TablaUnidades":
        datos = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        crudas = datos.get("unidades") or {}
        mapa: dict[str, Unidad] = {}
        for codigo, spec in crudas.items():
            base = str(spec["base"]).lower()
            if base not in BASES:
                raise ValueError(
                    f"unidad {codigo!r}: base {base!r} invalida; validas: {sorted(BASES)}"
                )
            factor = float(spec["factor"])
            if factor <= 0:
                raise ValueError(f"unidad {codigo!r}: factor debe ser > 0, es {factor}")
            mapa[cls.clave(codigo)] = Unidad(
                canonica=str(spec["canonica"]).lower(), base=base, factor=factor
            )
        if not mapa:
            raise ValueError(f"{path} no define ninguna unidad")
        return cls(mapa)

    @staticmethod
    def clave(codigo: str | None) -> str:
        """Normaliza el codigo informado para el lookup: 'Kg.' -> 'KG'.

        Si cambia esta regla hay que cambiar tambien SQL_CLAVE_UNIDAD, que es la
        version equivalente del lado de DuckDB. `test_clave_sql_coincide_con_python`
        verifica que las dos no se desincronicen.
        """
        if codigo is None:
            return ""
        return codigo.strip().upper().replace(".", "").replace(" ", "")

    def buscar(self, codigo: str | None) -> Unidad | None:
        return self._mapa.get(self.clave(codigo))

    def convertir(
        self, cantidad: float | None, codigo: str | None
    ) -> tuple[float | None, str | None, str | None]:
        """(cantidad, codigo) -> (cantidad_base, unidad_base, unidad_canonica).

        Devuelve (None, None, None) si la unidad no esta en la tabla: preferimos
        no inventar una conversion antes que ensuciar la base.
        """
        u = self.buscar(codigo)
        if u is None:
            return None, None, None
        if cantidad is None:
            return None, u.base, u.canonica
        return cantidad * u.factor, u.base, u.canonica

    def como_filas(self) -> list[tuple[str, str, str, float]]:
        """Filas (codigo_crudo, canonica, base, factor) para cargar en DuckDB."""
        return [
            (codigo, u.canonica, u.base, u.factor) for codigo, u in self._mapa.items()
        ]

    def __len__(self) -> int:
        return len(self._mapa)

    def __contains__(self, codigo: str) -> bool:
        return self.clave(codigo) in self._mapa


@functools.lru_cache(maxsize=4)
def cargar(path: Path) -> TablaUnidades:
    return TablaUnidades.desde_yaml(path)
