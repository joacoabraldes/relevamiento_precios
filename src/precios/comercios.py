"""Informantes de SEPA excluidos de la capa de analisis.

SEPA obliga a informar a todo comercio de consumo masivo, no solo a
supermercados: hay farmacias y tiendas de estacion de servicio en el dataset.
Esos informantes tienen muy poco surtido comparable y otra estructura de precios,
asi que aportan ruido en vez de cobertura.

La exclusion vive aca y no en el ETL a proposito: `raw/` y `staged/` siempre
guardan todo. Sacar un comercio de la lista y reprocesar alcanza para revertir,
sin volver a descargar nada.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import yaml


@dataclasses.dataclass(frozen=True)
class ComercioExcluido:
    id_comercio: str
    nombre: str
    motivo: str


class ListaComercios:
    def __init__(self, excluidos: dict[str, ComercioExcluido]) -> None:
        self._excluidos = excluidos

    @classmethod
    def desde_yaml(cls, path: Path) -> "ListaComercios":
        if not path.is_file():
            return cls({})
        datos = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        excluidos = {
            str(id_com): ComercioExcluido(
                id_comercio=str(id_com),
                nombre=(spec or {}).get("nombre", ""),
                motivo=" ".join((spec or {}).get("motivo", "").split()),
            )
            for id_com, spec in (datos.get("excluidos") or {}).items()
        }
        return cls(excluidos)

    @property
    def ids(self) -> list[str]:
        return sorted(self._excluidos)

    def esta_excluido(self, id_comercio: str | None) -> bool:
        return str(id_comercio) in self._excluidos

    def detalle(self, id_comercio: str) -> ComercioExcluido | None:
        return self._excluidos.get(str(id_comercio))

    def filtro_sql(self, columna: str = "id_comercio") -> str:
        """Condicion SQL que deja afuera a los excluidos. `TRUE` si no hay ninguno."""
        if not self._excluidos:
            return "TRUE"
        lista = ", ".join(f"'{i}'" for i in self.ids)
        return f"{columna} NOT IN ({lista})"

    def __len__(self) -> int:
        return len(self._excluidos)
