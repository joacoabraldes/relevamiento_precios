"""Maestro de provincias: ISO 3166-2 (`AR-B`) -> nombre (`Buenos Aires`).

El archivo oficial vive en el bucket (`_meta/referencia/maestro-provincias.xlsx`),
lo publica el mismo dataset de SEPA y cubre las 24 jurisdicciones.
"""

from __future__ import annotations

from pathlib import Path

# Cuando `sucursales_provincia` viene vacio no inventamos: se marca explicito.
PROVINCIA_DESCONOCIDA = "DESCONOCIDA"


class MaestroProvincias:
    def __init__(self, mapa: dict[str, str]) -> None:
        self._mapa = mapa

    @classmethod
    def desde_xlsx(cls, path: Path) -> "MaestroProvincias":
        import openpyxl

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        mapa: dict[str, str] = {}
        for ws in wb.worksheets:
            filas = ws.iter_rows(values_only=True)
            encabezado = next(filas, None)
            if not encabezado:
                continue
            # El maestro trae ('sucursales_provincia', 'provincia').
            for fila in filas:
                if not fila or len(fila) < 2:
                    continue
                codigo, nombre = fila[0], fila[1]
                if codigo is None or nombre is None:
                    continue
                mapa[str(codigo).strip().upper()] = str(nombre).strip()
        wb.close()
        if not mapa:
            raise ValueError(f"{path} no contiene ningun par codigo/provincia")
        return cls(mapa)

    def nombre(self, codigo: str | None) -> str:
        if not codigo or not str(codigo).strip():
            return PROVINCIA_DESCONOCIDA
        return self._mapa.get(str(codigo).strip().upper(), PROVINCIA_DESCONOCIDA)

    def como_filas(self) -> list[tuple[str, str]]:
        return sorted(self._mapa.items())

    def __len__(self) -> int:
        return len(self._mapa)

    def __contains__(self, codigo: str) -> bool:
        return str(codigo).strip().upper() in self._mapa
