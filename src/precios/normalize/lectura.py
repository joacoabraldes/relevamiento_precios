"""Apertura de los ZIP de SEPA y extraccion de los CSV de cada comercio.

Estructura real del archivo diario, verificada sobre datos del bucket:

    sepa_sabado.zip
      2026-08-01/
        sepa_1_comercio-sepa-13_2026-08-01_09-05-10.zip
          comercio.csv | sucursales.csv | productos.csv
        ...

Rarezas del formato que este modulo absorbe:
  - separador pipe `|`, no coma
  - BOM inconsistente entre comercios -> se lee con utf-8-sig
  - fin de linea mezclado (\\r\\n en unos, \\n en otros)
  - ultima linea "Ultima actualizacion: ..." (con y sin tilde) que no es dato
  - algun comercio publica un ZIP interno vacio o corrupto
"""

from __future__ import annotations

import dataclasses
import io
import logging
import re
import zipfile
from pathlib import Path
from typing import Iterator

from ..logs import log

ARCHIVOS_ESPERADOS = ("comercio.csv", "sucursales.csv", "productos.csv")

# `sepa_1_comercio-sepa-13_2026-08-01_09-05-10.zip` -> "13"
RE_COMERCIO = re.compile(r"comercio-sepa-(\d+)", re.IGNORECASE)


@dataclasses.dataclass
class PaqueteComercio:
    """Los 3 CSV de un comercio, ya extraidos a disco."""

    etiqueta: str
    id_comercio_archivo: str | None
    dir_csv: Path

    def path(self, archivo: str) -> Path:
        return self.dir_csv / archivo

    def tiene(self, archivo: str) -> bool:
        return self.path(archivo).is_file()


@dataclasses.dataclass
class PaqueteInvalido:
    etiqueta: str
    motivo: str


def _dir_fecha(nombres: list[str]) -> set[str]:
    return {n.strip("/").split("/")[0] for n in nombres if n.strip("/")}


def abrir_zip_diario(path_zip: Path) -> zipfile.ZipFile:
    try:
        return zipfile.ZipFile(path_zip)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"{path_zip} no es un ZIP valido: {exc}") from exc


def iterar_comercios(
    path_zip: Path, destino: Path
) -> Iterator[PaqueteComercio | PaqueteInvalido]:
    """Extrae los CSV de cada comercio, uno por vez.

    Un comercio roto no interrumpe al resto: se emite un PaqueteInvalido y se
    sigue. Perder un comercio de un dia es recuperable; abortar la corrida y
    perder el dia entero, no.
    """
    with abrir_zip_diario(path_zip) as z:
        internos = [n for n in z.namelist() if n.lower().endswith(".zip")]
        if not internos:
            raise ValueError(
                f"{path_zip} no contiene ZIP de comercios; raices={sorted(_dir_fecha(z.namelist()))}"
            )
        log(logging.INFO, "zip diario abierto", zip=str(path_zip), comercios=len(internos))

        for nombre in sorted(internos):
            etiqueta = Path(nombre).name
            m = RE_COMERCIO.search(etiqueta)
            id_archivo = m.group(1) if m else None
            try:
                crudo = z.read(nombre)
            except Exception as exc:  # noqa: BLE001
                yield PaqueteInvalido(etiqueta, f"no se pudo leer del ZIP: {exc}")
                continue

            if not crudo:
                yield PaqueteInvalido(etiqueta, "ZIP interno vacio (0 bytes)")
                continue

            dir_comercio = destino / etiqueta.replace(".zip", "")
            dir_comercio.mkdir(parents=True, exist_ok=True)
            try:
                with zipfile.ZipFile(io.BytesIO(crudo)) as iz:
                    presentes = {Path(n).name.lower(): n for n in iz.namelist()}
                    faltantes = [a for a in ARCHIVOS_ESPERADOS if a not in presentes]
                    if "productos.csv" in faltantes:
                        yield PaqueteInvalido(
                            etiqueta, f"sin productos.csv (tiene {sorted(presentes)})"
                        )
                        continue
                    for archivo in ARCHIVOS_ESPERADOS:
                        if archivo in presentes:
                            with iz.open(presentes[archivo]) as fh, (
                                dir_comercio / archivo
                            ).open("wb") as out:
                                while chunk := fh.read(1024 * 1024):
                                    out.write(chunk)
                    if faltantes:
                        log(
                            logging.WARNING,
                            "comercio sin todos los CSV",
                            comercio=etiqueta,
                            faltantes=faltantes,
                        )
            except zipfile.BadZipFile as exc:
                yield PaqueteInvalido(etiqueta, f"ZIP interno corrupto: {exc}")
                continue
            except Exception as exc:  # noqa: BLE001
                yield PaqueteInvalido(etiqueta, f"error extrayendo: {exc}")
                continue

            yield PaqueteComercio(etiqueta, id_archivo, dir_comercio)


def fecha_interna_del_zip(path_zip: Path) -> str | None:
    """Fecha declarada por el directorio raiz del ZIP (`2026-08-01/`)."""
    with abrir_zip_diario(path_zip) as z:
        raices = _dir_fecha(z.namelist())
    fechas = {r for r in raices if re.fullmatch(r"\d{4}-\d{2}-\d{2}", r)}
    return next(iter(fechas)) if len(fechas) == 1 else None
