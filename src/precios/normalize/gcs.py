"""Puente con GCS para la Etapa 1.

El nucleo del ETL (`etl.procesar_zip`) trabaja siempre con archivos locales, lo
que lo hace testeable sin red. Este modulo es la unica parte que habla con el
bucket: baja el ZIP del dia y sube las particiones Parquet resultantes.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from google.api_core import exceptions as gexc
from google.cloud import storage

from ..config import Config
from ..logs import log


class ClienteBucket:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._cliente = storage.Client()
        self._bucket = self._cliente.bucket(cfg.bucket)

    # -- lectura ------------------------------------------------------------ #

    def bajar(self, ruta: str, destino: Path) -> bool:
        blob = self._bucket.blob(ruta)
        try:
            destino.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(str(destino))
        except gexc.NotFound:
            return False
        return True

    def leer_texto(self, ruta: str) -> str | None:
        try:
            return self._bucket.blob(ruta).download_as_bytes().decode("utf-8")
        except gexc.NotFound:
            return None

    def listar(self, prefijo: str) -> list[str]:
        return [b.name for b in self._cliente.list_blobs(self._bucket, prefix=prefijo)]

    # -- escritura ---------------------------------------------------------- #

    def borrar_prefijo(self, prefijo: str) -> int:
        """Vacia una particion. Necesario para que reprocesar sea idempotente."""
        borrados = 0
        for blob in list(self._cliente.list_blobs(self._bucket, prefix=prefijo)):
            blob.delete()
            borrados += 1
        if borrados:
            log(logging.INFO, "particion vaciada", prefijo=prefijo, objetos=borrados)
        return borrados

    def subir_directorio(self, origen: Path, prefijo: str) -> int:
        subidos = 0
        for archivo in sorted(origen.glob("*.parquet")):
            blob = self._bucket.blob(f"{prefijo}/{archivo.name}")
            blob.content_type = "application/vnd.apache.parquet"
            blob.chunk_size = 8 * 1024 * 1024
            blob.upload_from_filename(str(archivo))
            subidos += 1
        return subidos


def zips_disponibles(cliente: ClienteBucket, cfg: Config) -> dict[date, str]:
    """Fechas archivadas en raw/, leidas del manifest.

    Cae a listar el bucket si el manifest no existe todavia.
    """
    import json

    encontrados: dict[date, str] = {}
    texto = cliente.leer_texto(cfg.manifest_path)
    if texto:
        for linea in texto.splitlines():
            linea = linea.strip()
            if not linea:
                continue
            try:
                reg = json.loads(linea)
            except json.JSONDecodeError:
                continue
            # Los registros de conflicto apuntan a _conflictos/: no son la ruta canonica.
            if reg.get("conflicto_con"):
                continue
            f = reg.get("fecha_datos")
            ruta = (reg.get("gcs_path") or "").split(f"{cfg.bucket}/", 1)[-1]
            if f and ruta:
                encontrados[date.fromisoformat(f)] = ruta
    if encontrados:
        return encontrados

    log(logging.WARNING, "manifest vacio o ausente; se lista el bucket")
    for nombre in cliente.listar(f"{cfg.prefijo_raw}/fecha_datos="):
        if not nombre.endswith(".zip") or "/_conflictos/" in nombre:
            continue
        try:
            f = date.fromisoformat(nombre.split("fecha_datos=")[1].split("/")[0])
        except (IndexError, ValueError):
            continue
        encontrados[f] = nombre
    return encontrados
