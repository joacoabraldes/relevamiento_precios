#!/usr/bin/env python3
"""Descarga diaria de los archivos SEPA (Precios Claros) y archivado crudo en GCS.

El dataset publica exactamente 7 recursos, uno por dia de la semana, y cada uno
se sobrescribe cada 7 dias. No hay historico: lo que no se archiva dentro de esa
ventana se pierde para siempre. Por eso este script baja los 7 todos los dias y
falla con exit code distinto de cero ante cualquier problema.

Alcance: solo colectar y archivar. No parsea CSVs ni calcula nada.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import random
import re
import shutil
import sys
import tempfile
import time
import unicodedata
import zipfile
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import httpx
from google.api_core import exceptions as gexc
from google.cloud import storage

# --------------------------------------------------------------------------- #
# Constantes del dataset
# --------------------------------------------------------------------------- #

DATASET_ID = "6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5"
BASE_URL = "https://datos.produccion.gob.ar/dataset/{dataset}/resource/{resource}/download/{filename}"
CKAN_PACKAGE_URL = (
    "https://datos.produccion.gob.ar/api/3/action/package_show?id=" + DATASET_ID
)

TZ_AR = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclasses.dataclass(frozen=True)
class Recurso:
    """Un recurso del dataset: el ZIP de un dia de la semana."""

    dia: str  # nombre sin acentos, tal como va en el path de GCS
    weekday: int  # 0 = lunes ... 6 = domingo (compatible con date.weekday())
    resource_id: str
    filename: str

    @property
    def url(self) -> str:
        return BASE_URL.format(
            dataset=DATASET_ID, resource=self.resource_id, filename=self.filename
        )


# Verificados contra la API CKAN de datos.produccion.gob.ar.
RECURSOS: tuple[Recurso, ...] = (
    Recurso("lunes", 0, "0a9069a9-06e8-4f98-874d-da5578693290", "sepa_lunes.zip"),
    Recurso("martes", 1, "9dc06241-cc83-44f4-8e25-c9b1636b8bc8", "sepa_martes.zip"),
    Recurso("miercoles", 2, "1e92cd42-4f94-4071-a165-62c4cb2ce23c", "sepa_miercoles.zip"),
    Recurso("jueves", 3, "d076720f-a7f0-4af8-b1d6-1b99d5a90c14", "sepa_jueves.zip"),
    Recurso("viernes", 4, "91bc072a-4726-44a1-85ec-4a8467aad27e", "sepa_viernes.zip"),
    Recurso("sabado", 5, "b3c3da5d-213d-41e7-8d74-f23fda0a3c30", "sepa_sabado.zip"),
    Recurso("domingo", 6, "f8e75128-515a-436e-bf8d-5c63a62f2005", "sepa_domingo.zip"),
)

RECURSOS_POR_DIA = {r.dia: r for r in RECURSOS}


@dataclasses.dataclass(frozen=True)
class Referencia:
    """Archivo de referencia: se baja una vez y se versiona en el bucket."""

    resource_id: str
    filename_origen: str
    nombre_destino: str

    @property
    def url(self) -> str:
        return BASE_URL.format(
            dataset=DATASET_ID,
            resource=self.resource_id,
            filename=self.filename_origen,
        )


REFERENCIAS: tuple[Referencia, ...] = (
    Referencia(
        "ace44eb9-c995-463f-bf8a-6f529d196a27",
        "anexo_6201340_2.pdf",
        "anexo_678_2020.pdf",
    ),
    Referencia(
        "e12edfc0-b0bc-4208-879a-b31b9573324b",
        "maestro-provincias.xlsx",
        "maestro-provincias.xlsx",
    ),
)

# Fecha embebida en nombres tipo `sepa_1_comercio-sepa-13_2026-08-01_09-05-10.zip`
RE_FECHA_EN_NOMBRE = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
RE_FECHA_COMPLETA = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Metodos de determinacion de fecha_datos, de mas a menos confiable.
METODO_DIR_ZIP = "zip_dir_nivel1"
METODO_NOMBRES_ZIP = "zip_nombres_internos"
METODO_LAST_MODIFIED = "http_last_modified"

USER_AGENT = "sepa-downloader/1.0 (+archivado de datos abiertos SEPA)"

LOG = logging.getLogger("sepa")


# --------------------------------------------------------------------------- #
# Logging estructurado
# --------------------------------------------------------------------------- #


class FormatoJson(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "nivel": record.levelname,
            "msg": record.getMessage(),
        }
        campos = getattr(record, "campos", None)
        if campos:
            payload.update(campos)
        if record.exc_info:
            payload["excepcion"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class FormatoTexto(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        campos = getattr(record, "campos", None)
        extra = ""
        if campos:
            extra = " " + " ".join(f"{k}={v}" for k, v in campos.items())
        ts = datetime.now(timezone.utc).strftime("%H:%M:%SZ")
        base = f"{ts} {record.levelname:<7} {record.getMessage()}{extra}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def log(nivel: int, msg: str, **campos: Any) -> None:
    LOG.log(nivel, msg, extra={"campos": campos})


def configurar_logging(nivel: str, formato: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(FormatoTexto() if formato == "texto" else FormatoJson())
    LOG.handlers[:] = [handler]
    LOG.setLevel(getattr(logging, nivel.upper(), logging.INFO))
    LOG.propagate = False
    # Las librerias cliente son ruidosas en DEBUG; las dejamos en WARNING.
    for ruidoso in ("httpx", "httpcore", "google", "urllib3"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #


def _env_bool(nombre: str, default: bool = False) -> bool:
    valor = os.environ.get(nombre)
    if valor is None:
        return default
    return valor.strip().lower() in {"1", "true", "t", "yes", "y", "si", "sí"}


def _env_int(nombre: str, default: int) -> int:
    try:
        return int(os.environ.get(nombre, default))
    except ValueError:
        return default


def _env_float(nombre: str, default: float) -> float:
    try:
        return float(os.environ.get(nombre, default))
    except ValueError:
        return default


@dataclasses.dataclass
class Config:
    bucket: str
    prefijo_raw: str
    prefijo_meta: str
    manifest_path: str
    estado_path: str
    prefijo_referencia: str
    tmpdir: str | None
    timeout_conexion: float
    timeout_lectura: float
    max_intentos: int
    backoff_base: float
    backoff_max: float
    fecha_estricta: bool
    log_nivel: str
    log_formato: str

    @classmethod
    def desde_entorno(cls, args: argparse.Namespace) -> "Config":
        prefijo_meta = os.environ.get("SEPA_PREFIJO_META", "_meta").strip("/")
        bucket = args.bucket or os.environ.get("SEPA_BUCKET", "outlier-archivos-precios")
        return cls(
            bucket=bucket,
            prefijo_raw=os.environ.get(
                "SEPA_PREFIJO_RAW", "raw/sepa/minorista"
            ).strip("/"),
            prefijo_meta=prefijo_meta,
            manifest_path=os.environ.get(
                "SEPA_MANIFEST_PATH", f"{prefijo_meta}/manifest.jsonl"
            ).strip("/"),
            estado_path=os.environ.get(
                "SEPA_ESTADO_PATH", f"{prefijo_meta}/estado.json"
            ).strip("/"),
            prefijo_referencia=os.environ.get(
                "SEPA_PREFIJO_REFERENCIA", f"{prefijo_meta}/referencia"
            ).strip("/"),
            tmpdir=os.environ.get("SEPA_TMPDIR") or None,
            timeout_conexion=_env_float("SEPA_TIMEOUT_CONEXION", 30.0),
            timeout_lectura=_env_float("SEPA_TIMEOUT_LECTURA", 1800.0),
            max_intentos=_env_int("SEPA_MAX_INTENTOS", 5),
            backoff_base=_env_float("SEPA_BACKOFF_BASE", 4.0),
            backoff_max=_env_float("SEPA_BACKOFF_MAX", 300.0),
            fecha_estricta=_env_bool("SEPA_FECHA_ESTRICTA", False),
            log_nivel=os.environ.get("SEPA_LOG_NIVEL", "INFO"),
            log_formato=os.environ.get("SEPA_LOG_FORMATO", "json"),
        )


# --------------------------------------------------------------------------- #
# HTTP con reintentos
# --------------------------------------------------------------------------- #


class ErrorTransitorio(Exception):
    """Falla que amerita reintento (red, timeout, 5xx, 429)."""


class ErrorPermanente(Exception):
    """Falla que no se arregla reintentando (404, ZIP corrupto, etc.)."""


def _es_status_transitorio(status: int) -> bool:
    return status == 429 or 500 <= status < 600


def _dormir_backoff(intento: int, cfg: Config) -> None:
    """Backoff exponencial con jitter completo."""
    tope = min(cfg.backoff_base * (2 ** (intento - 1)), cfg.backoff_max)
    espera = random.uniform(0.0, tope)
    log(logging.INFO, "esperando antes de reintentar", intento=intento, segundos=round(espera, 1))
    time.sleep(espera)


def con_reintentos(cfg: Config, descripcion: str, fn, **campos: Any):
    """Ejecuta `fn` reintentando ante ErrorTransitorio con backoff exponencial."""
    ultimo: Exception | None = None
    for intento in range(1, cfg.max_intentos + 1):
        try:
            return fn()
        except ErrorPermanente:
            raise
        except (httpx.TransportError, httpx.TimeoutException, ErrorTransitorio) as exc:
            ultimo = exc
            log(
                logging.WARNING,
                f"fallo transitorio en {descripcion}",
                intento=intento,
                max_intentos=cfg.max_intentos,
                error=f"{type(exc).__name__}: {exc}",
                **campos,
            )
            if intento < cfg.max_intentos:
                _dormir_backoff(intento, cfg)
    raise ErrorPermanente(
        f"{descripcion}: agotados {cfg.max_intentos} intentos; ultimo error: {ultimo}"
    ) from ultimo


@dataclasses.dataclass
class CabecerasRemotas:
    etag: str | None
    last_modified: str | None
    last_modified_dt: datetime | None
    bytes_esperados: int | None


def _total_desde_content_range(valor: str | None) -> int | None:
    # El servidor devuelve `Content-Range: bytes 0-235374439/235374440` incluso
    # en HEAD, y en cambio a veces omite Content-Length.
    if not valor:
        return None
    m = re.search(r"/(\d+)\s*$", valor)
    return int(m.group(1)) if m else None


def _parsear_last_modified(valor: str | None) -> datetime | None:
    if not valor:
        return None
    try:
        dt = parsedate_to_datetime(valor)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def head_recurso(cliente: httpx.Client, cfg: Config, url: str, **campos: Any) -> CabecerasRemotas:
    def _hacer() -> CabecerasRemotas:
        resp = cliente.head(url)
        if _es_status_transitorio(resp.status_code):
            raise ErrorTransitorio(f"HEAD devolvio {resp.status_code}")
        if resp.status_code >= 400:
            raise ErrorPermanente(f"HEAD devolvio {resp.status_code} para {url}")
        largo = resp.headers.get("content-length")
        esperados = int(largo) if largo and largo.isdigit() else None
        if esperados is None:
            esperados = _total_desde_content_range(resp.headers.get("content-range"))
        lm = resp.headers.get("last-modified")
        return CabecerasRemotas(
            etag=resp.headers.get("etag"),
            last_modified=lm,
            last_modified_dt=_parsear_last_modified(lm),
            bytes_esperados=esperados,
        )

    return con_reintentos(cfg, "HEAD", _hacer, **campos)


def descargar_a_archivo(
    cliente: httpx.Client,
    cfg: Config,
    url: str,
    destino: Path,
    bytes_esperados: int | None,
    **campos: Any,
) -> tuple[str, int, CabecerasRemotas]:
    """Baja `url` a `destino` en streaming. Devuelve (sha256, bytes, cabeceras)."""

    def _hacer() -> tuple[str, int, CabecerasRemotas]:
        hasher = hashlib.sha256()
        escritos = 0
        comenzado = time.monotonic()
        with cliente.stream("GET", url) as resp:
            if _es_status_transitorio(resp.status_code):
                raise ErrorTransitorio(f"GET devolvio {resp.status_code}")
            if resp.status_code >= 400:
                raise ErrorPermanente(f"GET devolvio {resp.status_code} para {url}")

            largo = resp.headers.get("content-length")
            esperados_resp = int(largo) if largo and largo.isdigit() else None
            if esperados_resp is None:
                esperados_resp = _total_desde_content_range(
                    resp.headers.get("content-range")
                )
            lm = resp.headers.get("last-modified")
            cabeceras = CabecerasRemotas(
                etag=resp.headers.get("etag"),
                last_modified=lm,
                last_modified_dt=_parsear_last_modified(lm),
                bytes_esperados=esperados_resp,
            )

            with destino.open("wb") as fh:
                for chunk in resp.iter_bytes(1024 * 1024):
                    fh.write(chunk)
                    hasher.update(chunk)
                    escritos += len(chunk)

        objetivo = esperados_resp or bytes_esperados
        if objetivo is not None and escritos != objetivo:
            # Descarga truncada: reintentable.
            raise ErrorTransitorio(
                f"descarga incompleta: {escritos} bytes de {objetivo} esperados"
            )

        segundos = max(time.monotonic() - comenzado, 0.001)
        log(
            logging.INFO,
            "descarga completa",
            bytes=escritos,
            segundos=round(segundos, 1),
            mb_por_seg=round(escritos / segundos / 1e6, 2),
            **campos,
        )
        return hasher.hexdigest(), escritos, cabeceras

    return con_reintentos(cfg, "GET", _hacer, **campos)


# --------------------------------------------------------------------------- #
# Inspeccion del ZIP y determinacion de fecha_datos
# --------------------------------------------------------------------------- #


@dataclasses.dataclass
class FechaDetectada:
    fecha: date
    metodo: str
    listado: list[str]
    advertencias: list[str]


def _a_fecha(texto: str) -> date | None:
    """`2026-08-01` -> date. None si no es una fecha calendaria valida."""
    if not RE_FECHA_COMPLETA.match(texto):
        return None
    try:
        return date.fromisoformat(texto)
    except ValueError:
        return None


def validar_zip(ruta: Path) -> list[str]:
    """Verifica integridad del ZIP y devuelve el listado de entradas."""
    try:
        with zipfile.ZipFile(ruta) as z:
            nombres = z.namelist()
            if not nombres:
                raise ErrorPermanente("el ZIP no tiene entradas")
            corrupta = z.testzip()
            if corrupta is not None:
                raise ErrorPermanente(f"CRC invalido en la entrada {corrupta!r}")
            return nombres
    except zipfile.BadZipFile as exc:
        raise ErrorPermanente(f"ZIP invalido o corrupto: {exc}") from exc


def _fecha_por_last_modified(recurso: Recurso, lm: datetime | None) -> date | None:
    """Retrocede desde el Last-Modified hasta el dia de semana del recurso.

    El dataset publica cada recurso el mismo dia que corresponden los precios,
    alrededor de las 16:18 UTC (13:18 ART), asi que interpretamos la fecha en
    hora argentina y volvemos atras hasta el weekday del recurso.
    """
    if lm is None:
        return None
    fecha = lm.astimezone(TZ_AR).date()
    delta = (fecha.weekday() - recurso.weekday) % 7
    return fecha - timedelta(days=delta)


def determinar_fecha_datos(
    recurso: Recurso, nombres: Sequence[str], lm: datetime | None
) -> FechaDetectada:
    """Extrae fecha_datos del contenido del ZIP; cae a Last-Modified si no puede."""
    advertencias: list[str] = []
    listado = sorted(nombres)

    # Nivel 1: un unico directorio raiz con formato YYYY-MM-DD.
    raices = {n.strip("/").split("/")[0] for n in nombres if n.strip("/")}
    raices_fecha = {f for f in (_a_fecha(r) for r in raices) if f is not None}

    fecha: date | None = None
    metodo = ""
    if len(raices) == 1 and len(raices_fecha) == 1:
        fecha = next(iter(raices_fecha))
        metodo = METODO_DIR_ZIP
    else:
        if raices_fecha:
            advertencias.append(
                f"raices del ZIP no homogeneas: {sorted(raices)[:10]}"
            )
        # Nivel 2: fechas embebidas en los nombres internos.
        encontradas = {
            f
            for n in nombres
            for m in RE_FECHA_EN_NOMBRE.findall(Path(n).name)
            if (f := _a_fecha(m)) is not None
        }
        if len(encontradas) == 1:
            fecha = next(iter(encontradas))
            metodo = METODO_NOMBRES_ZIP
        elif len(encontradas) > 1:
            advertencias.append(
                "el ZIP contiene mas de una fecha interna: "
                f"{[f.isoformat() for f in sorted(encontradas)]}"
            )

    if fecha is None:
        fallback = _fecha_por_last_modified(recurso, lm)
        if fallback is None:
            raise ErrorPermanente(
                "no se pudo determinar fecha_datos: el ZIP no expone una fecha "
                "reconocible y la respuesta no trajo Last-Modified"
            )
        advertencias.append(
            "fecha_datos derivada de Last-Modified porque el ZIP no expone una "
            "fecha inequivoca; revisar el listado"
        )
        return FechaDetectada(fallback, METODO_LAST_MODIFIED, listado, advertencias)

    if fecha.weekday() != recurso.weekday:
        advertencias.append(
            f"fecha_datos {fecha.isoformat()} cae {fecha.strftime('%A')} pero el "
            f"recurso es de {recurso.dia}; se respeta la fecha del ZIP"
        )

    hoy = datetime.now(TZ_AR).date()
    if fecha > hoy:
        advertencias.append(f"fecha_datos {fecha.isoformat()} es futura respecto de {hoy}")
    elif (hoy - fecha).days > 8:
        advertencias.append(
            f"fecha_datos {fecha.isoformat()} tiene {(hoy - fecha).days} dias de "
            "antiguedad; el recurso deberia renovarse cada 7"
        )

    return FechaDetectada(fecha, metodo, listado, advertencias)


# --------------------------------------------------------------------------- #
# Capa GCS
# --------------------------------------------------------------------------- #


class AlmacenGCS:
    """Acceso al bucket. Con `dry_run` no escribe nada, solo lee y loguea."""

    def __init__(self, cfg: Config, dry_run: bool) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        self._cliente = storage.Client()
        self._bucket = self._cliente.bucket(cfg.bucket)

    # -- lectura ----------------------------------------------------------- #

    def leer_texto(self, path: str) -> tuple[str | None, int | None]:
        """Devuelve (contenido, generation) o (None, None) si no existe."""
        blob = self._bucket.blob(path)
        try:
            datos = blob.download_as_bytes()
        except gexc.NotFound:
            return None, None
        return datos.decode("utf-8"), blob.generation

    def obtener_blob(self, path: str):
        return self._bucket.get_blob(path)

    def verificar_acceso(self) -> None:
        """Falla temprano si el bucket no existe o no podemos leer objetos.

        `bucket.reload()` pide `storage.buckets.get`, permiso que roles de solo
        objetos (storage.objectAdmin, objectCreator) no incluyen. Que falle con
        403 no significa que no podamos trabajar, asi que ante Forbidden solo
        avisamos y comprobamos el acceso real a nivel objeto.
        """
        try:
            self._bucket.reload()
            log(logging.INFO, "bucket accesible", bucket=self.cfg.bucket)
            return
        except gexc.NotFound as exc:
            raise ErrorPermanente(
                f"el bucket gs://{self.cfg.bucket} no existe"
            ) from exc
        except gexc.Forbidden:
            log(
                logging.INFO,
                "sin permiso storage.buckets.get; se verifica acceso a nivel objeto",
                bucket=self.cfg.bucket,
            )

        # Un GET sobre un objeto inexistente devuelve NotFound si tenemos lectura
        # y Forbidden si no tenemos nada.
        try:
            self._bucket.blob(f"{self.cfg.prefijo_meta}/.chequeo-de-acceso").reload()
        except gexc.NotFound:
            return
        except gexc.Forbidden as exc:
            raise ErrorPermanente(
                f"sin permisos de lectura sobre gs://{self.cfg.bucket}: {exc}"
            ) from exc

    # -- escritura --------------------------------------------------------- #

    def subir_archivo(
        self,
        origen: Path,
        destino: str,
        content_type: str,
        metadata: dict[str, str],
        solo_si_no_existe: bool = True,
    ) -> bool:
        """Sube `origen` a `destino`. Devuelve False si ya existia (sin pisar)."""
        if self.dry_run:
            log(
                logging.INFO,
                "[dry-run] se subiria el archivo",
                destino=f"gs://{self.cfg.bucket}/{destino}",
                bytes=origen.stat().st_size,
            )
            return True

        blob = self._bucket.blob(destino)
        blob.content_type = content_type
        blob.metadata = metadata
        # Chunks de 8 MiB: subida reanudable sin cargar el archivo en memoria.
        blob.chunk_size = 8 * 1024 * 1024
        try:
            blob.upload_from_filename(
                str(origen),
                content_type=content_type,
                if_generation_match=0 if solo_si_no_existe else None,
                timeout=self.cfg.timeout_lectura,
            )
        except gexc.PreconditionFailed:
            return False
        return True

    def escribir_texto(
        self, path: str, contenido: str, generation: int | None
    ) -> int | None:
        """Escribe texto con precondicion de generacion (evita pisadas en carreras)."""
        if self.dry_run:
            log(
                logging.INFO,
                "[dry-run] se escribiria el objeto",
                destino=f"gs://{self.cfg.bucket}/{path}",
                bytes=len(contenido.encode("utf-8")),
            )
            return generation
        blob = self._bucket.blob(path)
        blob.upload_from_string(
            contenido.encode("utf-8"),
            content_type="application/json; charset=utf-8",
            if_generation_match=0 if generation is None else generation,
            timeout=self.cfg.timeout_lectura,
        )
        return blob.generation


# --------------------------------------------------------------------------- #
# Manifest y estado
# --------------------------------------------------------------------------- #

class Manifest:
    """`_meta/manifest.jsonl`, append-only. GCS no soporta append real, asi que
    se reescribe entero con precondicion de generacion en cada agregado."""

    def __init__(self, almacen: AlmacenGCS, path: str) -> None:
        self.almacen = almacen
        self.path = path
        self._texto = ""
        self._generation: int | None = None
        self.registros: list[dict[str, Any]] = []

    def cargar(self) -> None:
        texto, generation = self.almacen.leer_texto(self.path)
        self._texto = texto or ""
        self._generation = generation
        self.registros = []
        malformadas = 0
        for numero, linea in enumerate(self._texto.splitlines(), start=1):
            linea = linea.strip()
            if not linea:
                continue
            try:
                self.registros.append(json.loads(linea))
            except json.JSONDecodeError:
                malformadas += 1
                log(logging.WARNING, "linea de manifest ilegible", linea_nro=numero)
        log(
            logging.INFO,
            "manifest cargado",
            path=self.path,
            registros=len(self.registros),
            lineas_malformadas=malformadas,
            existia=texto is not None,
        )

    # -- consultas --------------------------------------------------------- #

    def hashes(self) -> dict[str, str]:
        return {
            r["sha256"]: r.get("gcs_path", "")
            for r in self.registros
            if r.get("sha256")
        }

    def ultimo_de_dia(self, dia: str) -> dict[str, Any] | None:
        for r in reversed(self.registros):
            if r.get("dia_semana") == dia:
                return r
        return None

    def paths(self) -> set[str]:
        return {r["gcs_path"] for r in self.registros if r.get("gcs_path")}

    # -- escritura --------------------------------------------------------- #

    def agregar(self, registro: dict[str, Any]) -> None:
        linea = json.dumps(registro, ensure_ascii=False, sort_keys=False)
        for intento in range(1, 6):
            nuevo = self._texto + ("" if self._texto.endswith("\n") or not self._texto else "\n") + linea + "\n"
            try:
                self._generation = self.almacen.escribir_texto(
                    self.path, nuevo, self._generation
                )
                self._texto = nuevo
                self.registros.append(registro)
                return
            except gexc.PreconditionFailed:
                log(
                    logging.WARNING,
                    "el manifest cambio mientras escribiamos; releyendo",
                    intento=intento,
                )
                self.cargar()
        raise ErrorPermanente(
            "no se pudo agregar al manifest tras varios intentos por escrituras concurrentes"
        )


class Estado:
    """`_meta/estado.json`: memoria de lo observado por dia de semana.

    Existe porque el manifest solo registra descargas que terminaron en subida.
    Sin este archivo, un ZIP cuyo contenido ya estaba archivado (mismo sha256,
    distinto ETag) no dejaria rastro y se volveria a bajar entero todos los dias.
    """

    def __init__(self, almacen: AlmacenGCS, path: str) -> None:
        self.almacen = almacen
        self.path = path
        self._generation: int | None = None
        self.datos: dict[str, Any] = {"version": 1, "dias": {}, "sha256_vistos": {}}

    def cargar(self) -> None:
        texto, generation = self.almacen.leer_texto(self.path)
        self._generation = generation
        if texto:
            try:
                cargado = json.loads(texto)
                if isinstance(cargado, dict):
                    self.datos = {
                        "version": cargado.get("version", 1),
                        "dias": cargado.get("dias", {}) or {},
                        "sha256_vistos": cargado.get("sha256_vistos", {}) or {},
                    }
            except json.JSONDecodeError:
                log(logging.WARNING, "estado.json ilegible; se reconstruye", path=self.path)
        log(
            logging.INFO,
            "estado cargado",
            path=self.path,
            dias_conocidos=len(self.datos["dias"]),
            hashes_conocidos=len(self.datos["sha256_vistos"]),
        )

    def dia(self, dia: str) -> dict[str, Any]:
        return self.datos["dias"].get(dia, {})

    def registrar(self, dia: str, **campos: Any) -> None:
        entrada = dict(self.datos["dias"].get(dia, {}))
        entrada.update(campos)
        self.datos["dias"][dia] = entrada
        sha = campos.get("sha256")
        if sha:
            self.datos["sha256_vistos"][sha] = campos.get("gcs_path") or entrada.get(
                "gcs_path", ""
            )

    def guardar(self) -> None:
        contenido = json.dumps(self.datos, ensure_ascii=False, indent=2, sort_keys=True)
        for intento in range(1, 6):
            try:
                self._generation = self.almacen.escribir_texto(
                    self.path, contenido, self._generation
                )
                return
            except gexc.PreconditionFailed:
                log(logging.WARNING, "estado.json cambio; releyendo", intento=intento)
                anterior = dict(self.datos)
                self.cargar()
                # Reaplicamos lo nuestro encima de lo que haya en el bucket.
                self.datos["dias"].update(anterior["dias"])
                self.datos["sha256_vistos"].update(anterior["sha256_vistos"])
                contenido = json.dumps(
                    self.datos, ensure_ascii=False, indent=2, sort_keys=True
                )
        log(logging.ERROR, "no se pudo guardar estado.json", path=self.path)


# --------------------------------------------------------------------------- #
# Resultado por dia
# --------------------------------------------------------------------------- #

OK_SUBIDO = "subido"
OK_SALTEADO_ETAG = "salteado_etag"
OK_SALTEADO_HASH = "salteado_hash"
OK_YA_ARCHIVADO = "ya_archivado"
ANOMALIA_CONFLICTO = "conflicto"
FALLA = "fallo"


@dataclasses.dataclass
class Resultado:
    dia: str
    estado: str
    detalle: str = ""
    fecha_datos: str | None = None
    metodo_fecha: str | None = None
    gcs_path: str | None = None
    bytes: int | None = None
    advertencias: list[str] = dataclasses.field(default_factory=list)

    @property
    def es_falla(self) -> bool:
        return self.estado == FALLA

    @property
    def es_anomalia(self) -> bool:
        return self.estado == ANOMALIA_CONFLICTO or bool(self.advertencias)


# --------------------------------------------------------------------------- #
# Procesamiento de un dia
# --------------------------------------------------------------------------- #


def _ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ruta_destino(cfg: Config, fecha: date, dia: str) -> str:
    return f"{cfg.prefijo_raw}/fecha_datos={fecha.isoformat()}/sepa_{dia}.zip"


def _ruta_conflicto(cfg: Config, fecha: date, dia: str, sha256: str) -> str:
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        f"{cfg.prefijo_raw}/fecha_datos={fecha.isoformat()}/_conflictos/"
        f"sepa_{dia}__{marca}__{sha256[:12]}.zip"
    )


def procesar_dia(
    recurso: Recurso,
    cliente: httpx.Client,
    almacen: AlmacenGCS,
    manifest: Manifest,
    estado: Estado,
    cfg: Config,
    forzar: bool,
    tmpdir: Path,
) -> Resultado:
    campos = {"dia": recurso.dia, "resource_id": recurso.resource_id}
    log(logging.INFO, "procesando dia", url=recurso.url, **campos)

    cabeceras = head_recurso(cliente, cfg, recurso.url, **campos)
    log(
        logging.INFO,
        "cabeceras remotas",
        etag=cabeceras.etag,
        last_modified=cabeceras.last_modified,
        bytes=cabeceras.bytes_esperados,
        **campos,
    )

    conocido = estado.dia(recurso.dia)
    ultimo_manifest = manifest.ultimo_de_dia(recurso.dia)
    etag_conocido = conocido.get("etag") or (
        ultimo_manifest.get("etag") if ultimo_manifest else None
    )

    # 1. Salteo por ETag sin cambios.
    if not forzar and cabeceras.etag and cabeceras.etag == etag_conocido:
        gcs_path = conocido.get("gcs_path") or (
            ultimo_manifest.get("gcs_path") if ultimo_manifest else None
        )
        log(logging.INFO, "ETag sin cambios: no se descarga", etag=cabeceras.etag, **campos)
        return Resultado(
            dia=recurso.dia,
            estado=OK_SALTEADO_ETAG,
            detalle=f"ETag {cabeceras.etag} ya registrado",
            fecha_datos=conocido.get("fecha_datos"),
            gcs_path=gcs_path,
        )

    # 2. Descarga en streaming a temporal.
    destino_tmp = tmpdir / f"{recurso.dia}.zip"
    try:
        sha256, tamanio, cab_get = descargar_a_archivo(
            cliente, cfg, recurso.url, destino_tmp, cabeceras.bytes_esperados, **campos
        )
        etag = cab_get.etag or cabeceras.etag
        last_modified = cab_get.last_modified or cabeceras.last_modified
        last_modified_dt = cab_get.last_modified_dt or cabeceras.last_modified_dt

        # 3. Dedup por contenido: el ETag puede cambiar sin que cambie el ZIP.
        hashes = {**manifest.hashes(), **estado.datos["sha256_vistos"]}
        if not forzar and sha256 in hashes:
            log(
                logging.INFO,
                "contenido ya archivado (sha256 conocido): no se sube",
                sha256=sha256,
                gcs_path=hashes[sha256],
                **campos,
            )
            estado.registrar(
                recurso.dia,
                etag=etag,
                last_modified=last_modified,
                sha256=sha256,
                bytes=tamanio,
                gcs_path=hashes[sha256] or conocido.get("gcs_path"),
                fecha_datos=conocido.get("fecha_datos"),
                visto_en=_ahora_iso(),
                resultado=OK_SALTEADO_HASH,
            )
            return Resultado(
                dia=recurso.dia,
                estado=OK_SALTEADO_HASH,
                detalle="sha256 ya presente en el archivo historico",
                fecha_datos=conocido.get("fecha_datos"),
                gcs_path=hashes[sha256] or None,
                bytes=tamanio,
            )

        # 4. Integridad + fecha_datos desde el contenido del ZIP.
        nombres = validar_zip(destino_tmp)
        deteccion = determinar_fecha_datos(recurso, nombres, last_modified_dt)

        for adv in deteccion.advertencias:
            log(logging.WARNING, "anomalia al determinar fecha_datos", advertencia=adv, **campos)
        if deteccion.metodo == METODO_LAST_MODIFIED:
            log(
                logging.ERROR,
                "fecha_datos no pudo derivarse del ZIP; listado completo para revision",
                listado=deteccion.listado[:200],
                entradas=len(deteccion.listado),
                **campos,
            )
            if cfg.fecha_estricta:
                raise ErrorPermanente(
                    "SEPA_FECHA_ESTRICTA activo y fecha_datos no se pudo determinar "
                    "desde el ZIP; no se sube nada para este dia"
                )

        fecha = deteccion.fecha
        log(
            logging.INFO,
            "fecha_datos determinada",
            fecha_datos=fecha.isoformat(),
            metodo_fecha=deteccion.metodo,
            **campos,
        )

        destino = _ruta_destino(cfg, fecha, recurso.dia)
        metadata = {
            "sha256": sha256,
            "dia_semana": recurso.dia,
            "resource_id": recurso.resource_id,
            "fecha_datos": fecha.isoformat(),
            "metodo_fecha": deteccion.metodo,
            "etag_origen": etag or "",
            "last_modified_origen": last_modified or "",
        }

        # 5. No pisar. Si ya hay algo distinto en esa ruta, es una anomalia.
        existente = almacen.obtener_blob(destino)
        es_conflicto = False
        if existente is not None:
            sha_existente = (existente.metadata or {}).get("sha256")
            if sha_existente == sha256:
                log(
                    logging.INFO,
                    "el objeto ya estaba archivado con el mismo contenido",
                    gcs_path=destino,
                    **campos,
                )
                estado.registrar(
                    recurso.dia,
                    etag=etag,
                    last_modified=last_modified,
                    sha256=sha256,
                    bytes=tamanio,
                    gcs_path=destino,
                    fecha_datos=fecha.isoformat(),
                    visto_en=_ahora_iso(),
                    resultado=OK_YA_ARCHIVADO,
                )
                return Resultado(
                    dia=recurso.dia,
                    estado=OK_YA_ARCHIVADO,
                    detalle="mismo contenido ya presente en destino",
                    fecha_datos=fecha.isoformat(),
                    metodo_fecha=deteccion.metodo,
                    gcs_path=destino,
                    bytes=tamanio,
                    advertencias=deteccion.advertencias,
                )
            es_conflicto = True
            log(
                logging.ERROR,
                "CONFLICTO: ya existe otro contenido para esa fecha_datos",
                gcs_path=destino,
                sha256_nuevo=sha256,
                sha256_existente=sha_existente,
                **campos,
            )

        if es_conflicto:
            destino_final = _ruta_conflicto(cfg, fecha, recurso.dia, sha256)
            metadata["conflicto_con"] = destino
            subido = almacen.subir_archivo(
                destino_tmp, destino_final, "application/zip", metadata,
                solo_si_no_existe=True,
            )
        else:
            destino_final = destino
            subido = almacen.subir_archivo(
                destino_tmp, destino_final, "application/zip", metadata,
                solo_si_no_existe=True,
            )
            if not subido:
                # Alguien escribio esa ruta entre el chequeo y la subida.
                destino_final = _ruta_conflicto(cfg, fecha, recurso.dia, sha256)
                metadata["conflicto_con"] = destino
                es_conflicto = True
                log(
                    logging.ERROR,
                    "CONFLICTO por carrera: la ruta se ocupo durante la subida",
                    gcs_path=destino,
                    **campos,
                )
                almacen.subir_archivo(
                    destino_tmp, destino_final, "application/zip", metadata,
                    solo_si_no_existe=True,
                )

        # 6. Manifest.
        registro = {
            "fecha_datos": fecha.isoformat(),
            "dia_semana": recurso.dia,
            "resource_id": recurso.resource_id,
            "url": recurso.url,
            "gcs_path": f"gs://{cfg.bucket}/{destino_final}",
            "sha256": sha256,
            "etag": etag,
            "last_modified": last_modified,
            "bytes": tamanio,
            "descargado_en": _ahora_iso(),
            "metodo_fecha": deteccion.metodo,
        }
        if es_conflicto:
            registro["conflicto_con"] = f"gs://{cfg.bucket}/{destino}"
        if deteccion.advertencias:
            registro["advertencias"] = deteccion.advertencias

        if almacen.dry_run:
            log(logging.INFO, "[dry-run] linea de manifest", registro=registro, **campos)
        else:
            manifest.agregar(registro)

        estado.registrar(
            recurso.dia,
            etag=etag,
            last_modified=last_modified,
            sha256=sha256,
            bytes=tamanio,
            gcs_path=destino_final,
            fecha_datos=fecha.isoformat(),
            visto_en=_ahora_iso(),
            resultado=ANOMALIA_CONFLICTO if es_conflicto else OK_SUBIDO,
        )

        log(
            logging.INFO,
            "archivado",
            gcs_path=f"gs://{cfg.bucket}/{destino_final}",
            bytes=tamanio,
            **campos,
        )
        return Resultado(
            dia=recurso.dia,
            estado=ANOMALIA_CONFLICTO if es_conflicto else OK_SUBIDO,
            detalle=(
                "guardado aparte por conflicto de contenido"
                if es_conflicto
                else "subido"
            ),
            fecha_datos=fecha.isoformat(),
            metodo_fecha=deteccion.metodo,
            gcs_path=f"gs://{cfg.bucket}/{destino_final}",
            bytes=tamanio,
            advertencias=deteccion.advertencias,
        )
    finally:
        destino_tmp.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Archivos de referencia
# --------------------------------------------------------------------------- #

CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def procesar_referencias(
    cliente: httpx.Client,
    almacen: AlmacenGCS,
    cfg: Config,
    tmpdir: Path,
    forzar: bool,
) -> list[Resultado]:
    resultados: list[Resultado] = []
    for ref in REFERENCIAS:
        campos = {"referencia": ref.nombre_destino}
        destino = f"{cfg.prefijo_referencia}/{ref.nombre_destino}"
        tmp = tmpdir / ref.nombre_destino
        try:
            existente = almacen.obtener_blob(destino)
            if existente is not None and not forzar:
                cab = head_recurso(cliente, cfg, ref.url, **campos)
                if cab.etag and (existente.metadata or {}).get("etag_origen") == cab.etag:
                    log(logging.INFO, "referencia sin cambios", destino=destino, **campos)
                    resultados.append(
                        Resultado(ref.nombre_destino, OK_SALTEADO_ETAG, "ETag sin cambios")
                    )
                    continue

            sha256, tamanio, cab_get = descargar_a_archivo(
                cliente, cfg, ref.url, tmp, None, **campos
            )
            if existente is not None and (existente.metadata or {}).get("sha256") == sha256:
                log(logging.INFO, "referencia identica a la archivada", destino=destino, **campos)
                resultados.append(
                    Resultado(ref.nombre_destino, OK_YA_ARCHIVADO, "sha256 sin cambios")
                )
                continue

            sufijo = Path(ref.nombre_destino).suffix
            content_type = CONTENT_TYPES.get(sufijo, "application/octet-stream")
            metadata = {
                "sha256": sha256,
                "etag_origen": cab_get.etag or "",
                "last_modified_origen": cab_get.last_modified or "",
                "resource_id": ref.resource_id,
                "archivado_en": _ahora_iso(),
            }

            # Toda version distinta queda guardada; la ruta canonica apunta a la ultima.
            historico = (
                f"{cfg.prefijo_referencia}/historico/"
                f"{Path(ref.nombre_destino).stem}__{sha256[:12]}{sufijo}"
            )
            almacen.subir_archivo(tmp, historico, content_type, metadata, solo_si_no_existe=True)
            almacen.subir_archivo(tmp, destino, content_type, metadata, solo_si_no_existe=False)

            log(
                logging.INFO,
                "referencia archivada",
                destino=f"gs://{cfg.bucket}/{destino}",
                historico=f"gs://{cfg.bucket}/{historico}",
                bytes=tamanio,
                **campos,
            )
            resultados.append(
                Resultado(
                    ref.nombre_destino,
                    OK_SUBIDO,
                    "nueva version archivada",
                    gcs_path=f"gs://{cfg.bucket}/{destino}",
                    bytes=tamanio,
                )
            )
        except Exception as exc:  # noqa: BLE001 - una referencia rota no debe tumbar el run
            log(logging.ERROR, "fallo el archivo de referencia", error=str(exc), **campos)
            resultados.append(Resultado(ref.nombre_destino, FALLA, str(exc)))
        finally:
            tmp.unlink(missing_ok=True)
    return resultados


# --------------------------------------------------------------------------- #
# Verificacion de resource IDs contra CKAN
# --------------------------------------------------------------------------- #


def verificar_resource_ids(cliente: httpx.Client, cfg: Config) -> list[str]:
    """Compara los IDs hardcodeados con los que publica CKAN. No es fatal."""
    avisos: list[str] = []
    try:
        resp = cliente.get(CKAN_PACKAGE_URL, timeout=60.0)
        resp.raise_for_status()
        datos = resp.json()
    except Exception as exc:  # noqa: BLE001
        log(logging.WARNING, "no se pudo consultar CKAN para verificar IDs", error=str(exc))
        return avisos

    recursos = (datos.get("result") or {}).get("resources") or []
    por_filename: dict[str, str] = {}
    for r in recursos:
        nombre = (r.get("url") or "").rstrip("/").split("/")[-1].lower()
        if nombre:
            por_filename[nombre] = r.get("id", "")

    for recurso in RECURSOS:
        actual = por_filename.get(recurso.filename.lower())
        if actual is None:
            aviso = f"CKAN no lista {recurso.filename}"
            avisos.append(aviso)
            log(logging.WARNING, "recurso ausente en CKAN", dia=recurso.dia, archivo=recurso.filename)
        elif actual != recurso.resource_id:
            aviso = (
                f"{recurso.dia}: resource_id cambio de {recurso.resource_id} a {actual}"
            )
            avisos.append(aviso)
            log(
                logging.ERROR,
                "RESOURCE ID DESACTUALIZADO",
                dia=recurso.dia,
                esperado=recurso.resource_id,
                actual=actual,
            )
    if not avisos:
        log(logging.INFO, "resource IDs verificados contra CKAN", recursos=len(RECURSOS))
    return avisos


# --------------------------------------------------------------------------- #
# CLI y orquestacion
# --------------------------------------------------------------------------- #


def _normalizar(texto: str) -> str:
    sin_acentos = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in sin_acentos if not unicodedata.combining(c)).lower().strip()


def parsear_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sepa_downloader",
        description=(
            "Descarga los 7 ZIP diarios de SEPA y los archiva crudos en GCS. "
            "El dataset no tiene historico: cada dia sin correr es un dia perdido."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="hace todo (HEAD, descarga, validacion, fecha) pero no escribe en GCS",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="ignora manifest y estado: vuelve a bajar y subir todo",
    )
    p.add_argument(
        "--dia",
        default=None,
        help="procesa un solo dia (lunes, martes, miercoles, jueves, viernes, sabado, domingo)",
    )
    p.add_argument("--bucket", default=None, help="sobrescribe SEPA_BUCKET")
    p.add_argument(
        "--sin-referencia",
        action="store_true",
        help="no toca los archivos de referencia (diccionario y maestro de provincias)",
    )
    p.add_argument(
        "--solo-referencia",
        action="store_true",
        help="solo archiva los archivos de referencia y termina",
    )
    p.add_argument(
        "--sin-verificar-ids",
        action="store_true",
        help="saltea la consulta a CKAN que valida los resource IDs",
    )
    return p.parse_args(argv)


def _resolver_dias(arg_dia: str | None) -> list[Recurso]:
    if not arg_dia:
        return list(RECURSOS)
    clave = _normalizar(arg_dia)
    if clave not in RECURSOS_POR_DIA:
        validos = ", ".join(RECURSOS_POR_DIA)
        raise SystemExit(f"--dia invalido: {arg_dia!r}. Validos: {validos}")
    return [RECURSOS_POR_DIA[clave]]


def imprimir_resumen(
    resultados: list[Resultado],
    referencias: list[Resultado],
    avisos_ckan: list[str],
    cfg: Config,
    dry_run: bool,
) -> None:
    ancho_nombre = max(
        [11] + [len(r.dia) for r in (*resultados, *referencias)]
    )
    regla = "=" * 96
    print("", flush=True)
    print(regla)
    print(
        f"RESUMEN SEPA -> gs://{cfg.bucket}"
        + ("  [DRY-RUN: no se escribio nada]" if dry_run else "")
    )
    print(regla)
    print(
        f"{'RECURSO':<{ancho_nombre}} {'ESTADO':<16} {'FECHA_DATOS':<12} {'MB':>8}  DETALLE"
    )
    print("-" * 96)
    for r in resultados:
        mb = f"{r.bytes / 1e6:.1f}" if r.bytes else "-"
        print(
            f"{r.dia:<{ancho_nombre}} {r.estado:<16} "
            f"{(r.fecha_datos or '-'):<12} {mb:>8}  {r.detalle}"
        )
        for adv in r.advertencias:
            print(f"{'':<{ancho_nombre}} {'!':<16} {'':<12} {'':>8}  {adv}")
    if referencias:
        print("-" * 96)
        for r in referencias:
            mb = f"{r.bytes / 1e6:.1f}" if r.bytes else "-"
            print(
                f"{r.dia:<{ancho_nombre}} {r.estado:<16} "
                f"{'-':<12} {mb:>8}  {r.detalle}"
            )

    subidos = [r for r in resultados if r.estado == OK_SUBIDO]
    salteados = [r for r in resultados if r.estado.startswith("salteado") or r.estado == OK_YA_ARCHIVADO]
    conflictos = [r for r in resultados if r.estado == ANOMALIA_CONFLICTO]
    fallas = [r for r in resultados if r.es_falla] + [r for r in referencias if r.es_falla]
    advertidos = [r for r in resultados if r.advertencias]

    print("-" * 96)
    print(
        f"subidos={len(subidos)}  salteados={len(salteados)}  "
        f"conflictos={len(conflictos)}  fallas={len(fallas)}  "
        f"con_advertencias={len(advertidos)}"
    )
    if avisos_ckan:
        print("")
        print("AVISOS DE CKAN (revisar los resource IDs hardcodeados):")
        for a in avisos_ckan:
            print(f"  - {a}")
    if fallas:
        print("")
        print("FALLAS:")
        for r in fallas:
            print(f"  - {r.dia}: {r.detalle}")
    if conflictos:
        print("")
        print("CONFLICTOS (dos contenidos distintos para la misma fecha_datos):")
        for r in conflictos:
            print(f"  - {r.dia} -> {r.gcs_path}")
    print(regla, flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = parsear_args(argv)
    cfg = Config.desde_entorno(args)
    configurar_logging(cfg.log_nivel, cfg.log_formato)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    recursos = _resolver_dias(args.dia)
    log(
        logging.INFO,
        "inicio",
        bucket=cfg.bucket,
        dias=[r.dia for r in recursos],
        dry_run=args.dry_run,
        force=args.force,
    )

    timeout = httpx.Timeout(
        connect=cfg.timeout_conexion,
        read=cfg.timeout_lectura,
        write=cfg.timeout_lectura,
        pool=cfg.timeout_conexion,
    )

    resultados: list[Resultado] = []
    referencias: list[Resultado] = []
    avisos_ckan: list[str] = []
    tmp_raiz: Path | None = None

    try:
        almacen = AlmacenGCS(cfg, dry_run=args.dry_run)
        almacen.verificar_acceso()

        manifest = Manifest(almacen, cfg.manifest_path)
        manifest.cargar()
        estado = Estado(almacen, cfg.estado_path)
        estado.cargar()

        tmp_raiz = Path(tempfile.mkdtemp(prefix="sepa_", dir=cfg.tmpdir))
        libre = shutil.disk_usage(tmp_raiz).free
        if libre < 2 * 1024**3:
            log(
                logging.WARNING,
                "poco espacio libre en el temporal; los ZIP rondan los 350 MB",
                tmpdir=str(tmp_raiz),
                libre_mb=round(libre / 1e6),
            )

        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        ) as cliente:
            if not args.sin_verificar_ids:
                avisos_ckan = verificar_resource_ids(cliente, cfg)

            if not args.solo_referencia:
                for recurso in recursos:
                    try:
                        resultados.append(
                            procesar_dia(
                                recurso, cliente, almacen, manifest, estado,
                                cfg, args.force, tmp_raiz,
                            )
                        )
                    except Exception as exc:  # noqa: BLE001 - un dia no debe tumbar los otros
                        log(
                            logging.ERROR,
                            "fallo el dia",
                            dia=recurso.dia,
                            error=f"{type(exc).__name__}: {exc}",
                            exc_info=True,
                        )
                        resultados.append(
                            Resultado(recurso.dia, FALLA, f"{type(exc).__name__}: {exc}")
                        )
                    finally:
                        if not args.dry_run:
                            estado.guardar()

            if not args.sin_referencia:
                referencias = procesar_referencias(
                    cliente, almacen, cfg, tmp_raiz, args.force
                )

        if not args.dry_run:
            estado.guardar()

    except Exception as exc:  # noqa: BLE001
        log(logging.CRITICAL, "el run aborto", error=f"{type(exc).__name__}: {exc}", exc_info=True)
        imprimir_resumen(resultados, referencias, avisos_ckan, cfg, args.dry_run)
        return 1
    finally:
        if tmp_raiz is not None:
            shutil.rmtree(tmp_raiz, ignore_errors=True)

    imprimir_resumen(resultados, referencias, avisos_ckan, cfg, args.dry_run)

    hay_fallas = any(r.es_falla for r in resultados) or any(r.es_falla for r in referencias)
    hay_anomalias = any(r.es_anomalia for r in resultados) or bool(avisos_ckan)

    if hay_fallas:
        log(logging.ERROR, "fin con fallas", exit_code=1)
        return 1
    if hay_anomalias:
        log(logging.WARNING, "fin con anomalias para revisar", exit_code=2)
        return 2
    log(logging.INFO, "fin ok", exit_code=0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
