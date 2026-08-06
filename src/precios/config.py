"""Configuracion por entorno. Sin credenciales hardcodeadas.

Las credenciales de GCS salen siempre de Application Default Credentials
(`gcloud auth application-default login` en local, la identidad de la instancia
en Cloud Run, o `GOOGLE_APPLICATION_CREDENTIALS` apuntando a una service account).
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

# Raiz del repo: src/precios/config.py -> src/precios -> src -> repo
RAIZ_REPO = Path(__file__).resolve().parents[2]


def _env(nombre: str, default: str) -> str:
    return os.environ.get(nombre, default)


def _env_int(nombre: str, default: int) -> int:
    try:
        return int(os.environ.get(nombre, default))
    except ValueError:
        return default


@dataclasses.dataclass(frozen=True)
class Config:
    """Parametros del pipeline. Todos overrideables por variable de entorno."""

    bucket: str
    prefijo_raw: str
    prefijo_staged: str
    prefijo_meta: str
    manifest_path: str

    dir_config: Path
    path_unidades: Path
    path_provincias: Path

    tmpdir: str | None
    memoria_duckdb: str
    hilos_duckdb: int

    # Umbral de la Etapa 1: un precio <= 0 no es un precio bajo, es un dato ausente.
    precio_minimo: float

    log_nivel: str
    log_formato: str

    @classmethod
    def desde_entorno(cls) -> "Config":
        prefijo_meta = _env("PRECIOS_PREFIJO_META", "_meta").strip("/")
        dir_config = Path(_env("PRECIOS_DIR_CONFIG", str(RAIZ_REPO / "config")))
        return cls(
            bucket=_env("PRECIOS_BUCKET", "outlier-archivos-precios"),
            prefijo_raw=_env("PRECIOS_PREFIJO_RAW", "raw/sepa/minorista").strip("/"),
            prefijo_staged=_env("PRECIOS_PREFIJO_STAGED", "staged").strip("/"),
            prefijo_meta=prefijo_meta,
            manifest_path=_env(
                "PRECIOS_MANIFEST_PATH", f"{prefijo_meta}/manifest.jsonl"
            ).strip("/"),
            dir_config=dir_config,
            path_unidades=Path(
                _env("PRECIOS_PATH_UNIDADES", str(dir_config / "unidades.yaml"))
            ),
            # El maestro de provincias vive en el bucket; se cachea local.
            path_provincias=Path(
                _env(
                    "PRECIOS_PATH_PROVINCIAS",
                    str(dir_config / "maestro-provincias.xlsx"),
                )
            ),
            tmpdir=os.environ.get("PRECIOS_TMPDIR") or None,
            memoria_duckdb=_env("PRECIOS_MEMORIA_DUCKDB", "4GB"),
            hilos_duckdb=_env_int("PRECIOS_HILOS_DUCKDB", 4),
            precio_minimo=float(_env("PRECIOS_PRECIO_MINIMO", "0")),
            log_nivel=_env("PRECIOS_LOG_NIVEL", "INFO"),
            log_formato=_env("PRECIOS_LOG_FORMATO", "json"),
        )

    # -- rutas derivadas ---------------------------------------------------- #

    def ruta_zip(self, fecha_datos: str, dia_semana: str) -> str:
        return f"{self.prefijo_raw}/fecha_datos={fecha_datos}/sepa_{dia_semana}.zip"

    def particion(self, tabla: str, fecha) -> str:
        """`staged/<tabla>/anio=YYYY/mes=MM/dia=DD`"""
        return (
            f"{self.prefijo_staged}/{tabla}/"
            f"anio={fecha.year:04d}/mes={fecha.month:02d}/dia={fecha.day:02d}"
        )
