"""Fixtures: construccion de ZIP sinteticos con la forma real de SEPA."""

from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

from precios.config import Config  # noqa: E402
from precios.normalize.provincias import MaestroProvincias  # noqa: E402
from precios.normalize.unidades import TablaUnidades  # noqa: E402

CABECERA_PRODUCTOS = (
    "id_comercio|id_bandera|id_sucursal|id_producto|productos_ean|"
    "productos_descripcion|productos_cantidad_presentacion|"
    "productos_unidad_medida_presentacion|productos_marca|productos_precio_lista|"
    "productos_precio_referencia|productos_cantidad_referencia|"
    "productos_unidad_medida_referencia|productos_precio_unitario_promo1|"
    "productos_leyenda_promo1|productos_precio_unitario_promo2|"
    "productos_leyenda_promo2"
)

CABECERA_SUCURSALES = (
    "id_comercio|id_bandera|id_sucursal|sucursales_nombre|sucursales_tipo|"
    "sucursales_calle|sucursales_numero|sucursales_latitud|sucursales_longitud|"
    "sucursales_observaciones|sucursales_barrio|sucursales_codigo_postal|"
    "sucursales_localidad|sucursales_provincia|sucursales_lunes_horario_atencion|"
    "sucursales_martes_horario_atencion|sucursales_miercoles_horario_atencion|"
    "sucursales_jueves_horario_atencion|sucursales_viernes_horario_atencion|"
    "sucursales_sabado_horario_atencion|sucursales_domingo_horario_atencion"
)

CABECERA_COMERCIO = (
    "id_comercio|id_bandera|comercio_cuit|comercio_razon_social|"
    "comercio_bandera_nombre|comercio_bandera_url|comercio_ultima_actualizacion|"
    "comercio_version_sepa"
)


def armar_csv(
    cabecera: str,
    filas: list[str],
    *,
    bom: bool = False,
    crlf: bool = True,
    pie: str | None = "Última actualización: 2026-08-01T02:02:18-03:00",
) -> bytes:
    """Reproduce el formato real: pipe, BOM opcional, CRLF/LF, linea de pie."""
    sep = "\r\n" if crlf else "\n"
    cuerpo = sep.join([cabecera, *filas])
    if pie is not None:
        cuerpo += sep + sep + pie
    cuerpo += sep
    datos = cuerpo.encode("utf-8")
    return (b"\xef\xbb\xbf" + datos) if bom else datos


def armar_zip_comercio(
    productos: bytes, sucursales: bytes | None = None, comercio: bytes | None = None
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("productos.csv", productos)
        if sucursales is not None:
            z.writestr("sucursales.csv", sucursales)
        if comercio is not None:
            z.writestr("comercio.csv", comercio)
    return buf.getvalue()


def armar_zip_diario(path: Path, fecha: str, comercios: dict[str, bytes]) -> Path:
    """`comercios`: {etiqueta -> bytes del zip interno} (bytes vacios = zip roto)."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        z.writestr(f"{fecha}/", b"")
        for etiqueta, contenido in comercios.items():
            z.writestr(f"{fecha}/{etiqueta}", contenido)
    return path


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    import os

    os.environ["PRECIOS_TMPDIR"] = str(tmp_path / "tmp")
    (tmp_path / "tmp").mkdir(exist_ok=True)
    c = Config.desde_entorno()
    return c


@pytest.fixture
def unidades() -> TablaUnidades:
    return TablaUnidades.desde_yaml(RAIZ / "config" / "unidades.yaml")


@pytest.fixture
def provincias() -> MaestroProvincias:
    return MaestroProvincias(
        {"AR-C": "CABA", "AR-B": "Buenos Aires", "AR-X": "Córdoba"}
    )
