"""ETL diario: ZIP crudo de SEPA -> Parquet normalizado.

Una fila de salida = un producto, en una sucursal, en un dia.

Es idempotente por particion: reprocesar un dia borra su particion y la
reescribe. Eso es lo opuesto al archivo crudo (que nunca se pisa) y es
deliberado: el procesado siempre tiene que poder reconstruirse desde el crudo.
"""

from __future__ import annotations

import dataclasses
import logging
import shutil
import tempfile
from datetime import date
from pathlib import Path

import duckdb

from ..config import Config
from ..logs import log
from . import lectura
from .provincias import PROVINCIA_DESCONOCIDA, MaestroProvincias
from .unidades import SQL_CLAVE_UNIDAD, TablaUnidades

# Columnas de productos.csv segun el Anexo II, verificadas contra datos reales.
COLUMNAS_PRODUCTOS = (
    "id_comercio",
    "id_bandera",
    "id_sucursal",
    "id_producto",
    "productos_ean",
    "productos_descripcion",
    "productos_cantidad_presentacion",
    "productos_unidad_medida_presentacion",
    "productos_marca",
    "productos_precio_lista",
    "productos_precio_referencia",
    "productos_cantidad_referencia",
    "productos_unidad_medida_referencia",
    "productos_precio_unitario_promo1",
    "productos_leyenda_promo1",
    "productos_precio_unitario_promo2",
    "productos_leyenda_promo2",
)

# Columnas de sucursales.csv. Solo usamos provincia/localidad/tipo, pero hay que
# declararlas todas para que el parseo posicional sea estricto.
COLUMNAS_SUCURSALES = (
    "id_comercio",
    "id_bandera",
    "id_sucursal",
    "sucursales_nombre",
    "sucursales_tipo",
    "sucursales_calle",
    "sucursales_numero",
    "sucursales_latitud",
    "sucursales_longitud",
    "sucursales_observaciones",
    "sucursales_barrio",
    "sucursales_codigo_postal",
    "sucursales_localidad",
    "sucursales_provincia",
    "sucursales_lunes_horario_atencion",
    "sucursales_martes_horario_atencion",
    "sucursales_miercoles_horario_atencion",
    "sucursales_jueves_horario_atencion",
    "sucursales_viernes_horario_atencion",
    "sucursales_sabado_horario_atencion",
    "sucursales_domingo_horario_atencion",
)

# La ultima linea del CSV es "Ultima actualizacion: <ISO>" (con y sin tilde).
# No es dato: se descarta explicitamente en vez de con ignore_errors, para no
# tapar filas genuinamente mal formadas.
PATRON_PIE = "%ltima actualizaci%"

MOTIVO_SIN_PRODUCTO = "sin_id_producto"
MOTIVO_SIN_SUCURSAL = "sin_id_sucursal"
MOTIVO_PRECIO_NULO = "precio_lista_nulo_o_vacio"
MOTIVO_PRECIO_NO_NUM = "precio_lista_no_numerico"
MOTIVO_PRECIO_NO_POS = "precio_lista_no_positivo"


@dataclasses.dataclass
class ResumenComercio:
    etiqueta: str
    filas_leidas: int = 0
    observaciones: int = 0
    rechazos: int = 0
    duplicados: int = 0
    unidad_desconocida: int = 0
    sucursal_sin_provincia: int = 0
    error: str | None = None


@dataclasses.dataclass
class ResumenDia:
    fecha: date
    comercios_ok: int = 0
    # El comercio no publico nada usable (ZIP vacio o corrupto en el origen).
    # Es un problema de la fuente, no del pipeline: pasa de forma recurrente y
    # no deberia disparar la misma alerta que un bug nuestro.
    comercios_sin_datos: int = 0
    # Fallo el procesamiento de un comercio que si traia datos. Eso si es nuestro.
    comercios_con_error: int = 0
    observaciones: int = 0
    rechazos: int = 0
    duplicados: int = 0
    unidad_desconocida: int = 0
    detalle: list[ResumenComercio] = dataclasses.field(default_factory=list)

    @property
    def comercios_totales(self) -> int:
        return self.comercios_ok + self.comercios_sin_datos + self.comercios_con_error

    @property
    def hubo_fallas(self) -> bool:
        return self.comercios_con_error > 0

    @property
    def hubo_anomalias(self) -> bool:
        return self.comercios_sin_datos > 0


def conectar_duckdb(cfg: Config) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{cfg.memoria_duckdb}'")
    con.execute(f"SET threads={cfg.hilos_duckdb}")
    # Necesario para que ROW_NUMBER() refleje el orden del archivo y el
    # "quedarse con el ultimo registro" sea determinista.
    con.execute("SET preserve_insertion_order=true")
    _registrar_macros(con)
    return con


def _registrar_macros(con: duckdb.DuckDBPyConnection) -> None:
    """Conversion de texto a numero tolerante a separadores mixtos.

    El Anexo manda punto decimal, pero no todos los comercios lo respetan.
    Cuando aparecen los dos separadores, el que esta mas a la derecha es el
    decimal ("1.234,56" -> 1234.56 y "1,234.56" -> 1234.56).
    """
    con.execute(
        """
        CREATE OR REPLACE MACRO a_numero(s) AS (
            TRY_CAST(
                CASE
                    WHEN s IS NULL OR trim(s) = '' THEN NULL
                    WHEN strpos(s, ',') > 0 AND strpos(s, '.') > 0 THEN
                        CASE WHEN strpos(reverse(s), ',') < strpos(reverse(s), '.')
                             THEN replace(replace(trim(s), '.', ''), ',', '.')
                             ELSE replace(trim(s), ',', '')
                        END
                    WHEN strpos(s, ',') > 0 THEN replace(trim(s), ',', '.')
                    ELSE trim(s)
                END
            AS DOUBLE)
        );
        CREATE OR REPLACE MACRO limpiar(s) AS (
            nullif(trim(regexp_replace(coalesce(s, ''), '\\s+', ' ', 'g')), '')
        );
        """
    )


def _cargar_tablas_auxiliares(
    con: duckdb.DuckDBPyConnection, unidades: TablaUnidades, provincias: MaestroProvincias
) -> None:
    con.execute(
        "CREATE OR REPLACE TABLE dim_unidades "
        "(codigo VARCHAR, canonica VARCHAR, base VARCHAR, factor DOUBLE)"
    )
    con.executemany(
        "INSERT INTO dim_unidades VALUES (?, ?, ?, ?)", unidades.como_filas()
    )
    con.execute(
        "CREATE OR REPLACE TABLE dim_provincias (codigo VARCHAR, provincia VARCHAR)"
    )
    con.executemany("INSERT INTO dim_provincias VALUES (?, ?)", provincias.como_filas())


def _leer_csv(path: Path, columnas: tuple[str, ...]) -> str:
    """SQL de lectura estricta: sin ignore_errors, con null_padding.

    `null_padding` deja pasar la linea de pie (que tiene una sola columna) para
    poder descartarla explicitamente; `ignore_errors` la tiraria junto con
    cualquier fila corrupta, que es justo lo que queremos ver.

    `auto_detect=false` es necesario: el sniffer de DuckDB mira solo las
    primeras filas y en archivos chicos toma la linea de pie como muestra,
    concluye que hay una sola columna y aborta. El dialecto ya lo conocemos por
    el Anexo II, asi que lo declaramos entero y no dejamos nada librado a la
    deteccion (comillas dobles con escape duplicado, pipe, UTF-8).
    """
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in columnas)
    return (
        f"read_csv('{path.as_posix()}', delim='|', quote='\"', escape='\"', "
        f"header=true, columns={{{cols}}}, null_padding=true, "
        f"auto_detect=false, encoding='utf-8')"
    )


SQL_OBSERVACIONES = """
CREATE OR REPLACE TEMP TABLE crudo AS
SELECT *, ROW_NUMBER() OVER () AS _orden
FROM {lectura_productos};

-- La linea de pie del archivo no es dato.
CREATE OR REPLACE TEMP TABLE pie AS
SELECT * FROM crudo
WHERE id_producto IS NULL AND lower(coalesce(id_comercio, '')) LIKE '{patron_pie}';

CREATE OR REPLACE TEMP TABLE filas AS
SELECT * FROM crudo WHERE _orden NOT IN (SELECT _orden FROM pie);

CREATE OR REPLACE TEMP TABLE tipado AS
SELECT
    limpiar(id_comercio)                            AS id_comercio,
    limpiar(id_bandera)                             AS id_bandera,
    limpiar(id_sucursal)                            AS id_sucursal,
    limpiar(id_producto)                            AS id_producto,
    limpiar(productos_ean) = '1'                    AS es_ean,
    upper(limpiar(productos_descripcion))           AS descripcion,
    upper(limpiar(productos_marca))                 AS marca,
    a_numero(productos_cantidad_presentacion)       AS cantidad_presentacion,
    upper(limpiar(productos_unidad_medida_presentacion)) AS unidad_presentacion_raw,
    -- Misma normalizacion que TablaUnidades.clave(): sin esto "GR." no matchea
    -- con "GR" y se pierde el 15% de las filas.
    {clave_unidad}                                  AS unidad_clave,
    a_numero(productos_precio_lista)                AS precio_lista,
    productos_precio_lista                          AS precio_lista_crudo,
    a_numero(productos_precio_unitario_promo1)      AS precio_promo,
    a_numero(productos_precio_referencia)           AS precio_referencia,
    a_numero(productos_cantidad_referencia)         AS cantidad_referencia,
    upper(limpiar(productos_unidad_medida_referencia)) AS unidad_referencia,
    _orden
FROM filas;

-- Motivo de rechazo: primero el que falta, despues el que no parsea.
CREATE OR REPLACE TEMP TABLE clasificado AS
SELECT *,
    CASE
        WHEN id_producto IS NULL                     THEN '{m_sin_producto}'
        WHEN id_sucursal IS NULL                     THEN '{m_sin_sucursal}'
        WHEN precio_lista_crudo IS NULL
          OR trim(precio_lista_crudo) = ''           THEN '{m_precio_nulo}'
        WHEN precio_lista IS NULL                    THEN '{m_precio_no_num}'
        WHEN precio_lista <= {precio_minimo}         THEN '{m_precio_no_pos}'
        ELSE NULL
    END AS motivo_rechazo
FROM tipado;
"""


def _procesar_comercio(
    con: duckdb.DuckDBPyConnection,
    paquete: lectura.PaqueteComercio,
    fecha: date,
    cfg: Config,
    dir_obs: Path,
    dir_rech: Path,
) -> ResumenComercio:
    res = ResumenComercio(etiqueta=paquete.etiqueta)

    con.execute(
        SQL_OBSERVACIONES.format(
            lectura_productos=_leer_csv(paquete.path("productos.csv"), COLUMNAS_PRODUCTOS),
            patron_pie=PATRON_PIE,
            clave_unidad=SQL_CLAVE_UNIDAD.format(
                col="productos_unidad_medida_presentacion"
            ),
            precio_minimo=cfg.precio_minimo,
            m_sin_producto=MOTIVO_SIN_PRODUCTO,
            m_sin_sucursal=MOTIVO_SIN_SUCURSAL,
            m_precio_nulo=MOTIVO_PRECIO_NULO,
            m_precio_no_num=MOTIVO_PRECIO_NO_NUM,
            m_precio_no_pos=MOTIVO_PRECIO_NO_POS,
        )
    )
    res.filas_leidas = con.execute("SELECT count(*) FROM filas").fetchone()[0]

    # Sucursales del comercio, para decodificar provincia.
    if paquete.tiene("sucursales.csv"):
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE suc AS
                SELECT limpiar(id_comercio) AS id_comercio,
                       limpiar(id_sucursal) AS id_sucursal,
                       upper(limpiar(sucursales_provincia)) AS provincia_iso,
                       limpiar(sucursales_localidad) AS localidad,
                       limpiar(sucursales_tipo) AS sucursal_tipo
                FROM {_leer_csv(paquete.path('sucursales.csv'), COLUMNAS_SUCURSALES)}
                WHERE id_sucursal IS NOT NULL"""
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE suc AS "
            "SELECT NULL::VARCHAR AS id_comercio, NULL::VARCHAR AS id_sucursal, "
            "NULL::VARCHAR AS provincia_iso, NULL::VARCHAR AS localidad, "
            "NULL::VARCHAR AS sucursal_tipo WHERE false"
        )

    # Observaciones validas, deduplicadas por (fecha, comercio, sucursal, producto).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE obs AS
        SELECT
            DATE '{fecha.isoformat()}'                       AS fecha,
            c.id_comercio, c.id_bandera, c.id_sucursal,
            coalesce(p.provincia, '{PROVINCIA_DESCONOCIDA}')  AS provincia,
            s.provincia_iso,
            c.id_producto, c.es_ean, c.descripcion, c.marca,
            c.cantidad_presentacion,
            u.canonica                                        AS unidad_presentacion,
            c.unidad_presentacion_raw,
            c.cantidad_presentacion * u.factor                AS cantidad_base,
            u.base                                            AS unidad_base,
            c.precio_lista,
            c.precio_promo,
            coalesce(c.precio_promo, c.precio_lista)          AS precio_efectivo,
            c.precio_referencia, c.cantidad_referencia, c.unidad_referencia
        FROM clasificado c
        LEFT JOIN suc s
               ON s.id_sucursal = c.id_sucursal
              AND (s.id_comercio = c.id_comercio OR s.id_comercio IS NULL)
        LEFT JOIN dim_provincias p ON p.codigo = s.provincia_iso
        LEFT JOIN dim_unidades   u ON u.codigo = c.unidad_clave
        WHERE c.motivo_rechazo IS NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.id_comercio, c.id_sucursal, c.id_producto
            ORDER BY c._orden DESC
        ) = 1
        """
    )

    validas = con.execute(
        "SELECT count(*) FROM clasificado WHERE motivo_rechazo IS NULL"
    ).fetchone()[0]
    res.observaciones = con.execute("SELECT count(*) FROM obs").fetchone()[0]
    res.duplicados = validas - res.observaciones
    res.unidad_desconocida = con.execute(
        "SELECT count(*) FROM obs WHERE unidad_base IS NULL"
    ).fetchone()[0]
    res.sucursal_sin_provincia = con.execute(
        f"SELECT count(*) FROM obs WHERE provincia = '{PROVINCIA_DESCONOCIDA}'"
    ).fetchone()[0]

    salida_obs = dir_obs / f"{paquete.etiqueta.replace('.zip', '')}.parquet"
    con.execute(
        f"COPY (SELECT * FROM obs) TO '{salida_obs.as_posix()}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    # Rechazados: se guardan con el motivo, nunca se descartan en silencio.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE rech AS
        SELECT DATE '{fecha.isoformat()}' AS fecha,
               id_comercio, id_bandera, id_sucursal, id_producto,
               descripcion, marca, precio_lista_crudo, motivo_rechazo,
               '{paquete.etiqueta}' AS archivo_origen
        FROM clasificado WHERE motivo_rechazo IS NOT NULL
        """
    )
    res.rechazos = con.execute("SELECT count(*) FROM rech").fetchone()[0]
    if res.rechazos:
        salida_rech = dir_rech / f"{paquete.etiqueta.replace('.zip', '')}.parquet"
        con.execute(
            f"COPY (SELECT * FROM rech) TO '{salida_rech.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    return res


def procesar_zip(
    path_zip: Path,
    fecha: date,
    cfg: Config,
    dir_salida: Path,
    unidades: TablaUnidades,
    provincias: MaestroProvincias,
) -> ResumenDia:
    """Procesa un ZIP diario completo a Parquet en `dir_salida`.

    `dir_salida` recibe dos subdirectorios, `observaciones/` y `rechazados/`,
    que se vacian antes de escribir para que reprocesar sea idempotente.
    """
    resumen = ResumenDia(fecha=fecha)
    dir_obs = dir_salida / "observaciones"
    dir_rech = dir_salida / "rechazados"
    for d in (dir_obs, dir_rech):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)

    con = conectar_duckdb(cfg)
    _cargar_tablas_auxiliares(con, unidades, provincias)

    with tempfile.TemporaryDirectory(prefix="sepa_csv_", dir=cfg.tmpdir) as tmp:
        for paquete in lectura.iterar_comercios(path_zip, Path(tmp)):
            if isinstance(paquete, lectura.PaqueteInvalido):
                log(
                    logging.ERROR,
                    "comercio invalido: se saltea",
                    comercio=paquete.etiqueta,
                    motivo=paquete.motivo,
                )
                resumen.comercios_sin_datos += 1
                resumen.detalle.append(
                    ResumenComercio(etiqueta=paquete.etiqueta, error=paquete.motivo)
                )
                continue
            try:
                r = _procesar_comercio(con, paquete, fecha, cfg, dir_obs, dir_rech)
                resumen.comercios_ok += 1
                resumen.observaciones += r.observaciones
                resumen.rechazos += r.rechazos
                resumen.duplicados += r.duplicados
                resumen.unidad_desconocida += r.unidad_desconocida
                resumen.detalle.append(r)
                log(
                    logging.INFO,
                    "comercio procesado",
                    comercio=paquete.etiqueta,
                    filas=r.filas_leidas,
                    observaciones=r.observaciones,
                    rechazos=r.rechazos,
                    duplicados=r.duplicados,
                    unidad_desconocida=r.unidad_desconocida,
                )
            except Exception as exc:  # noqa: BLE001 - un comercio no tumba el dia
                log(
                    logging.ERROR,
                    "fallo el comercio",
                    comercio=paquete.etiqueta,
                    error=f"{type(exc).__name__}: {exc}",
                    exc_info=True,
                )
                resumen.comercios_con_error += 1
                resumen.detalle.append(
                    ResumenComercio(etiqueta=paquete.etiqueta, error=str(exc))
                )
            finally:
                shutil.rmtree(paquete.dir_csv, ignore_errors=True)

    con.close()
    return resumen
