"""CLI del pipeline. Etapa 2 (procesamiento): `python -m precios.cli etl`."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

from .config import Config
from .logs import configurar, log
from .normalize import etl as etl_mod
from .normalize.provincias import MaestroProvincias
from .normalize.unidades import TablaUnidades

RUTA_MAESTRO_BUCKET = "_meta/referencia/maestro-provincias.xlsx"


def _rango(desde: date, hasta: date) -> list[date]:
    return [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]


def _fechas_pedidas(args: argparse.Namespace, disponibles: dict) -> list[date]:
    if args.fecha:
        return [date.fromisoformat(args.fecha)]
    if args.desde or args.hasta:
        d = date.fromisoformat(args.desde) if args.desde else min(disponibles)
        h = date.fromisoformat(args.hasta) if args.hasta else max(disponibles)
        return _rango(d, h)
    return sorted(disponibles)


def comando_etl(args: argparse.Namespace, cfg: Config) -> int:
    from .normalize.gcs import ClienteBucket, zips_disponibles

    unidades = TablaUnidades.desde_yaml(cfg.path_unidades)
    log(logging.INFO, "tabla de unidades cargada", codigos=len(unidades))

    cliente = ClienteBucket(cfg)

    # El maestro de provincias vive en el bucket; se cachea local.
    if not cfg.path_provincias.is_file():
        cfg.path_provincias.parent.mkdir(parents=True, exist_ok=True)
        if not cliente.bajar(RUTA_MAESTRO_BUCKET, cfg.path_provincias):
            log(logging.CRITICAL, "no se encontro el maestro de provincias",
                ruta=RUTA_MAESTRO_BUCKET)
            return 1
    provincias = MaestroProvincias.desde_xlsx(cfg.path_provincias)
    log(logging.INFO, "maestro de provincias cargado", jurisdicciones=len(provincias))

    disponibles = zips_disponibles(cliente, cfg)
    if not disponibles:
        log(logging.CRITICAL, "no hay ZIP archivados en raw/")
        return 1
    log(
        logging.INFO,
        "archivo crudo disponible",
        dias=len(disponibles),
        desde=str(min(disponibles)),
        hasta=str(max(disponibles)),
    )

    fechas = _fechas_pedidas(args, disponibles)
    resultados: list[tuple[date, str, etl_mod.ResumenDia | None]] = []

    for fecha in fechas:
        ruta_zip = disponibles.get(fecha)
        if ruta_zip is None:
            log(logging.WARNING, "sin ZIP en raw/ para esa fecha", fecha=str(fecha))
            resultados.append((fecha, "sin_origen", None))
            continue

        prefijo_obs = cfg.particion("observaciones", fecha)
        if not args.forzar and not args.salida_local and cliente.listar(prefijo_obs):
            log(logging.INFO, "particion ya procesada: se saltea", fecha=str(fecha))
            resultados.append((fecha, "ya_procesada", None))
            continue

        tmp = Path(tempfile.mkdtemp(prefix="sepa_etl_", dir=cfg.tmpdir))
        try:
            local_zip = tmp / Path(ruta_zip).name
            log(logging.INFO, "bajando ZIP", fecha=str(fecha), ruta=ruta_zip)
            if not cliente.bajar(ruta_zip, local_zip):
                log(logging.ERROR, "el ZIP no existe en el bucket", ruta=ruta_zip)
                resultados.append((fecha, "sin_origen", None))
                continue

            destino = (
                Path(args.salida_local) / f"fecha={fecha.isoformat()}"
                if args.salida_local
                else tmp / "salida"
            )
            resumen = etl_mod.procesar_zip(
                local_zip, fecha, cfg, destino, unidades, provincias
            )

            if args.dry_run:
                log(logging.INFO, "[dry-run] no se sube nada", fecha=str(fecha))
            elif not args.salida_local:
                for tabla in ("observaciones", "rechazados"):
                    prefijo = cfg.particion(tabla, fecha)
                    cliente.borrar_prefijo(prefijo)
                    n = cliente.subir_directorio(destino / tabla, prefijo)
                    log(logging.INFO, "particion subida", tabla=tabla,
                        prefijo=prefijo, archivos=n)

            if resumen.hubo_fallas:
                estado = "fallo"
            elif resumen.hubo_anomalias:
                estado = "ok_con_avisos"
            else:
                estado = "ok"
            resultados.append((fecha, estado, resumen))
        except Exception as exc:  # noqa: BLE001 - un dia no tumba el rango
            log(logging.ERROR, "fallo el dia", fecha=str(fecha),
                error=f"{type(exc).__name__}: {exc}", exc_info=True)
            resultados.append((fecha, "error", None))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    return _resumen(resultados, cfg, args)


def _resumen(resultados, cfg: Config, args: argparse.Namespace) -> int:
    ancho = 98
    print("", flush=True)
    print("=" * ancho)
    print(
        f"ETL SEPA -> gs://{cfg.bucket}/{cfg.prefijo_staged}"
        + ("  [DRY-RUN]" if args.dry_run else "")
        + (f"  [LOCAL: {args.salida_local}]" if args.salida_local else "")
    )
    print("=" * ancho)
    print(f"{'FECHA':<12} {'ESTADO':<14} {'OBSERVAC.':>12} {'RECHAZ.':>9} "
          f"{'DUPLIC.':>8} {'SIN UNID.':>10} {'COMERCIOS':>10}")
    print("-" * ancho)
    tot_obs = tot_rech = tot_dup = tot_sin = 0
    fallas = anomalias = 0
    for fecha, estado, r in resultados:
        if r is None:
            print(f"{fecha.isoformat():<12} {estado:<14}")
            if estado in ("error", "sin_origen"):
                fallas += 1
            continue
        comercios = f"{r.comercios_ok}/{r.comercios_totales}"
        print(f"{fecha.isoformat():<12} {estado:<14} {r.observaciones:>12,} "
              f"{r.rechazos:>9,} {r.duplicados:>8,} {r.unidad_desconocida:>10,} "
              f"{comercios:>10}")
        tot_obs += r.observaciones
        tot_rech += r.rechazos
        tot_dup += r.duplicados
        tot_sin += r.unidad_desconocida
        if r.hubo_fallas:
            fallas += 1
        if r.hubo_anomalias:
            anomalias += 1
        for d in r.detalle:
            if d.error:
                print(f"{'':<12} {'!':<14} {d.etiqueta}: {d.error[:58]}")
    print("-" * ancho)
    print(f"{'TOTAL':<12} {'':<14} {tot_obs:>12,} {tot_rech:>9,} "
          f"{tot_dup:>8,} {tot_sin:>10,}")
    print("=" * ancho, flush=True)

    if fallas:
        log(logging.ERROR, "fin con fallas", dias_con_falla=fallas, exit_code=1)
        return 1
    if anomalias:
        # Un comercio que no publico no es un bug nuestro, pero hay que verlo.
        log(logging.WARNING, "fin con avisos: hubo comercios sin datos en el origen",
            dias_con_avisos=anomalias, exit_code=2)
        return 2
    log(logging.INFO, "fin ok", exit_code=0)
    return 0


def comando_clasificar(args: argparse.Namespace, cfg: Config) -> int:
    """Etapa 3: propone asignaciones producto -> categoria para revision humana."""
    from .classify import proponer as prop_mod
    from .classify.taxonomia import Taxonomia
    from .comercios import ListaComercios

    taxonomia = Taxonomia.desde_yaml(cfg.path_categorias)
    log(logging.INFO, "taxonomia cargada", categorias=len(taxonomia),
        clases=len(taxonomia.clases))

    comercios = ListaComercios.desde_yaml(cfg.path_comercios)
    if len(comercios):
        for i in comercios.ids:
            d = comercios.detalle(i)
            log(logging.INFO, "informante excluido del analisis",
                id_comercio=i, nombre=d.nombre if d else "", motivo=d.motivo if d else "")

    tmp: Path | None = None
    try:
        if args.origen_local:
            glob = f"{Path(args.origen_local).as_posix()}/*.parquet"
        else:
            from .normalize.gcs import ClienteBucket

            cliente = ClienteBucket(cfg)
            fecha = date.fromisoformat(args.fecha)
            prefijo = cfg.particion("observaciones", fecha)
            nombres = [n for n in cliente.listar(prefijo) if n.endswith(".parquet")]
            if not nombres:
                log(logging.CRITICAL, "no hay datos procesados para esa fecha",
                    fecha=args.fecha, prefijo=prefijo)
                return 1
            tmp = Path(tempfile.mkdtemp(prefix="sepa_clas_", dir=cfg.tmpdir))
            log(logging.INFO, "bajando particion", fecha=args.fecha, archivos=len(nombres))
            for n in nombres:
                cliente.bajar(n, tmp / Path(n).name)
            glob = f"{tmp.as_posix()}/*.parquet"

        catalogo = prop_mod.construir_catalogo(glob, cfg.memoria_duckdb, comercios)
        log(logging.INFO, "catalogo construido", productos=len(catalogo))

        propuestas, resumen = prop_mod.proponer(catalogo, taxonomia)
        prop_mod.log_resumen(resumen, taxonomia)

        dir_salida = Path(args.salida)
        n_mapeo = prop_mod.escribir_mapeo(propuestas, cfg.path_mapeo)
        n_rev = prop_mod.escribir_revision(propuestas, dir_salida / "revisar_ambiguos.csv")
        n_sin = prop_mod.escribir_sin_clasificar(
            catalogo, propuestas, dir_salida / "sin_clasificar_top.csv"
        )
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)

    ancho = 92
    print("", flush=True)
    print("=" * ancho)
    print(f"CLASIFICACION PROPUESTA — {len(taxonomia)} categorias piloto")
    print("=" * ancho)
    print(f"{'CATEGORIA':<38} {'PRODUCTOS':>10} {'OBSERVACIONES':>15}")
    print("-" * ancho)
    for cod in sorted(resumen.por_categoria, key=lambda c: -resumen.por_categoria[c][1]):
        n_p, n_o = resumen.por_categoria[cod]
        print(f"{cod:<38} {n_p:>10,} {n_o:>15,}")
    vacias = [r.codigo for r in taxonomia.reglas if r.codigo not in resumen.por_categoria]
    for cod in vacias:
        print(f"{cod:<38} {0:>10} {0:>15}   <-- sin productos")
    print("-" * ancho)
    print(f"{'TOTAL ASIGNADO':<38} {resumen.productos_asignados:>10,} "
          f"{resumen.obs_asignadas:>15,}")
    print("-" * ancho)
    print(f"productos en el catalogo (con EAN): {resumen.productos_totales:,}")
    print(f"cobertura: {resumen.cobertura_productos:.2f}% de los productos, "
          f"{resumen.cobertura_obs:.2f}% de las observaciones")
    print(f"ambiguos (reglas que se pisan):     {resumen.ambiguos_entre_categorias:,}")
    print(f"ambiguos (cadenas que discrepan):   {resumen.ambiguos_entre_variantes:,}")
    print("")
    print(f"mapeo versionable  -> {cfg.path_mapeo}  ({n_mapeo:,} filas)")
    print(f"para revisar       -> {dir_salida / 'revisar_ambiguos.csv'}  ({n_rev:,} filas)")
    print(f"sin clasificar     -> {dir_salida / 'sin_clasificar_top.csv'}  ({n_sin:,} productos)")
    print("=" * ancho, flush=True)

    if vacias:
        log(logging.WARNING, "hay categorias sin ningun producto", categorias=vacias,
            exit_code=2)
        return 2
    log(logging.INFO, "fin ok", exit_code=0)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="precios", description="Pipeline de precios SEPA")
    sub = p.add_subparsers(dest="comando", required=True)

    e = sub.add_parser("etl", help="Etapa 2: ZIP crudo -> Parquet normalizado")
    e.add_argument("--fecha", help="procesa una sola fecha (YYYY-MM-DD)")
    e.add_argument("--desde", help="inicio del rango (YYYY-MM-DD)")
    e.add_argument("--hasta", help="fin del rango (YYYY-MM-DD)")
    e.add_argument("--forzar", action="store_true",
                   help="reprocesa aunque la particion ya exista")
    e.add_argument("--dry-run", action="store_true",
                   help="procesa pero no escribe en GCS")
    e.add_argument("--salida-local", help="escribe el Parquet en este directorio local")

    c = sub.add_parser("clasificar",
                       help="Etapa 3: propone producto -> categoria para revision")
    c.add_argument("--fecha", help="fecha procesada a usar como base (YYYY-MM-DD)")
    c.add_argument("--origen-local", help="directorio local con los Parquet, en vez de GCS")
    c.add_argument("--salida", default="salida",
                   help="directorio para los reportes de revision (default: salida/)")

    args = p.parse_args(argv)
    cfg = Config.desde_entorno()
    configurar(cfg.log_nivel, cfg.log_formato)

    if args.comando == "etl":
        return comando_etl(args, cfg)
    if args.comando == "clasificar":
        if not args.fecha and not args.origen_local:
            p.error("clasificar necesita --fecha o --origen-local")
        return comando_clasificar(args, cfg)
    return 2


if __name__ == "__main__":
    sys.exit(main())
