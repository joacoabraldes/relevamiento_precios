# relevamiento-precios

Pipeline de relevamiento de precios de **SEPA** (Sistema Electrónico de Publicidad de
Precios Argentinos) para construir un índice de precios de supermercado.

## Estado

| Etapa | Qué hace | Estado |
|---|---|---|
| **Descarga** — [`sepa_downloader.py`](sepa_downloader.py) | Baja los 7 ZIP diarios y los archiva crudos en `raw/` | ✅ andando |
| **Procesamiento** — [`src/precios/normalize/`](src/precios/normalize/) | ZIP → Parquet normalizado y tipado, en `staged/` | ✅ andando |
| **Clasificación** — [`src/precios/classify/`](src/precios/classify/) | `id_producto → categoría`, piloto de 15 categorías | ✅ piloto andando |
| **Índice mensual** | Jevons + Laspeyres, Postgres | ⏳ pendiente |
| **API + dashboard** | FastAPI, Metabase | ⏳ pendiente |
| **Scraping de precios online** | Fuente distinta de SEPA | ⏳ sin empezar |

> **El índice necesita dos meses de datos acumulados** para producir la primera variación
> mensual. La descarga arrancó el 2026-08-02.

## Estructura

```
sepa_downloader.py           etapa 1: descarga y archivado crudo
src/precios/
  config.py                  configuración por entorno, sin credenciales
  logs.py                    logging estructurado a stdout
  cli.py                     python -m precios.cli {etl,clasificar}
  normalize/                 etapa 2: procesamiento
    lectura.py               apertura de los ZIP anidados de SEPA
    unidades.py              normalización de unidades de medida
    provincias.py            maestro ISO 3166-2 -> nombre
    etl.py                   transformación a Parquet
    gcs.py                   única capa que habla con el bucket
  classify/                  etapa 3: clasificación
    taxonomia.py             carga de categorías y motor de reglas
    proponer.py              propuestas para revisión humana
config/
  unidades.yaml              mapeo de unidades, versionado
  categorias.yaml            taxonomía y reglas, versionado
  mapeo_productos.csv        producto -> categoría, revisado a mano
tests/                       99 tests
```

El núcleo del ETL trabaja siempre con archivos locales; `gcs.py` es lo único que toca la
red. Por eso los tests corren sin credenciales y sin bucket.

---

# Etapa 1 — Descarga y archivo crudo

Descarga diaria de los ZIP de SEPA y archivado **crudo** en GCS (`raw/`). Solo colecta y
archiva: no parsea CSVs, no calcula índices, no toca bases de datos.

---

## Por qué este script tiene que correr todos los días

El dataset **no tiene histórico**. Son exactamente 7 recursos, uno por día de la semana, y
cada uno se **sobrescribe cada 7 días**.

- Lo que no se archiva dentro de esa ventana de 7 días **se pierde para siempre**.
- Por eso el script baja **los 7 archivos todos los días**, no solo el del día: si estuvo
  caído 3 días, al volver recupera todo lo que siga publicado.
- Por eso **falla ruidosamente**: exit code distinto de 0 para que el scheduler lo marque
  como fallido. El silencio es el modo de falla peligroso acá.

### Cuándo se publica cada archivo (importante para elegir el horario)

Verificado contra la API de CKAN: cada recurso se actualiza **el mismo día al que
corresponden los precios**, alrededor de las **16:18 UTC (13:18 ART)**.

Consecuencia práctica: si corrés a las 6 de la mañana, el archivo del día de hoy **todavía
es el de la semana pasada**. No se pierde nada (lo levantás al día siguiente, muy dentro de
la ventana de 7 días), pero los datos más frescos que tenés siempre van a estar un día
atrasados. **Si querés el dato del día lo antes posible, corré después de las 17:00 ART.**
Los ejemplos de scheduling de abajo usan las 17:30 ART.

---

## Qué archiva y dónde

```
gs://outlier-archivos-precios/
├── raw/sepa/minorista/
│   ├── fecha_datos=2026-08-01/
│   │   ├── sepa_sabado.zip
│   │   └── _conflictos/                     # solo si hubo anomalías (ver abajo)
│   │       └── sepa_sabado__20260802T...__a1b2c3d4e5f6.zip
│   └── fecha_datos=2026-07-31/
│       └── sepa_viernes.zip
└── _meta/
    ├── manifest.jsonl                       # una línea JSON por descarga archivada
    ├── estado.json                          # memoria de ETags/hashes vistos
    └── referencia/
        ├── anexo_678_2020.pdf               # diccionario de campos (Res. 678/2020)
        ├── maestro-provincias.xlsx
        └── historico/                       # toda versión distinta queda guardada
            ├── anexo_678_2020__4b9987696401.pdf
            └── maestro-provincias__6098a9de576c.xlsx
```

### `fecha_datos`

Es la fecha a la que **corresponden los precios**, no la fecha en que corrió el script.

Se determina inspeccionando el ZIP, con tres métodos en orden de confiabilidad. El método
usado queda registrado en el campo `metodo_fecha` del manifest:

| `metodo_fecha` | Cómo se obtuvo |
|---|---|
| `zip_dir_nivel1` | El ZIP tiene un único directorio raíz con formato `YYYY-MM-DD`. Es el caso normal. |
| `zip_nombres_internos` | No hay directorio raíz, pero todos los nombres internos (`sepa_1_comercio-sepa-13_2026-08-01_09-05-10.zip`) coinciden en una única fecha. |
| `http_last_modified` | **Fallback.** El ZIP no expone una fecha inequívoca. Se deriva del `Last-Modified` HTTP retrocediendo hasta el día de semana del recurso. Se loguea el listado completo del ZIP a nivel ERROR y el run termina con exit code 2. |

Si el ZIP cae en el fallback, el archivo **igual se sube** (perder datos irrecuperables es
peor que archivarlos con una fecha dudosa, y `metodo_fecha` deja constancia), pero el run
sale con código 2 para que lo revises. Si preferís que directamente **no suba nada** en ese
caso, poné `SEPA_FECHA_ESTRICTA=1`.

También se emiten advertencias (sin frenar) si la fecha del ZIP no cae en el día de semana
del recurso, si es futura, o si tiene más de 8 días de antigüedad.

---

## Idempotencia: cómo evita bajar y guardar dos veces

Se bajan los 7 archivos todos los días, así que la enorme mayoría ya los vamos a tener. El
script chequea en tres niveles, del más barato al más caro:

1. **`HEAD` + ETag.** Si el ETag coincide con el último registrado para ese día de la
   semana, no descarga nada. Este es el caso normal: ~6 de 7 días, cero bytes transferidos.
2. **SHA-256 del contenido.** Si igual hubo que bajarlo pero el hash ya está archivado, no
   sube nada. El ETag puede cambiar sin que cambie el contenido.
3. **Chequeo en destino.** Antes de subir mira si ya hay un objeto en esa ruta.

### Conflictos

Si ya existe un objeto en la ruta destino **con contenido distinto**, el script **no lo
pisa**. Guarda el nuevo en `_conflictos/` con timestamp y hash, loguea a nivel ERROR y sale
con exit code 2. Que dos contenidos distintos reclamen la misma `fecha_datos` es una anomalía
para revisar a mano, no algo para resolver en silencio.

La subida usa `if_generation_match=0`, así que ni siquiera una carrera entre dos ejecuciones
simultáneas puede pisar un objeto existente.

### `_meta/estado.json`

> **Nota de diseño.** Este archivo no estaba en la especificación original; lo agregué porque
> sin él la idempotencia no cierra.
>
> El manifest registra solo descargas que terminaron en **subida**. Pero el paso 2 (dedup por
> SHA-256) justamente **no sube nada**, así que no dejaría rastro. Sin memoria de ese evento,
> el ETag de ese día nunca coincidiría con nada registrado y el script volvería a bajar ~300 MB
> todos los días, para siempre, para descartarlos al final.
>
> `estado.json` guarda el último ETag/hash observado por día de la semana **haya habido subida
> o no**. El manifest queda exactamente como se especificó: append-only, una línea por descarga
> efectivamente archivada.

---

## Manifest

`_meta/manifest.jsonl`, append-only, una línea JSON por descarga archivada:

```json
{"fecha_datos":"2026-08-01","dia_semana":"sabado","resource_id":"b3c3da5d-213d-41e7-8d74-f23fda0a3c30","url":"https://datos.produccion.gob.ar/dataset/6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5/resource/b3c3da5d-213d-41e7-8d74-f23fda0a3c30/download/sepa_sabado.zip","gcs_path":"gs://outlier-archivos-precios/raw/sepa/minorista/fecha_datos=2026-08-01/sepa_sabado.zip","sha256":"...","etag":"\"1785601090.58-235374440\"","last_modified":"Sat, 01 Aug 2026 16:18:10 GMT","bytes":235374440,"descargado_en":"2026-08-02T20:41:03Z","metodo_fecha":"zip_dir_nivel1"}
```

En casos anómalos se agregan `advertencias` (lista de strings) y `conflicto_con` (la ruta
que no se pisó). GCS no soporta append real, así que el archivo se reescribe entero con
precondición de generación: si dos runs escriben a la vez, el segundo relee y reintenta en
lugar de pisar.

---

## Instalación

Requiere **Python 3.11+**.

```bash
git clone <este-repo> && cd relevamiento_precios

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

O como paquete: `pip install -e .` (deja el comando `sepa-downloader` en el PATH).

---

## Autenticación

El script usa **Application Default Credentials**. Funciona sin cambios de código en los
tres escenarios:

```bash
# 1. Local, con tu cuenta de Google
gcloud auth application-default login

# 2. Service account con archivo de credenciales
export GOOGLE_APPLICATION_CREDENTIALS=/ruta/a/sa-key.json

# 3. Cloud Run / GCE / GKE: identidad de la instancia, sin configurar nada
```

### Permisos necesarios

Alcanza con **`roles/storage.objectAdmin`** sobre el bucket. No hace falta
`storage.buckets.get`: el script detecta que no lo tiene y verifica el acceso a nivel objeto.

```bash
gcloud storage buckets add-iam-policy-binding gs://outlier-archivos-precios \
  --member="serviceAccount:sepa-downloader@TU_PROYECTO.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Si el bucket todavía no existe

```bash
gcloud storage buckets create gs://outlier-archivos-precios \
  --project=TU_PROYECTO \
  --location=southamerica-east1 \
  --uniform-bucket-level-access \
  --public-access-prevention

# Muy recomendado para un archivo histórico irrecuperable:
gcloud storage buckets update gs://outlier-archivos-precios --versioning
```

Volumen esperado: ~2 GB/día, ~730 GB/año. Vale la pena una regla de ciclo de vida que mueva
los objetos a Nearline/Coldline después de unos meses; los datos crudos se leen poco una vez
procesados.

---

## Uso manual

```bash
# Corrida normal: los 7 días + archivos de referencia
python sepa_downloader.py

# Ver qué haría sin escribir nada en GCS (igual descarga y valida)
python sepa_downloader.py --dry-run

# Un solo día (acepta con o sin acento: miercoles / miércoles)
python sepa_downloader.py --dia lunes

# Re-bajar y re-subir ignorando manifest y estado
python sepa_downloader.py --force

# Solo los archivos de referencia (PDF del diccionario + maestro de provincias)
python sepa_downloader.py --solo-referencia

# Logs legibles en consola en vez de JSON
SEPA_LOG_FORMATO=texto python sepa_downloader.py
```

### Flags

| Flag | Efecto |
|---|---|
| `--dry-run` | Hace todo (HEAD, descarga, validación de ZIP, detección de fecha) pero no escribe en GCS. |
| `--force` | Ignora manifest y estado: vuelve a bajar y subir todo. |
| `--dia <nombre>` | Procesa un solo día de la semana. |
| `--bucket <nombre>` | Sobrescribe `SEPA_BUCKET`. |
| `--sin-referencia` | No toca los archivos de referencia. |
| `--solo-referencia` | Solo archiva los de referencia y termina. |
| `--sin-verificar-ids` | Saltea la consulta a CKAN que valida los resource IDs. |

### Exit codes

| Código | Significado | Qué hacer |
|---|---|---|
| `0` | Todo bien. | Nada. |
| `1` | Al menos un día **falló**. | **Urgente**: quedan menos de 7 días para recuperarlo. |
| `2` | Sin fallas, pero hay **anomalías**: conflicto de contenido, `fecha_datos` por fallback, o un resource ID que cambió en CKAN. | Revisar, sin apuro inmediato. Los datos se archivaron. |

---

## Configuración por variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `SEPA_BUCKET` | `outlier-archivos-precios` | Bucket destino. |
| `SEPA_PREFIJO_RAW` | `raw/sepa/minorista` | Prefijo de los ZIP diarios. |
| `SEPA_PREFIJO_META` | `_meta` | Prefijo de metadatos. |
| `SEPA_MANIFEST_PATH` | `_meta/manifest.jsonl` | Ruta del manifest. |
| `SEPA_ESTADO_PATH` | `_meta/estado.json` | Ruta del archivo de estado. |
| `SEPA_PREFIJO_REFERENCIA` | `_meta/referencia` | Prefijo de los archivos de referencia. |
| `SEPA_TMPDIR` | temp del sistema | Dónde se bajan los ZIP antes de subirlos. Necesita ~400 MB libres. |
| `SEPA_TIMEOUT_CONEXION` | `30` | Timeout de conexión, en segundos. |
| `SEPA_TIMEOUT_LECTURA` | `1800` | Timeout de lectura/escritura, en segundos. Generoso: son archivos de 200-350 MB. |
| `SEPA_MAX_INTENTOS` | `5` | Intentos por request antes de dar por fallado el día. |
| `SEPA_BACKOFF_BASE` | `4.0` | Base del backoff exponencial, en segundos. |
| `SEPA_BACKOFF_MAX` | `300` | Tope del backoff, en segundos. |
| `SEPA_FECHA_ESTRICTA` | `0` | Con `1`, si `fecha_datos` no se puede sacar del ZIP no sube nada para ese día y lo marca como falla. |
| `SEPA_LOG_NIVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `SEPA_LOG_FORMATO` | `json` | `json` (structured logging a stdout) o `texto` (legible). |

---

## Opción A: cron en una VM

Pensado para una VM chica (e2-small alcanza; lo que importa es el disco y el ancho de banda).

**1. Preparar la VM**

```bash
sudo apt-get update && sudo apt-get install -y python3.11 python3.11-venv git

sudo mkdir -p /opt/sepa && sudo chown "$USER" /opt/sepa
git clone <este-repo> /opt/sepa
cd /opt/sepa

python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

**2. Credenciales**

Si la VM está en GCP, asignale una service account con `roles/storage.objectAdmin` sobre el
bucket y no configures nada más. Si no, dejá la key en `/opt/sepa/sa-key.json` (permisos
`600`) y exportá `GOOGLE_APPLICATION_CREDENTIALS`.

**3. Script de arranque** — `/opt/sepa/run.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd /opt/sepa
export SEPA_BUCKET=outlier-archivos-precios
export SEPA_LOG_FORMATO=json
# export GOOGLE_APPLICATION_CREDENTIALS=/opt/sepa/sa-key.json   # solo fuera de GCP

FECHA="$(date -u +%Y-%m-%d)"
mkdir -p /var/log/sepa

/opt/sepa/.venv/bin/python /opt/sepa/sepa_downloader.py \
  >> "/var/log/sepa/${FECHA}.log" 2>&1
CODIGO=$?

if [ $CODIGO -ne 0 ]; then
  echo "sepa-downloader terminó con código ${CODIGO}" >&2
  tail -40 "/var/log/sepa/${FECHA}.log" >&2
fi

exit $CODIGO
```

```bash
chmod +x /opt/sepa/run.sh
```

**4. Crontab**

```bash
crontab -e
```

```cron
# SEPA: 17:30 ART = 20:30 UTC, después de que se publica el archivo del día.
# Si preferís temprano a la mañana, usá "0 9 * * *" (6:00 ART); en ese caso el
# archivo del día de hoy se levanta recién mañana, lo cual es perfectamente seguro.
MAILTO=tu-mail@ejemplo.com
30 20 * * * /opt/sepa/run.sh
```

Con `MAILTO` configurado y un MTA en la VM, cron te manda mail cuando el script sale con
código distinto de 0. Sin MTA, reemplazá el bloque de error en `run.sh` por una llamada a
tu canal de alertas (webhook de Slack, `curl` a healthchecks.io, lo que uses).

**5. Rotación de logs** — `/etc/logrotate.d/sepa`

```
/var/log/sepa/*.log {
    weekly
    rotate 8
    compress
    missingok
    notifempty
}
```

**6. Verificar**

```bash
/opt/sepa/.venv/bin/python /opt/sepa/sepa_downloader.py --dry-run --dia lunes
```

---

## Opción B: Cloud Run Job + Cloud Scheduler

El `Dockerfile` ya está en el repo.

> **Ojo con la memoria.** En Cloud Run, `/tmp` es un **tmpfs en RAM**: el ZIP temporal de
> ~350 MB cuenta contra la memoria del contenedor. Por eso los comandos de abajo piden
> `--memory=2Gi`. Con el default de 512 MiB el job muere por OOM.

**1. Variables**

```bash
export PROJECT_ID=outlier-474418
export REGION=southamerica-east1
export BUCKET=outlier-archivos-precios
export REPO=sepa
export JOB=sepa-downloader
export SA=sepa-downloader@${PROJECT_ID}.iam.gserviceaccount.com
```

**2. APIs y repositorio de imágenes**

```bash
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  --project="$PROJECT_ID"

gcloud artifacts repositories create "$REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID"
```

**3. Service account**

```bash
gcloud iam service-accounts create sepa-downloader \
  --display-name="SEPA downloader" \
  --project="$PROJECT_ID"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA}" \
  --role="roles/storage.objectAdmin"
```

**4. Build y push**

```bash
export IMAGEN="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${JOB}:latest"

gcloud builds submit --tag "$IMAGEN" --project="$PROJECT_ID"
```

**5. Crear el job**

```bash
gcloud run jobs create "$JOB" \
  --image="$IMAGEN" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --service-account="$SA" \
  --memory=2Gi \
  --cpu=1 \
  --task-timeout=3600s \
  --max-retries=2 \
  --set-env-vars="SEPA_BUCKET=${BUCKET},SEPA_LOG_FORMATO=json,SEPA_TMPDIR=/tmp"
```

Para actualizarlo después de un cambio de código: `gcloud builds submit --tag "$IMAGEN"` y
después `gcloud run jobs update "$JOB" --image="$IMAGEN" --region="$REGION"`.

**6. Probarlo a mano**

```bash
gcloud run jobs execute "$JOB" --region="$REGION" --project="$PROJECT_ID" --wait
```

**7. Cloud Scheduler**

```bash
# Service account que puede disparar el job
gcloud iam service-accounts create sepa-scheduler \
  --display-name="Disparador de SEPA" \
  --project="$PROJECT_ID"

gcloud run jobs add-iam-policy-binding "$JOB" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:sepa-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# 17:30 ART, todos los días
gcloud scheduler jobs create http sepa-diario \
  --location="$REGION" \
  --project="$PROJECT_ID" \
  --schedule="30 17 * * *" \
  --time-zone="America/Argentina/Buenos_Aires" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB}:run" \
  --http-method=POST \
  --oauth-service-account-email="sepa-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
  --attempt-deadline=1800s
```

**8. Alerta cuando falla**

El exit code distinto de 0 marca la ejecución como fallida en Cloud Run. Para enterarte:

```bash
gcloud alpha monitoring policies create \
  --project="$PROJECT_ID" \
  --notification-channels="TU_CANAL_ID" \
  --display-name="SEPA downloader falló" \
  --condition-display-name="Ejecución fallida" \
  --condition-filter='resource.type="cloud_run_job" AND
    metric.type="run.googleapis.com/job/completed_task_attempt_count" AND
    metric.label.result="failed"' \
  --condition-threshold-value=0 \
  --condition-threshold-comparison=COMPARISON_GT \
  --condition-threshold-duration=0s
```

Configurá el canal de notificación en Monitoring → Alerting → Notification channels y usá
su ID. **Vale la pena hacerlo:** la ventana para recuperar un día perdido es de 7 días, y
enterarte tarde significa perder datos que no se pueden recuperar de ninguna forma.

**9. Ver logs**

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="'"$JOB"'"' \
  --project="$PROJECT_ID" --limit=50 --format=json
```

Como el logging es JSON estructurado a stdout, los campos (`dia`, `fecha_datos`, `sha256`,
`metodo_fecha`, `exit_code`) quedan indexados y se pueden filtrar en Logs Explorer.

---

## Monitoreo: qué mirar

Consultas útiles sobre el manifest una vez que hay historia:

```bash
# Bajar el manifest
gcloud storage cat gs://outlier-archivos-precios/_meta/manifest.jsonl > manifest.jsonl

# ¿Qué fechas tenemos archivadas?
jq -r '.fecha_datos' manifest.jsonl | sort -u

# ¿Falta algún día en el último mes? (huecos = datos perdidos para siempre)
jq -r '.fecha_datos' manifest.jsonl | sort -u > tenemos.txt
seq 0 30 | while read -r d; do date -u -d "-${d} days" +%Y-%m-%d; done | sort > esperados.txt
comm -13 tenemos.txt esperados.txt

# ¿Alguna fecha se determinó por fallback?
jq -r 'select(.metodo_fecha != "zip_dir_nivel1") | "\(.fecha_datos) \(.metodo_fecha)"' manifest.jsonl

# ¿Hubo conflictos?
jq -r 'select(.conflicto_con) | "\(.fecha_datos) \(.dia_semana) \(.gcs_path)"' manifest.jsonl
```

---

## Recursos del dataset

Los 9 resource IDs están hardcodeados en [sepa_downloader.py](sepa_downloader.py) y fueron
verificados contra la API de CKAN. **En cada corrida el script los vuelve a verificar** y
avisa a nivel ERROR (con exit code 2) si alguno cambió, así que no hace falta chequearlos a
mano. Se puede saltear con `--sin-verificar-ids`.

| Día | resource_id |
|---|---|
| lunes | `0a9069a9-06e8-4f98-874d-da5578693290` |
| martes | `9dc06241-cc83-44f4-8e25-c9b1636b8bc8` |
| miércoles | `1e92cd42-4f94-4071-a165-62c4cb2ce23c` |
| jueves | `d076720f-a7f0-4af8-b1d6-1b99d5a90c14` |
| viernes | `91bc072a-4726-44a1-85ec-4a8467aad27e` |
| sábado | `b3c3da5d-213d-41e7-8d74-f23fda0a3c30` |
| domingo | `f8e75128-515a-436e-bf8d-5c63a62f2005` |
| diccionario de campos (PDF) | `ace44eb9-c995-463f-bf8a-6f529d196a27` |
| maestro de provincias (XLSX) | `e12edfc0-b0bc-4208-879a-b31b9573324b` |

Dataset: `6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5`
CKAN: <https://datos.produccion.gob.ar/api/3/action/package_show?id=6f47ec76-d1ce-4e34-a7e1-621fe9b1d0b5>

### Notas sobre el servidor de origen

- **No soporta Range requests.** Anuncia `Accept-Ranges: bytes` y devuelve `Content-Range`,
  pero ignora el header `Range` y manda el archivo entero con `200`. **No hay descargas
  reanudables**: si se corta a los 300 MB, el reintento empieza de cero.
- **A veces no manda `Content-Length`.** En `HEAD` el tamaño viene solo en `Content-Range`;
  el script lee los dos.
- El `ETag` tiene forma `"<mtime>.<size>"`, o sea que cambia cada vez que se republica el
  archivo aunque el contenido sea idéntico. De ahí el segundo nivel de dedup por SHA-256.

---

## Alcance

Este script **solo baja y archiva**. Nada de parsear CSVs, calcular índices, tocar bases de
datos ni convertir a Parquet. Eso lo hace la etapa siguiente.

---
---

# Etapa 2 — Procesamiento diario: ZIP → Parquet

Toma los ZIP archivados en `raw/` y produce Parquet normalizado y tipado en `staged/`.
Una fila de salida = **un producto, en una sucursal, en un día**.

```bash
# todos los días disponibles en raw/ que todavía no se procesaron
python -m precios.cli etl

# un día puntual
python -m precios.cli etl --fecha 2026-08-01

# un rango hacia atrás (para reprocesar tras un cambio de lógica)
python -m precios.cli etl --desde 2026-07-27 --hasta 2026-08-02 --forzar

# sin escribir en GCS
python -m precios.cli etl --fecha 2026-08-01 --salida-local ./salida
python -m precios.cli etl --dry-run
```

**Rendimiento medido:** ~10,4 millones de observaciones por día en ~2,5 minutos,
con DuckDB y un límite de 4 GB de memoria.

## Salida

```
staged/observaciones/anio=YYYY/mes=MM/dia=DD/<comercio>.parquet
staged/rechazados/anio=YYYY/mes=MM/dia=DD/<comercio>.parquet
```

Un Parquet por comercio dentro de cada partición (ZSTD). Reprocesar un día **vacía la
partición y la reescribe**: es idempotente por diseño, al revés que el archivo crudo, que nunca
se pisa. El procesado siempre tiene que poder reconstruirse desde el crudo.

### Schema de `observaciones`

| Columna | Tipo | Nota |
|---|---|---|
| `fecha` | DATE | sale de la partición de `raw/`, no del CSV |
| `id_comercio`, `id_bandera`, `id_sucursal` | VARCHAR | |
| `provincia` | VARCHAR | decodificada con el maestro; `DESCONOCIDA` si falta |
| `provincia_iso` | VARCHAR | código crudo ISO 3166-2 (`AR-B`) |
| `id_producto` | VARCHAR | **nunca numérico**: `0000000060257` pierde los ceros |
| `es_ean` | BOOLEAN | `productos_ean='1'`; 98,96% en datos reales |
| `descripcion`, `marca` | VARCHAR | trim + colapso de espacios + upper |
| `cantidad_presentacion` | DOUBLE | |
| `unidad_presentacion` | VARCHAR | canónica (`l`, `ml`, `kg`, `un`…) |
| `unidad_presentacion_raw` | VARCHAR | lo que informó el comercio, para auditar |
| `cantidad_base` | DOUBLE | convertida a la unidad base |
| `unidad_base` | VARCHAR | `kg`, `l`, `un`, `m`, `m2` |
| `precio_lista` | DOUBLE | |
| `precio_promo` | DOUBLE | `promo1`; nulo en el 96,9% de los casos |
| `precio_efectivo` | DOUBLE | `coalesce(promo, lista)` — la segunda serie |
| `precio_referencia`, `cantidad_referencia`, `unidad_referencia` | | precio por unidad de medida |

**`cantidad_base` es lo que va a habilitar la clasificación.** Un producto de `1 L`, uno de
`1000 ML` y uno de `1 LT` colapsan los tres a `cantidad_base=1.0, unidad_base='l'`, así que
"leche entera sachet 1 litro" puede agrupar las tres variantes sin reglas por comercio.

## Correcciones al diseño original

Tres cosas del spec original resultaron incorrectas al contrastarlas contra el Anexo II y
los datos reales:

**1. No existe el campo `ean`.** El identificador es `id_producto`; `productos_ean` es un
flag 1/0 que indica si ese código es un EAN/GTIN real o un código interno del comercio.

**2. La clave tiene que incluir `id_comercio`.** `id_sucursal` es un código interno de cada
comercio y **no es único globalmente** — la sucursal "1" existe en varios comercios a la vez.
Medido sobre datos reales:

```
(id_sucursal, id_producto)                → 33.146 claves duplicadas
(id_comercio, id_sucursal, id_producto)   → 0 duplicadas
```

Con la clave sin `id_comercio`, productos de cadenas distintas se colapsan en un mismo
quote y el ratio `precio_t / precio_{t-1}` compara cosas que no tienen relación. Es un bug
silencioso: no rompe nada, solo devuelve un índice equivocado.

**3. `precio_unitario_referencia` son tres campos, no uno:** `productos_precio_referencia`,
`productos_cantidad_referencia` y `productos_unidad_medida_referencia`.

## Rarezas del formato que el ETL absorbe

Ninguna de estas es lo default de un CSV, y todas están cubiertas por tests:

- **Separador pipe `|`**, no coma.
- **BOM inconsistente**: 2 de cada 3 comercios lo mandan. Se lee con `utf-8-sig`.
- **Fin de línea mezclado**: `\r\n` en unos comercios, `\n` en otros.
- **Línea de pie**: el archivo termina con una línea en blanco y
  `Última actualización: <ISO>` — que un comercio escribe `Ultima`, sin tilde. No es dato.
  Se descarta **explícitamente por patrón**, no con `ignore_errors`, que también se tragaría
  las filas genuinamente corruptas.
- **Sin autodetección de dialecto**: el sniffer de DuckDB muestrea las primeras filas, en
  archivos chicos toma la línea de pie y concluye que hay una sola columna. El dialecto se
  declara entero (`auto_detect=false`).
- **Unidades libres**: el Anexo especifica `l, ml, kg, gr, unidad`, pero los comercios
  mandan más de 30 variantes. Buena parte resultaron ser códigos **UN/CEFACT Rec. 20**
  (`KGM`, `GRM`, `LTR`, `CMQ`, `DMQ`, `CMT`, `EA`) porque exportan desde ERPs que usan ese
  estándar. El mapeo vive en [config/unidades.yaml](config/unidades.yaml) y es versionado.
- **Marcas truncadas**: algunos comercios cortan `productos_marca` a 5 caracteres
  (`LA SE`, `TREGA`). No es un bug del ETL, viene así del origen. La clasificación de la
  Etapa 2 va a tener que apoyarse en `descripcion`, no en `marca`.
- **Comercios que no publican**: el 2026-08-01, `comercio-sepa-36` subió un ZIP de 0 bytes.
  Un comercio roto nunca interrumpe el día: se registra y se sigue.

## Nada se descarta en silencio

El filtro duro es `precio_lista > 0` — un precio 0 no es un precio bajo, es un dato ausente
disfrazado. Todo lo que no pasa va a `rechazados/` con su motivo:

| `motivo_rechazo` | |
|---|---|
| `sin_id_producto` / `sin_id_sucursal` | falta la clave |
| `precio_lista_nulo_o_vacio` | campo vacío |
| `precio_lista_no_numerico` | no parsea ni con separadores mixtos |
| `precio_lista_no_positivo` | `<= 0` |

Hay un test que verifica la identidad `observaciones + rechazados + duplicados = filas del
archivo`, así que ninguna fila puede desaparecer sin dejar rastro.

Una **unidad desconocida no es motivo de rechazo**: se conserva la observación con
`cantidad_base` nula y se cuenta aparte en el resumen. Tirar un precio válido porque no
entendemos su unidad sería peor que el problema.

## Exit codes

| Código | Significado |
|---|---|
| `0` | Todo bien. |
| `1` | Falló el procesamiento de algún comercio o día. **Es un bug nuestro.** |
| `2` | Algún comercio no publicó datos usables en el origen (ZIP vacío o corrupto). Es un problema de la fuente, pasa seguido, y se separa a propósito del código 1 para que no genere fatiga de alertas. |

## Variables de entorno

| Variable | Default |
|---|---|
| `PRECIOS_BUCKET` | `outlier-archivos-precios` |
| `PRECIOS_PREFIJO_RAW` | `raw/sepa/minorista` |
| `PRECIOS_PREFIJO_STAGED` | `staged` |
| `PRECIOS_PATH_UNIDADES` | `config/unidades.yaml` |
| `PRECIOS_MEMORIA_DUCKDB` | `4GB` |
| `PRECIOS_HILOS_DUCKDB` | `4` |
| `PRECIOS_PRECIO_MINIMO` | `0` |
| `PRECIOS_LOG_FORMATO` | `json` (o `texto`) |

## Tests

```bash
pip install -e ".[dev]"
pytest
```

61 tests sobre ZIP sintéticos que reproducen todas las rarezas de arriba. Los más
importantes:

- `test_mismo_producto_y_sucursal_en_comercios_distintos_no_se_mezclan` — el bug de la clave.
- `test_nada_se_descarta_en_silencio` — la identidad de conteo.
- `test_reprocesar_es_idempotente` — dos corridas dan byte a byte lo mismo.
- `test_clave_sql_coincide_con_python` — la normalización de unidades existe en Python y en
  SQL; si se desincronizan se pierde el 15% de las filas sin ruido. Pasó de verdad.

---
---

# Etapa 3 — Clasificación de productos

Agrupa productos sustituibles entre sí en **categorías elementales**. Es lo que convierte
70 mil descripciones sueltas en canastas comparables, y sin esto no hay índice posible.

```bash
# propone asignaciones sobre un día ya procesado
python -m precios.cli clasificar --fecha 2026-07-30

# o contra Parquet locales
python -m precios.cli clasificar --origen-local ./dia30
```

## El principio: propone la máquina, decide una persona

La clasificación **no se resuelve en runtime**. El proceso propone candidatos y el
resultado queda en un archivo versionado:

| Archivo | Qué es |
|---|---|
| [config/categorias.yaml](config/categorias.yaml) | Taxonomía y reglas. **Fuente de verdad.** |
| `config/mapeo_productos.csv` | Mapeo `id_producto → categoría`, versionado. Se revisa a mano; la columna `revisado` arranca en `no`. |
| `salida/revisar_ambiguos.csv` | Productos que matchearon más de una categoría. |
| `salida/sin_clasificar_top.csv` | Los productos de más volumen que ninguna regla tocó, para decidir qué categoría agregar después. |

Si un producto cambia de categoría, tiene que verse en un diff de git. Nada de similitud
semántica ni modelos: el criterio tiene que poder explicarse en una línea.

## Taxonomía

Jerarquía estilo COICOP: `división → grupo → clase → categoría elemental`. El piloto son
**15 categorías elementales** de lácteos, almacén y bebidas, sobre 6 clases COICOP.

## Cómo se evalúa una regla

```yaml
almacen.arroz_largo_fino_1kg:
  clase: "01.1.1"
  patrones: ['\bARROZ\b', '\bLARG', '\bFINO\b']    # TODOS deben aparecer
  excluye: ['PARBOIL', '\bBARRA\b', 'ALFAJOR', ...] # NINGUNO puede aparecer
  unidad_base: kg
  cantidad_min: 0.9
  cantidad_max: 1.1
```

**La presentación hace la mitad del trabajo.** Exigir 1 kg descarta sola las barritas de
arroz de 20 g. Para eso sirve el `cantidad_base` que normaliza la Etapa 2: un producto de
"1 L", otro de "1000 ML" y otro de "1 LT" son el mismo para las reglas.

### Un EAN es un producto físico, no una descripción

Ésta es la decisión de diseño central. El mismo EAN viene descripto distinto en cada
cadena, así que las reglas se evalúan sobre **todas** sus descripciones a la vez, con una
asimetría deliberada:

| | Criterio | Por qué |
|---|---|---|
| **Exclusiones** | alcanza con que **una** variante la dispare | Ver "PARBOIL" una vez es evidencia fuerte de qué producto es |
| **Inclusiones** | alcanza con que **una** variante cumpla todos los patrones | Las descripciones vienen truncadas a ~20 caracteres; una cadena con el nombre completo rescata al resto |
| **Presentación** | alcanza con que **una** variante entre en el rango | Hay cadenas que informan "1 unidad" y pierden el peso real |

Los patrones de inclusión deben cumplirse **dentro de una misma variante**: si no, "ARROZ"
de una descripción y "LARGO FINO" de otra se combinarían en un match que ninguna justifica.

Esta asimetría salió de casos reales. El EAN `7791120037559` es arroz parboil, pero una
cadena lo describe `ARROZ ALA DORADO LARGO FINO`: evaluando cada descripción por separado
entraba como largo fino y contaminaba esa categoría.

## Resultado del piloto (2026-07-30)

| | |
|---|---|
| Productos en el catálogo (con EAN) | 79.901 |
| Productos clasificados | **987** |
| Observaciones cubiertas | **449.518** (3,07%) |
| Ambigüedades sin resolver | 0 |

La cobertura baja es **esperada y correcta**: son 15 categorías sobre un universo de 70 mil
productos que incluye electrodomésticos, perfumería y librería. Lo que importa en esta
etapa es la **precisión**, no el volumen: un producto mal clasificado mete ruido en el
índice para siempre, uno sin clasificar simplemente no participa.

## Falsos positivos que hubo que cazar

Buscar por palabra suelta trae basura. Todos éstos son casos reales del dataset y cada uno
tiene su test de regresión:

| Producto | Entraba como | Por qué |
|---|---|---|
| `PALMERITAS DE MANTECA` | manteca | Repostería que lleva manteca en el nombre |
| `ARROZ PARBOIL` | arroz largo fino | Una cadena lo describe como "largo fino" |
| `HARINA 0000 P/PIZZA` | harina común | Es otro producto a otro precio |
| `BARRA ARROZ TRADICIO` | arroz | Descartado por presentación (60 g) |
| `GALLETAS AZUCARADAS` | azúcar | Exclusión por `GALLET` |
| `CREMA DE LECHE` | leche entera | Exclusión por `\bCREMA\b` |

Y un caso de precisión fina: `\b000\b` **no** matchea `HARINA 0000`. Sin los límites de
palabra, las dos categorías de harina se solapaban.

## Tests

27 tests específicos de clasificación, además de los 72 del resto del pipeline. Los que
más importan:

- `test_exclusion_en_una_variante_descarta_el_producto` — el caso del arroz parboil.
- `test_una_variante_completa_rescata_a_las_truncadas` — descripciones cortadas.
- `test_los_patrones_deben_cumplirse_en_una_misma_variante` — no ensamblar patrones entre descripciones distintas.
- `test_harina_000_no_matchea_0000` — el límite de palabra.
- `test_la_taxonomia_del_repo_no_tiene_solapamientos` — ningún producto real cae en dos categorías.

## Informantes excluidos

SEPA obliga a informar a todo comercio de consumo masivo, no solo a supermercados. Tres de
los 18 informantes no tienen surtido comparable y quedan fuera del análisis, según
[config/comercios.yaml](config/comercios.yaml):

| Informante | Productos distintos | Motivo |
|---|---|---|
| Farmacity / Simplicity | 1.952 | Farmacia |
| Axion Energy | 177 | Tienda de estación de servicio |
| Estación Lima | 259 | Tienda de estación de servicio, 1 sucursal |

Para comparar: una cadena de supermercados informa entre 10.000 y 37.000 productos. Con
177 productos no se sostienen quotes mes a mes en ninguna categoría.

**La exclusión no toca `raw/` ni `staged/`**: el archivo crudo y el procesado siguen
guardando a todos los informantes siempre. Se aplica recién en la capa de análisis, así que
revertirla es borrar una línea del YAML y reprocesar, sin volver a descargar nada.
