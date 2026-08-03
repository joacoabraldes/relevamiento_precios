# sepa-downloader

Descarga diaria de los archivos de precios **SEPA** (Sistema Electrónico de Publicidad de
Precios Argentinos) y archivado **crudo** en Google Cloud Storage.

Es la primera etapa de un proyecto de índice de precios: **solo colecta y archiva**. No
parsea CSVs, no calcula índices, no toca bases de datos, no convierte a Parquet.

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
datos ni convertir a Parquet. Todo eso viene después, encima de los datos que este vaya
juntando.
