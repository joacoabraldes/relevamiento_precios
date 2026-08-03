FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SEPA_TMPDIR=/tmp

WORKDIR /app

# Certificados raiz para TLS contra datos.produccion.gob.ar.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sepa_downloader.py .

RUN useradd --create-home --uid 1000 sepa
USER sepa

ENTRYPOINT ["python", "/app/sepa_downloader.py"]
