"""Logging estructurado a stdout, compartido por todos los modulos."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

LOG = logging.getLogger("precios")


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
        extra = " " + " ".join(f"{k}={v}" for k, v in campos.items()) if campos else ""
        ts = datetime.now(timezone.utc).strftime("%H:%M:%SZ")
        base = f"{ts} {record.levelname:<7} {record.getMessage()}{extra}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def log(nivel: int, msg: str, **campos: Any) -> None:
    LOG.log(nivel, msg, extra={"campos": campos})


def configurar(nivel: str = "INFO", formato: str = "json") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(FormatoTexto() if formato == "texto" else FormatoJson())
    LOG.handlers[:] = [handler]
    LOG.setLevel(getattr(logging, nivel.upper(), logging.INFO))
    LOG.propagate = False
    for ruidoso in ("httpx", "httpcore", "google", "urllib3"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
