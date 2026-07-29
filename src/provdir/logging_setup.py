"""Structured logging: JSON lines to output/logs/, human summary to console.

Secrets hygiene: a filter redacts anything that looks like a credential value
appearing in log records (defence-in-depth; we also avoid logging secrets at
the source).
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import OUTPUT_DIR

_LOG_DIR = OUTPUT_DIR / "logs"

# Redact common secret-bearing query params / headers if they ever reach a log.
_REDACT_PATTERNS = [
    re.compile(r"(?i)(client_secret|access_token|api[_-]?key|password|authorization)=([^&\s]+)"),
    re.compile(r"(?i)(bearer)\s+[A-Za-z0-9._\-]+"),
]


def _redact(msg: str) -> str:
    for pat in _REDACT_PATTERNS:
        msg = pat.sub(lambda m: f"{m.group(1)}=***", msg)
    return msg


class _RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Redacting only record.msg misses everything passed as a %-arg, which is
        # where the secrets actually are: call sites log formatted exception text
        # (and FhirError embeds the full request URL) via log.warning("...: %s", exc).
        # Render first, then redact, so both the template and its args are covered.
        if record.args:
            try:
                record.msg = record.getMessage()
                record.args = ()
            except Exception:  # noqa: BLE001 - never let logging break the run
                pass
        if isinstance(record.msg, str):
            record.msg = _redact(record.msg)
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Attach structured extras (anything set via logger.info(..., extra={...})).
        for key, val in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            try:
                json.dumps(val)
            except (TypeError, ValueError):
                val = repr(val)
            payload[key] = val
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


_RESERVED = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "asctime", "taskName"}


class _ConsoleFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[37m",
        "INFO": "\033[36m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[41m",
    }
    RESET = "\033[0m"

    def __init__(self, color: bool) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S")
        self.color = color

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        if self.color:
            c = self.COLORS.get(record.levelname, "")
            return f"{c}{line}{self.RESET}"
        return line


_configured = False


def setup_logging(
    level: int = logging.INFO,
    run_name: Optional[str] = None,
    color: Optional[bool] = None,
) -> Path:
    """Configure root logging. Returns the path of the JSON log file."""
    global _configured
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = f"{run_name + '_' if run_name else ''}{stamp}.jsonl"
    log_path = _LOG_DIR / name

    root = logging.getLogger()
    root.setLevel(level)
    # Clear any prior handlers (idempotent across CLI invocations within a process).
    for h in list(root.handlers):
        root.removeHandler(h)

    redaction = _RedactionFilter()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(_JsonFormatter())
    file_handler.addFilter(redaction)
    root.addHandler(file_handler)

    if color is None:
        color = sys.stderr.isatty()
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(_ConsoleFormatter(color=color))
    console.addFilter(redaction)
    root.addHandler(console)

    # Tame noisy libraries.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    _configured = True
    logging.getLogger(__name__).debug("logging configured", extra={"log_file": str(log_path)})
    return log_path


def get_logger(name: str) -> logging.Logger:
    if not _configured:
        setup_logging()
    return logging.getLogger(name)
