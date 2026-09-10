"""
Centralized logging configuration module.

Reads LOG_LEVEL and LOG_FORMAT from the environment.

LOG_FORMAT=text (default)
    Plain text: ``{asctime} - {name} - {levelname} - [request_id={...}] {message}``
    Best for local dev where humans tail journalctl.

LOG_FORMAT=json
    One JSON object per line, with the standard fields plus ``request_id``
    when set by the API middleware. Best for production where logs are
    shipped to an aggregator (CloudWatch / Datadog / ELK).

Request-ID propagation:
    ``request_id_var`` is a contextvar set by ``RequestIdMiddleware`` in
    ``src.api.middleware``. ``RequestIdFilter`` reads the contextvar on
    every emit so any logger.info(...) inside an async request handler
    automatically carries the request id. Background tasks that have
    not had the contextvar set will log ``request_id=-`` (text) or omit
    the field (json).

Usage:
    from src.utils import get_logger
    logger = get_logger(__name__)
    logger.info("Application started")
"""

import logging
import os
import uuid
from contextvars import ContextVar
from typing import Optional

from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

# Valid logging levels mapping
VALID_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# Track if logging has been configured
_logging_configured = False

# Request-ID context variable. The API middleware in src.api.middleware
# sets this per-request; the filter below copies it into each log record.
# Default sentinel "-" mirrors common log conventions for "no value".
request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


def new_request_id() -> str:
    """Return a fresh hex UUID for use as a request id."""
    return uuid.uuid4().hex


class RequestIdFilter(logging.Filter):
    """Inject the current request_id contextvar into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


def _build_handler() -> logging.Handler:
    """Return a single StreamHandler configured per LOG_FORMAT.

    Always installs RequestIdFilter so handlers see record.request_id
    even when LOG_FORMAT=text (where %(request_id)s is referenced in
    the format string).
    """
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    handler = logging.StreamHandler()

    if log_format == "json":
        try:
            from pythonjsonlogger import jsonlogger
        except ImportError:  # pragma: no cover - optional dependency
            print(
                "Warning: LOG_FORMAT=json but python-json-logger is not "
                "installed; falling back to text. Run `make install-dev`."
            )
            log_format = "text"
        else:
            # Fields auto-populated by the LogRecord; rename a couple so
            # the output keys are consumer-friendly (timestamp/level vs.
            # asctime/levelname).
            formatter = jsonlogger.JsonFormatter(
                "%(asctime)s %(name)s %(levelname)s %(message)s %(request_id)s",
                rename_fields={"asctime": "timestamp", "levelname": "level"},
            )
            handler.setFormatter(formatter)

    if log_format != "json":
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - [request_id=%(request_id)s] %(message)s"
            )
        )

    handler.addFilter(RequestIdFilter())
    return handler


class _DropExactMessages(logging.Filter):
    """Drop records whose rendered message matches one of a fixed set."""

    def __init__(self, messages):
        super().__init__()
        self._messages = frozenset(messages)

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage() not in self._messages


# uvicorn hands its OWN "uvicorn.error" logger to the websockets server, so
# every connection logs "connection open" then "connection closed" at INFO
# under that name. Measured on the API 2026-09-01: ~3,900 lines/hour, the two
# largest repeated shapes in a 300M journal that held only ~2 hours of history
# -- which is what left the 2026-08-17 post-mortem with no evening to read.
#
# Raising the logger's level is not an option: uvicorn's real errors share it.
# Drop the two messages by name and nothing else. Matching on message text is
# admittedly brittle across uvicorn/websockets versions, but it fails SAFE --
# a renamed message simply comes back, it never silences anything new.
# QUIET_WEBSOCKET_LOGS=false restores them.
_UVICORN_WS_CHATTER = ("connection open", "connection closed")


def _quiet_third_party_chatter() -> None:
    if os.getenv("QUIET_WEBSOCKET_LOGS", "true").strip().lower() == "false":
        return
    ws_logger = logging.getLogger("uvicorn.error")
    # Idempotent: _configure_logging can run more than once per process.
    if any(isinstance(f, _DropExactMessages) for f in ws_logger.filters):
        return
    ws_logger.addFilter(_DropExactMessages(_UVICORN_WS_CHATTER))


def _configure_logging() -> int:
    """
    Configure the root logger with settings from environment variables.
    Called automatically on first logger creation. Idempotent.
    """
    global _logging_configured

    if _logging_configured:
        return logging.getLogger().level

    log_level_str = os.getenv("LOG_LEVEL", "INFO").split("#")[0].strip().upper()
    if log_level_str in VALID_LEVELS:
        log_level = VALID_LEVELS[log_level_str]
    else:
        log_level = logging.INFO
        print(
            f"Warning: Invalid LOG_LEVEL '{log_level_str}', defaulting to INFO. "
            f"Valid options: {', '.join(VALID_LEVELS.keys())}"
        )

    root = logging.getLogger()
    # Replace any existing handlers (e.g. from a prior basicConfig call)
    # so our format + filter take effect even if some other module beat
    # us to configuring root.
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(_build_handler())
    root.setLevel(log_level)

    _quiet_third_party_chatter()

    _logging_configured = True
    return log_level


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a configured logger instance.

    Args:
        name: Logger name, typically __name__ from the calling module.
    """
    _configure_logging()
    return logging.getLogger(name)


def set_log_level(level: str) -> None:
    """Dynamically change the root log level at runtime."""
    level_upper = level.upper()
    if level_upper not in VALID_LEVELS:
        raise ValueError(
            f"Invalid log level '{level}'. Valid options: {', '.join(VALID_LEVELS.keys())}"
        )
    logging.getLogger().setLevel(VALID_LEVELS[level_upper])


# For backward compatibility, provide a default logger
logger = get_logger(__name__)
