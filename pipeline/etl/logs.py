"""JSON-lines logging on stdout through the standard `logging` module.

Modules log with `logging.getLogger(__name__)`; the message is the event name and `extra=` carries
the structured fields: `logger.info("phase_end", extra={"phase": name, "duration_s": 1.2})`.
Spawned processes start with an unconfigured interpreter, so every process entry point calls
`setup_logging`."""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import orjson

# Attributes every LogRecord has; anything else on a record came from `extra=`.
_RECORD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            "logger": record.name,
            "process": record.processName,
        }
        entry.update((k, v) for k, v in vars(record).items() if k not in _RECORD_ATTRS)
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return orjson.dumps(entry, default=str).decode()


def setup_logging(level: str = "INFO") -> None:
    """Routes all loggers, including library ones, to stdout as JSON lines."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
