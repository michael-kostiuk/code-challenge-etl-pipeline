"""Streaming reader for gzipped NDJSON envelopes: {id, date_updated, serialized_data}."""
from __future__ import annotations

import gzip
from pathlib import Path
from typing import Iterator

import orjson

from etl.deadletter import DeadLetter
from etl.metrics import Counters


class MalformedRecord(ValueError):
    pass


def read_lines(path: Path) -> Iterator[tuple[int, bytes]]:
    """Yields (1-based line number, raw line) for every non-blank line; constant memory."""
    with gzip.open(path, "rb") as fh:
        for lineno, line in enumerate(fh, start=1):
            if line.strip():
                yield lineno, line


def parse_envelope(line: bytes) -> tuple[int, dict]:
    """Returns (forager_id, serialized_data) or raises MalformedRecord."""
    try:
        envelope = orjson.loads(line)
    except orjson.JSONDecodeError as err:
        raise MalformedRecord(f"invalid JSON: {err}") from None
    data = envelope.get("serialized_data") if isinstance(envelope, dict) else None
    if not isinstance(data, dict):
        raise MalformedRecord("missing serialized_data object")
    forager_id = data.get("forager_id")
    if not isinstance(forager_id, int) or isinstance(forager_id, bool):
        raise MalformedRecord("missing integer serialized_data.forager_id")
    return forager_id, data


def read_records(files: list[Path], kind: str, dead_letter: DeadLetter, counters: Counters) -> Iterator[tuple[int, dict]]:
    """Yields (forager_id, serialized_data) for every well-formed line of `files`; malformed lines
    are dead-lettered as `kind` and counted."""
    for path in files:
        for lineno, line in read_lines(path):
            try:
                yield parse_envelope(line)
            except MalformedRecord as err:
                dead_letter.write(kind, file=path.name, line=lineno, error=str(err),
                                  raw=line[:1000].decode("utf-8", "replace"))
                counters.add("malformed")
