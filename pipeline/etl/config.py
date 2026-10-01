"""Runtime configuration, read once from environment variables."""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class Config:
    es_url: str
    redis_url: str
    data_dir: Path
    out_dir: Path
    stage_dir: Path
    index_name: str
    org_store: str            # "redis" | "lmdb" | "sqlite"
    workers: int              # person-phase worker processes
    loaders: int              # org-phase parser processes
    lookup_batch: int         # persons per org-store lookup
    bulk_bytes: int           # send a _bulk request once its documents reach this many bytes
    in_flight: int            # concurrent _bulk requests per worker
    shards: int
    index_org_arrays: bool    # index organizations.technologies / .keywords
    person_files: int         # use only the first N person files (0 = all); for quick sweeps
    max_retries: int
    retry_backoff_s: float
    progress_interval_s: float

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Config":
        get = env.get
        return cls(
            es_url=get("ES_URL", "http://localhost:9200").rstrip("/"),
            redis_url=get("REDIS_URL", "redis://localhost:6379/0"),
            data_dir=Path(get("DATA_DIR", "/data")),
            out_dir=Path(get("OUT_DIR", "/out")),
            stage_dir=Path(get("STAGE_DIR", "/stage")),
            index_name=get("INDEX_NAME", "persons"),
            org_store=get("ORG_STORE", "redis"),
            workers=int(get("WORKERS", "4")),
            loaders=int(get("LOADERS", "4")),
            lookup_batch=int(get("LOOKUP_BATCH", "500")),
            bulk_bytes=int(get("BULK_BYTES", str(10 * 1024 * 1024))),
            in_flight=int(get("IN_FLIGHT", "2")),
            shards=int(get("SHARDS", "4")),
            index_org_arrays=get("INDEX_ORG_ARRAYS", "0") == "1",
            person_files=int(get("PERSON_FILES", "0")),
            max_retries=int(get("MAX_RETRIES", "8")),
            retry_backoff_s=float(get("RETRY_BACKOFF_S", "0.5")),
            progress_interval_s=float(get("PROGRESS_INTERVAL_S", "5")),
        )

    def as_log_fields(self) -> dict:
        return {k: str(v) if isinstance(v, Path) else v for k, v in asdict(self).items()}
