# ETL Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream 1M persons and 529K orgs from gzipped NDJSON, join them through a pluggable org store, and bulk-index one document per person into the Elasticsearch index `persons` as fast as possible within the pipeline's 2 GB / 4 CPU limits.

**Architecture:** Phase 1 parses org files in 4 processes into an external org store (Redis, LMDB or SQLite, selected by `ORG_STORE`). Phase 2 runs 4 spawned worker processes. Each runs a uvloop event loop that parses person batches, prefetches their orgs, splices org JSON bytes into documents with `orjson.Fragment`, and POSTs raw NDJSON `_bulk` bodies with bounded concurrency. The parent process sets up and finalizes the index, aggregates shared-memory counters into JSON-lines progress logs, verifies the final count and writes `out/metrics.json`.

**Tech Stack:** Python 3.12, orjson, aiohttp, uvloop, redis-py, py-lmdb, sqlite3 (stdlib), elasticsearch-py 8.13 (index admin only), pytest, Docker Compose, Elasticsearch 8.13.4, Redis 7.2.

**Spec:** `docs/superpowers/specs/2026-10-01-etl-pipeline-design.md`

## Global Constraints

- `pipeline` service keeps `mem_limit: 2g` and `cpus: 4.0`. Never raise them.
- Do not modify `bench/correctness.py` or `bench/expected.json`.
- Everything runs from `docker compose up` on a fresh machine with only Docker. No preprocessing outside the pipeline.
- Index name is `persons`. Document `_id` = `forager_id`.
- `roles` and `organizations` are mapped as `object`, never `nested`. `roles.role_title` and `organizations.name` have a `.keyword` subfield with `ignore_above: 256`.
- Mapping root is `dynamic: false`. Every input field stays in `_source`.
- `organizations[]` holds full org-feed records. Input `serialized_data.organizations` moves to `affiliations`. Unresolved org IDs are flagged, never dropped.
- Tests follow `.claude/skills/principle-test-behavior-not-implementation/SKILL.md`: call the code as its users do and assert literal expected values. No call assertions, no constant pins.
- All Python runs inside the pipeline container: `docker compose run --rm pipeline ...` (`./pipeline` is bind-mounted at `/app`, so code edits need no rebuild; dependency changes need `docker compose build pipeline`).
- Keep the `EVALUATION.md` Trade-offs section current: one terse bullet per decision, numbers over prose, replace `_TBD_` once measured.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Work on branch `feature/etl-pipeline`.

## Review Focus

1. **Re-running on a used environment** (index and org store already populated from a previous run). The result must be identical to a fresh run: same count, no stale orgs, no duplicates. Pinned by the end-to-end test running the pipeline twice (Task 5).
2. **A single document larger than `BULK_BYTES`** (real data has a 182 KB document). It must be sent on its own, not get stuck or be dropped. Pinned by the bulk tests using `BULK_BYTES=1` (Task 4).
3. **Elasticsearch failing a whole request** (503 or connection reset during heavy load). It must be retried with no document lost. Pinned by the fake server failing the first request (Task 4).
4. **Org store unreachable** (Redis down or wrong URL). The run must exit non-zero quickly with an error log, not hang. Pinned by `test_run_exits_nonzero_when_org_store_unreachable` (Task 5).
5. **Peak memory approaching 2 GB on full data.** An OOM kill must surface as a failed run, never as a silent partial index. The parent treats any non-zero worker exit code as fatal (Task 5), and `scripts/verify_index.py` asserts peak memory below 1.9 GiB (Task 6).

## Deviations from spec (deliberate, minor)

- There is no in-memory `OrgStore`. The transform tests pass a plain dict, and the store test exercises the real backends.
- The org load runs before index creation, so an unreachable org store fails before Elasticsearch is touched.
- The fallback peak-memory source is the largest single-process RSS, not a sum. The sum isn't available without polling; cgroup `memory.peak` is the primary source.

## File Structure

| Path | Responsibility |
|---|---|
| `pipeline/requirements.txt` | runtime dependencies (pinned) |
| `pipeline/requirements-dev.txt` | pytest |
| `pipeline/Dockerfile` | image with runtime and dev dependencies |
| `pipeline/main.py` | entrypoint: `sys.exit(main())` |
| `pipeline/etl/__init__.py` | package marker |
| `pipeline/etl/config.py` | `Config` dataclass, built from environment variables |
| `pipeline/etl/metrics.py` | `log()`, `Counters`, `totals()`, `ProgressReporter`, `peak_memory_bytes()` |
| `pipeline/etl/deadletter.py` | `DeadLetter`, an append-only NDJSON file shared by all processes (`flock` per write) |
| `pipeline/etl/reader.py` | `read_lines()`, `parse_envelope()`, `MalformedRecord`, `malformed_fields()` |
| `pipeline/etl/transform.py` | `referenced_org_ids()`, `build_document()` |
| `pipeline/etl/orgstore.py` | `RedisOrgStore`, `LmdbOrgStore`, `SqliteOrgStore`, `create_store()`, `open_store()` |
| `pipeline/etl/bulk.py` | `BulkSender`, `BulkFailed` |
| `pipeline/etl/index_admin.py` | `build_mappings()`, `create_index()`, `finalize_index()` |
| `pipeline/etl/procs.py` | `split_files()`, `wait_all()` |
| `pipeline/etl/orgload.py` | phase 1: `load_orgs()` |
| `pipeline/etl/persons.py` | phase 2 worker: `run_persons()`, `run_worker()` |
| `pipeline/etl/run.py` | orchestration: `main()`, `run(cfg) -> int` |
| `pipeline/tests/test_transform.py` | unit tests 1–2 |
| `pipeline/tests/test_orgstore.py` | unit test 3 |
| `pipeline/tests/test_bulk.py` | unit tests 4–5 |
| `pipeline/tests/test_pipeline_e2e.py` | end-to-end and fail-fast tests |
| `docker-compose.yml` | redis service, ES env tuning, pipeline env and volumes |
| `bench/perf.sh` | prints throughput and memory from `out/metrics.json` |
| `scripts/verify_index.py` | full-data check against the profiler's numbers |
| `scripts/bench.py` | runs one benchmark configuration, prints the comparison table |

---

### Task 1: Scaffold: dependencies, compose, config, metrics, dead letter

**Files:**
- Modify: `pipeline/requirements.txt`, `pipeline/Dockerfile`, `docker-compose.yml`, `.gitignore`
- Create: `pipeline/requirements-dev.txt`, `pipeline/etl/__init__.py`, `pipeline/etl/config.py`, `pipeline/etl/metrics.py`, `pipeline/etl/deadletter.py`

**Interfaces:**
- Produces:
  - `Config` (frozen dataclass) with fields `es_url: str, redis_url: str, data_dir: Path, out_dir: Path, stage_dir: Path, index_name: str, org_store: str, workers: int, loaders: int, lookup_batch: int, bulk_bytes: int, in_flight: int, shards: int, index_org_arrays: bool, person_files: int, max_retries: int, retry_backoff_s: float, progress_interval_s: float`, plus `Config.from_env(env: Mapping[str, str] = os.environ) -> Config` and `Config.as_log_fields() -> dict`.
  - `metrics.log(event: str, level: str = "info", **fields) -> None`
  - `metrics.FIELDS: tuple[str, ...]`
  - `metrics.Counters(values=None)` with `.shared(ctx) -> Counters` (classmethod), `.add(field: str, n: float = 1.0)`, `.get(field: str) -> float` and `.snapshot() -> dict[str, float]`
  - `metrics.totals(blocks: Iterable[Counters]) -> dict[str, float]`
  - `metrics.ProgressReporter(blocks: list[Counters], interval_s: float)` (a thread, with `.start()` and `.stop()`)
  - `metrics.peak_memory_bytes() -> tuple[int, str]`
  - `deadletter.DeadLetter(path: Path)` with `.write(kind: str, **fields)`, `.count: int` and `.close()`

This task is infrastructure only, so it has no unit tests (testing `Config` defaults would be a constant pin). It's verified by the image building, the services becoming healthy, and the imports resolving.

- [ ] **Step 1: Pin dependencies**

`pipeline/requirements.txt`:
```
elasticsearch==8.13.2
orjson==3.10.7
aiohttp==3.9.5
uvloop==0.19.0
redis==5.0.4
lmdb==1.5.1
```

`pipeline/requirements-dev.txt`:
```
pytest==8.2.2
```

- [ ] **Step 2: Update the Dockerfile**

`pipeline/Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

COPY . .

CMD ["python", "-u", "main.py"]
```

- [ ] **Step 3: Update docker-compose.yml**

Replace the whole file with:
```yaml
services:
  elasticsearch:
    image: elasticsearch:8.13.4
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      # Unconstrained service; heap and indexing buffer are benchmark knobs (scripts/bench.py).
      - ES_JAVA_OPTS=-Xms${ES_HEAP:-4g} -Xmx${ES_HEAP:-4g}
      - indices.memory.index_buffer_size=${ES_INDEX_BUFFER:-10%}
    ports:
      - "9200:9200"
    volumes:
      - ./es-config/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:ro
      - es-data:/usr/share/elasticsearch/data
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9200/_cluster/health | grep -q '\"status\":\"\\(green\\|yellow\\)\"'"]
      interval: 1s
      timeout: 3s
      retries: 120

  # Org lookup store for the join (ORG_STORE=redis). Ephemeral cache: persistence off.
  redis:
    image: redis:7.2-alpine
    command: ["redis-server", "--save", "", "--appendonly", "no"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 1s
      timeout: 3s
      retries: 30

  pipeline:
    build: ./pipeline
    depends_on:
      elasticsearch:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      - ES_URL=http://elasticsearch:9200
      - REDIS_URL=redis://redis:6379/0
      - DATA_DIR=/data
      - OUT_DIR=/out
      - STAGE_DIR=/stage
    volumes:
      - ./data:/data:ro
      - ./pipeline:/app
      - ./out:/out
      - stage:/stage
    # Hard ceiling — applies ONLY to the pipeline service. Do not raise.
    # The grader runs the pipeline against the same limits.
    # ES (and any other staging services you add) are unconstrained — give
    # them whatever resources you want.
    mem_limit: 2g
    cpus: 4.0
    command: python -u /app/main.py

volumes:
  es-data:
  stage:
```

Append to `.gitignore`:
```
out/
```

Create `out/` as your own user now, so later host-side scripts can write to `out/bench/` (Docker would otherwise create it owned by root):
```bash
mkdir -p out/bench
```

- [ ] **Step 4: Write `pipeline/etl/__init__.py`** as an empty file.

- [ ] **Step 5: Write `pipeline/etl/config.py`**

```python
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
```

- [ ] **Step 6: Write `pipeline/etl/metrics.py`**

```python
"""Structured logging, cross-process counters and resource metrics."""
from __future__ import annotations

import array
import resource
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import orjson

FIELDS = (
    "orgs_loaded",
    "persons_read",
    "docs_indexed",
    "bytes_sent",
    "malformed",
    "unresolved_refs",
    "persons_with_unresolved",
    "persons_with_affiliations",
    "retries",
    "rejected_items",
    "failed_docs",
    "loop_blocked_s",
    "slot_wait_s",
)
_INDEX = {name: i for i, name in enumerate(FIELDS)}


def log(event: str, level: str = "info", **fields) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "event": event,
        **fields,
    }
    sys.stdout.write(orjson.dumps(record, default=str).decode() + "\n")
    sys.stdout.flush()


class Counters:
    """One process's counters. `shared()` places them in shared memory so the parent can
    read them; each block has exactly one writing process, so no locking is needed."""

    def __init__(self, values=None):
        self._v = values if values is not None else array.array("d", [0.0] * len(FIELDS))

    @classmethod
    def shared(cls, ctx) -> "Counters":
        return cls(ctx.RawArray("d", len(FIELDS)))

    def add(self, field: str, n: float = 1.0) -> None:
        self._v[_INDEX[field]] += n

    def get(self, field: str) -> float:
        return self._v[_INDEX[field]]

    def snapshot(self) -> dict[str, float]:
        return {name: self._v[i] for i, name in enumerate(FIELDS)}


def totals(blocks: Iterable[Counters]) -> dict[str, float]:
    out = dict.fromkeys(FIELDS, 0.0)
    for block in blocks:
        for name, value in block.snapshot().items():
            out[name] += value
    return out


class ProgressReporter(threading.Thread):
    """Logs aggregated counters and indexing rate every `interval_s` seconds."""

    def __init__(self, blocks: list[Counters], interval_s: float):
        super().__init__(name="progress", daemon=True)
        self._blocks = blocks
        self._interval_s = interval_s
        self._stopped = threading.Event()

    def run(self) -> None:
        started = last_t = time.monotonic()
        last_docs = 0.0
        while not self._stopped.wait(self._interval_s):
            t = totals(self._blocks)
            now = time.monotonic()
            log(
                "progress",
                orgs_loaded=int(t["orgs_loaded"]),
                persons_indexed=int(t["docs_indexed"]),
                rate_now=round((t["docs_indexed"] - last_docs) / (now - last_t)),
                rate_avg=round(t["docs_indexed"] / (now - started)),
                mb_sent=round(t["bytes_sent"] / 2**20),
                retries=int(t["retries"]),
                rejected_items=int(t["rejected_items"]),
                failed_docs=int(t["failed_docs"]),
                unresolved_refs=int(t["unresolved_refs"]),
                loop_blocked_s=round(t["loop_blocked_s"], 1),
                slot_wait_s=round(t["slot_wait_s"], 1),
            )
            last_t, last_docs = now, t["docs_indexed"]

    def stop(self) -> None:
        self._stopped.set()
        self.join()


def peak_memory_bytes() -> tuple[int, str]:
    """Peak memory of this container (cgroup v2), else the largest single-process RSS."""
    try:
        return int(Path("/sys/fs/cgroup/memory.peak").read_text()), "cgroup"
    except (OSError, ValueError):
        kb = max(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        )
        return kb * 1024, "max_process_rss"
```

- [ ] **Step 7: Write `pipeline/etl/deadletter.py`**

```python
"""Append-only NDJSON record of inputs the pipeline could not index.

All processes append to the same file. Each record is one unbuffered write under an exclusive
flock, so concurrent writers never interleave lines. Dead letters are rare (0 in the real data),
so the lock costs nothing measurable."""
from __future__ import annotations

import fcntl
from pathlib import Path

import orjson


class DeadLetter:
    def __init__(self, path: Path):
        self.path = path
        self.count = 0
        self._fh = None

    def write(self, kind: str, **fields) -> None:
        if self._fh is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self.path, "ab", buffering=0)
        fcntl.flock(self._fh, fcntl.LOCK_EX)
        try:
            self._fh.write(orjson.dumps({"kind": kind, **fields}, default=str) + b"\n")
        finally:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        self.count += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
```

- [ ] **Step 8: Build and verify**

Run:
```bash
docker compose build pipeline
docker compose up -d --wait elasticsearch redis
docker compose run --rm pipeline python -c "import aiohttp, lmdb, orjson, redis, uvloop; from etl.config import Config; print(Config.from_env().org_store, Config.from_env().es_url)"
```
Expected: the build succeeds, both services report healthy, and the last command prints `redis http://elasticsearch:9200`.

If `pip install lmdb==1.5.1` fails while compiling (no wheel for Python 3.12), change the Dockerfile `RUN` line to `RUN apt-get update && apt-get install -y --no-install-recommends gcc && pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt && apt-get purge -y gcc && rm -rf /var/lib/apt/lists/*` and rebuild.

- [ ] **Step 9: Commit**

```bash
git add pipeline/requirements.txt pipeline/requirements-dev.txt pipeline/Dockerfile docker-compose.yml .gitignore pipeline/etl/
git commit -m "Scaffold pipeline package: config, metrics, dead letter, redis service

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Transform — person + orgs → document

**Files:**
- Create: `pipeline/etl/transform.py`
- Test: `pipeline/tests/test_transform.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `referenced_org_ids(person: dict) -> set[int]`
  - `build_document(person: dict, orgs: Mapping[int, bytes]) -> tuple[bytes, int]`. It returns the document's JSON bytes and the number of unresolved role references. **It mutates `person`**: after the call, `"affiliations" in person` tells the caller whether the person had input affiliations.

- [ ] **Step 1: Write the failing tests**

`pipeline/tests/test_transform.py`:
```python
import orjson

from etl.transform import build_document

DELL = b'{"forager_id":140717,"name":"Dell Technologies","technologies":["ASP.NET"]}'


def test_joins_full_org_record_once_and_moves_input_organizations_to_affiliations():
    person = {
        "forager_id": 1,
        "first_name": "Ann",
        "roles": [
            {"role_title": "Engineer", "organization_id": 140717},
            {"role_title": "Manager", "organization_id": 140717},
        ],
        "organizations": [{"name": "KGI Club"}],
    }

    doc, unresolved = build_document(person, {140717: DELL})

    assert orjson.loads(doc) == {
        "forager_id": 1,
        "first_name": "Ann",
        "roles": [
            {"role_title": "Engineer", "organization_id": 140717, "organization_resolved": True},
            {"role_title": "Manager", "organization_id": 140717, "organization_resolved": True},
        ],
        "organizations": [
            {"forager_id": 140717, "name": "Dell Technologies", "technologies": ["ASP.NET"]}
        ],
        "affiliations": [{"name": "KGI Club"}],
    }
    assert unresolved == 0


def test_keeps_and_flags_roles_whose_organization_is_unknown_or_missing():
    person = {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
    }

    doc, unresolved = build_document(person, {140717: DELL})

    assert orjson.loads(doc) == {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999, "organization_resolved": False},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
        "unresolved_organization_ids": [999],
    }
    assert unresolved == 1
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `docker compose run --rm --no-deps pipeline python -m pytest -q tests/test_transform.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'etl.transform'`

- [ ] **Step 3: Implement `pipeline/etl/transform.py`**

```python
"""Turn one person record plus its looked-up orgs into the indexed document."""
from __future__ import annotations

from typing import Mapping

import orjson


def referenced_org_ids(person: dict) -> set[int]:
    return {
        role["organization_id"]
        for role in person.get("roles") or ()
        if role.get("organization_id") is not None
    }


def build_document(person: dict, orgs: Mapping[int, bytes]) -> tuple[bytes, int]:
    """Mutates `person` into the document and returns (JSON bytes, unresolved reference count).

    - `organizations` becomes the full org-feed records for the person's roles, deduplicated, in
      role order. Org bytes are spliced in verbatim (orjson.Fragment), never re-parsed.
    - Input `organizations` (LinkedIn affiliations, not employers) moves to `affiliations`.
    - Each role with an organization_id gets `organization_resolved`; unknown ids are also listed
      in `unresolved_organization_ids`. Roles are never dropped.
    """
    joined: list[orjson.Fragment] = []
    seen: set[int] = set()
    unresolved: list[int] = []
    unresolved_refs = 0

    for role in person.get("roles") or ():
        org_id = role.get("organization_id")
        if org_id is None:
            continue
        raw = orgs.get(org_id)
        role["organization_resolved"] = raw is not None
        if raw is None:
            unresolved_refs += 1
            if org_id not in unresolved:
                unresolved.append(org_id)
        elif org_id not in seen:
            seen.add(org_id)
            joined.append(orjson.Fragment(raw))

    affiliations = person.pop("organizations", None)
    if affiliations:
        person["affiliations"] = affiliations
    person["organizations"] = joined
    if unresolved:
        person["unresolved_organization_ids"] = unresolved
    return orjson.dumps(person), unresolved_refs
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `docker compose run --rm --no-deps pipeline python -m pytest -q tests/test_transform.py`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline/etl/transform.py pipeline/tests/test_transform.py
git commit -m "Add person/org document transform

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Org stores — Redis, LMDB, SQLite

**Files:**
- Create: `pipeline/etl/orgstore.py`
- Test: `pipeline/tests/test_orgstore.py`

**Interfaces:**
- Consumes: `Config` (Task 1). Uses the fields `org_store`, `redis_url` and `stage_dir`.
- Produces:
  - `create_store(cfg: Config) -> OrgStore`: returns an empty, writable store, after wiping any previous contents.
  - `open_store(cfg: Config) -> OrgStore`: opens an existing store for reading. Redis handles can also write.
  - Each store has:
    - `parallel_writes: bool`: True means several processes may call `put_many` concurrently.
    - `put_many(items: Sequence[tuple[int, bytes]]) -> None`
    - `async get_many(ids: Collection[int]) -> dict[int, bytes]`: missing IDs are absent from the result.
    - `close() -> None`: synchronous; for handles used outside an event loop.
    - `async aclose() -> None`: for handles used in a worker's event loop.

- [ ] **Step 1: Write the failing test**

`pipeline/tests/test_orgstore.py`:
```python
import asyncio
import os

import pytest
import redis

from etl.config import Config
from etl.orgstore import create_store, open_store


def _redis_reachable() -> bool:
    try:
        return redis.Redis.from_url(Config.from_env().redis_url, socket_connect_timeout=1).ping()
    except redis.exceptions.ConnectionError:
        return False


BACKENDS = [
    "sqlite",
    "lmdb",
    pytest.param("redis", marks=pytest.mark.skipif(not _redis_reachable(), reason="redis not reachable")),
]


@pytest.mark.parametrize("backend", BACKENDS)
def test_returns_stored_org_bytes_and_omits_unknown_ids(backend, tmp_path):
    cfg = Config.from_env({**os.environ, "ORG_STORE": backend, "STAGE_DIR": str(tmp_path)})
    writer = create_store(cfg)
    writer.put_many([(1, b'{"a":1}')])
    writer.close()

    async def read():
        reader = open_store(cfg)
        try:
            return await reader.get_many([1, 2])
        finally:
            await reader.aclose()

    assert asyncio.run(read()) == {1: b'{"a":1}'}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `docker compose run --rm pipeline python -m pytest -q tests/test_orgstore.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'etl.orgstore'`

- [ ] **Step 3: Implement `pipeline/etl/orgstore.py`**

```python
"""Org lookup stores for the join: org forager_id -> compact org JSON bytes.

Three interchangeable backends, selected by ORG_STORE and compared by benchmark:
  redis  — separate service; loader processes write in parallel; batched MGET lookups.
  lmdb   — memory-mapped file on the stage volume; single writer.
  sqlite — file on the stage volume; single writer.
"""
from __future__ import annotations

import shutil
import sqlite3
import struct
from pathlib import Path
from typing import Collection, Protocol, Sequence

import lmdb
import redis
import redis.asyncio
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from etl.config import Config

_REDIS_RETRY_ERRORS = [RedisConnectionError, RedisTimeoutError]


class OrgStore(Protocol):
    parallel_writes: bool

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None: ...
    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]: ...
    def close(self) -> None: ...
    async def aclose(self) -> None: ...


class RedisOrgStore:
    parallel_writes = True

    def __init__(self, url: str):
        self._url = url
        self._sync: redis.Redis | None = None
        self._async: redis.asyncio.Redis | None = None

    @staticmethod
    def _key(org_id: int) -> bytes:
        return b"org:%d" % org_id

    def _client(self) -> redis.Redis:
        if self._sync is None:
            self._sync = redis.Redis.from_url(
                self._url,
                retry=Retry(ExponentialBackoff(cap=2.0, base=0.1), 5),
                retry_on_error=_REDIS_RETRY_ERRORS,
            )
        return self._sync

    def reset(self) -> None:
        self._client().flushdb()

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._client().mset({self._key(org_id): data for org_id, data in items})

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        if not ids:
            return {}
        if self._async is None:
            self._async = redis.asyncio.Redis.from_url(
                self._url,
                retry=AsyncRetry(ExponentialBackoff(cap=2.0, base=0.1), 5),
                retry_on_error=_REDIS_RETRY_ERRORS,
            )
        ids = list(ids)
        values = await self._async.mget([self._key(org_id) for org_id in ids])
        return {org_id: v for org_id, v in zip(ids, values) if v is not None}

    def close(self) -> None:
        if self._sync is not None:
            self._sync.close()

    async def aclose(self) -> None:
        if self._async is not None:
            await self._async.aclose()
        self.close()


class LmdbOrgStore:
    parallel_writes = False

    def __init__(self, path: Path, *, writer: bool):
        self._env = lmdb.open(
            str(path),
            map_size=8 << 30,  # sparse upper bound, not an allocation
            readonly=not writer,
            readahead=False,   # random lookups; don't pull neighbouring pages
            sync=False,        # throwaway staging data; durability not needed
            metasync=False,
        )

    @staticmethod
    def _key(org_id: int) -> bytes:
        return struct.pack(">Q", org_id)

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        with self._env.begin(write=True) as txn:
            txn.cursor().putmulti([(self._key(org_id), data) for org_id, data in items])

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        out = {}
        with self._env.begin() as txn:
            for org_id in ids:
                value = txn.get(self._key(org_id))
                if value is not None:
                    out[org_id] = value
        return out

    def close(self) -> None:
        self._env.close()

    async def aclose(self) -> None:
        self.close()


class SqliteOrgStore:
    parallel_writes = False

    def __init__(self, path: Path, *, writer: bool):
        if writer:
            self._db = sqlite3.connect(path)
            self._db.executescript(
                "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                "CREATE TABLE IF NOT EXISTS orgs (id INTEGER PRIMARY KEY, data BLOB NOT NULL);"
            )
        else:
            self._db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._db.executemany("INSERT OR REPLACE INTO orgs VALUES (?, ?)", items)
        self._db.commit()

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        ids = list(ids)
        if not ids:
            return {}
        placeholders = ",".join("?" * len(ids))
        return dict(self._db.execute(f"SELECT id, data FROM orgs WHERE id IN ({placeholders})", ids))

    def close(self) -> None:
        self._db.close()

    async def aclose(self) -> None:
        self.close()


def _file_path(cfg: Config) -> Path:
    return cfg.stage_dir / {"lmdb": "orgs.lmdb", "sqlite": "orgs.sqlite"}[cfg.org_store]


def create_store(cfg: Config) -> OrgStore:
    """An empty, writable store; anything from a previous run is discarded."""
    if cfg.org_store == "redis":
        store = RedisOrgStore(cfg.redis_url)
        store.reset()
        return store
    if cfg.org_store == "lmdb":
        path = _file_path(cfg)
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True)
        return LmdbOrgStore(path, writer=True)
    if cfg.org_store == "sqlite":
        path = _file_path(cfg)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        return SqliteOrgStore(path, writer=True)
    raise ValueError(f"unknown ORG_STORE {cfg.org_store!r}; expected redis, lmdb or sqlite")


def open_store(cfg: Config) -> OrgStore:
    """A handle on the store filled by create_store(); open one per process."""
    if cfg.org_store == "redis":
        return RedisOrgStore(cfg.redis_url)
    if cfg.org_store == "lmdb":
        return LmdbOrgStore(_file_path(cfg), writer=False)
    if cfg.org_store == "sqlite":
        return SqliteOrgStore(_file_path(cfg), writer=False)
    raise ValueError(f"unknown ORG_STORE {cfg.org_store!r}; expected redis, lmdb or sqlite")
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `docker compose run --rm pipeline python -m pytest -q tests/test_orgstore.py`
Expected: `3 passed` (Redis is reachable inside compose; it would show as skipped otherwise).

- [ ] **Step 5: Commit**

```bash
git add pipeline/etl/orgstore.py pipeline/tests/test_orgstore.py
git commit -m "Add Redis, LMDB and SQLite org stores

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Bulk sender — batching, concurrency, retries, dead letters

**Files:**
- Create: `pipeline/etl/bulk.py`
- Test: `pipeline/tests/test_bulk.py`

**Interfaces:**
- Consumes:
  - `Config` (Task 1). Uses the fields `es_url`, `index_name`, `bulk_bytes`, `in_flight`, `max_retries` and `retry_backoff_s`.
  - `DeadLetter` and `Counters` (Task 1).
  - `metrics.log`.
- Produces:
  - `BulkSender(session: aiohttp.ClientSession, cfg: Config, dead_letter: DeadLetter, counters: Counters)` with:
    - `async add(doc_id: int, doc: bytes)`: sends a request once the buffer reaches `bulk_bytes`. It waits for a free in-flight slot, which is the backpressure.
    - `async flush()`: sends the rest and waits for every request.
  - Both methods raise `BulkFailed` once any request has exhausted its retries.
  - Counters it updates: `docs_indexed`, `bytes_sent`, `retries`, `rejected_items`, `failed_docs` and `slot_wait_s`.
  - Dead-letter records it writes: `{"kind": "es_rejected", "_id": "<id>", "status": <int>, "error": {...}}`.

- [ ] **Step 1: Write the failing tests**

`pipeline/tests/test_bulk.py`:
```python
import asyncio

import aiohttp
import orjson
from aiohttp import web
from aiohttp.test_utils import TestServer

from etl.bulk import BulkSender
from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters

DOCS = [(1, b'{"n":1}'), (2, b'{"n":2}'), (3, b'{"n":3}')]


class FakeBulkServer:
    """Minimal `_bulk` endpoint. `item_status(doc_id, attempt)` decides each document's outcome."""

    def __init__(self, item_status, fail_first_request=False):
        self.stored: set[str] = set()
        self._item_status = item_status
        self._fail_next = fail_first_request
        self._attempts: dict[str, int] = {}

    async def handle(self, request: web.Request) -> web.Response:
        if self._fail_next:
            self._fail_next = False
            return web.Response(status=503)
        lines = (await request.read()).splitlines()
        items, errors = [], False
        for action in lines[::2]:
            doc_id = orjson.loads(action)["index"]["_id"]
            attempt = self._attempts[doc_id] = self._attempts.get(doc_id, 0) + 1
            status = self._item_status(doc_id, attempt)
            item = {"_id": doc_id, "status": status}
            if status < 300:
                self.stored.add(doc_id)
            else:
                errors = True
                error_type = "mapper_parsing_exception" if status == 400 else "es_rejected_execution_exception"
                item["error"] = {"type": error_type, "reason": "test"}
            items.append({"index": item})
        return web.json_response({"errors": errors, "items": items})


async def send_docs(server: FakeBulkServer, tmp_path) -> Counters:
    app = web.Application()
    app.router.add_post("/persons/_bulk", server.handle)
    http = TestServer(app)
    await http.start_server()
    # BULK_BYTES=1: every document exceeds the limit, so each goes out as its own request.
    cfg = Config.from_env(
        {"ES_URL": str(http.make_url("")), "BULK_BYTES": "1", "RETRY_BACKOFF_S": "0.01"}
    )
    counters = Counters()
    dead_letter = DeadLetter(tmp_path / "dead.ndjson")
    try:
        async with aiohttp.ClientSession() as session:
            sender = BulkSender(session, cfg, dead_letter, counters)
            for doc_id, doc in DOCS:
                await sender.add(doc_id, doc)
            await sender.flush()
    finally:
        dead_letter.close()
        await http.close()
    return counters


def test_transient_failures_do_not_lose_documents(tmp_path):
    server = FakeBulkServer(
        lambda doc_id, attempt: 429 if doc_id == "2" and attempt == 1 else 201,
        fail_first_request=True,
    )

    asyncio.run(send_docs(server, tmp_path))

    assert server.stored == {"1", "2", "3"}


def test_rejected_document_is_dead_lettered_and_counted(tmp_path):
    server = FakeBulkServer(lambda doc_id, attempt: 400 if doc_id == "2" else 201)

    counters = asyncio.run(send_docs(server, tmp_path))

    assert server.stored == {"1", "3"}
    assert counters.get("failed_docs") == 1
    [line] = (tmp_path / "dead.ndjson").read_bytes().splitlines()
    record = orjson.loads(line)
    assert (record["kind"], record["_id"], record["status"], record["error"]["type"]) == (
        "es_rejected",
        "2",
        400,
        "mapper_parsing_exception",
    )
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `docker compose run --rm --no-deps pipeline python -m pytest -q tests/test_bulk.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'etl.bulk'`

- [ ] **Step 3: Implement `pipeline/etl/bulk.py`**

```python
"""Async `_bulk` sender: byte-capped batches, bounded concurrency, retries, dead letters.

Request bodies are built from already-serialized documents; the elasticsearch client's bulk
helpers are avoided because they re-serialize every document.
"""
from __future__ import annotations

import asyncio
import random
import time

import aiohttp
import orjson

from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters, log

_HEADERS = {"Content-Type": "application/x-ndjson"}


class BulkFailed(RuntimeError):
    """Documents could not be indexed after all retries; the run must fail."""


class _Retryable(Exception):
    pass


def _bulk_body(items: list[tuple[int, bytes]]) -> bytes:
    parts = []
    for doc_id, doc in items:
        parts.append(b'{"index":{"_id":"%d"}}\n' % doc_id)
        parts.append(doc)
        parts.append(b"\n")
    return b"".join(parts)


class BulkSender:
    def __init__(self, session: aiohttp.ClientSession, cfg: Config, dead_letter: DeadLetter, counters: Counters):
        self._session = session
        self._url = f"{cfg.es_url}/{cfg.index_name}/_bulk"
        self._max_bytes = cfg.bulk_bytes
        self._max_retries = cfg.max_retries
        self._backoff_s = cfg.retry_backoff_s
        self._slots = asyncio.Semaphore(cfg.in_flight)
        self._tasks: set[asyncio.Task] = set()
        self._buffer: list[tuple[int, bytes]] = []
        self._buffered = 0
        self._dead_letter = dead_letter
        self._counters = counters
        self._error: BaseException | None = None

    async def add(self, doc_id: int, doc: bytes) -> None:
        self._raise_if_failed()
        self._buffer.append((doc_id, doc))
        self._buffered += len(doc)
        if self._buffered >= self._max_bytes:
            await self._dispatch()

    async def flush(self) -> None:
        if self._buffer:
            await self._dispatch()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._raise_if_failed()

    async def _dispatch(self) -> None:
        items, self._buffer, self._buffered = self._buffer, [], 0
        waited_from = time.monotonic()
        await self._slots.acquire()  # backpressure: at most `in_flight` requests per worker
        self._counters.add("slot_wait_s", time.monotonic() - waited_from)
        task = asyncio.create_task(self._send(items))
        self._tasks.add(task)
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        self._slots.release()
        if not task.cancelled() and task.exception() is not None and self._error is None:
            self._error = task.exception()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    async def _send(self, items: list[tuple[int, bytes]]) -> None:
        pending = items
        for attempt in range(self._max_retries + 1):
            if attempt:
                await asyncio.sleep(self._backoff_s * 2 ** (attempt - 1) * (0.5 + random.random()))
            body = _bulk_body(pending)
            try:
                async with self._session.post(self._url, data=body, headers=_HEADERS) as resp:
                    payload = await resp.read()
                    if resp.status == 429 or resp.status >= 500:
                        raise _Retryable(f"HTTP {resp.status}")
                    if resp.status >= 400:
                        raise BulkFailed(f"bulk request rejected: HTTP {resp.status}: {payload[:500]!r}")
            except (aiohttp.ClientError, asyncio.TimeoutError, _Retryable) as err:
                self._counters.add("retries")
                log("bulk_retry", level="warning", attempt=attempt + 1, docs=len(pending), error=str(err))
                continue
            self._counters.add("bytes_sent", len(body))
            pending = self._handle_items(pending, orjson.loads(payload))
            if not pending:
                return
            self._counters.add("rejected_items", len(pending))
        raise BulkFailed(f"{len(pending)} documents still failing after {self._max_retries} retries")

    def _handle_items(self, pending: list[tuple[int, bytes]], response: dict) -> list[tuple[int, bytes]]:
        """Counts successes, dead-letters permanent failures, returns documents worth retrying."""
        if not response.get("errors"):
            self._counters.add("docs_indexed", len(pending))
            return []
        retry = []
        for (doc_id, doc), item in zip(pending, response["items"]):
            result = item["index"]
            status = result["status"]
            if status < 300:
                self._counters.add("docs_indexed")
            elif status == 429 or status >= 500:
                retry.append((doc_id, doc))
            else:
                self._counters.add("failed_docs")
                self._dead_letter.write("es_rejected", _id=str(doc_id), status=status, error=result.get("error"))
        return retry
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `docker compose run --rm --no-deps pipeline python -m pytest -q tests/test_bulk.py`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add pipeline/etl/bulk.py pipeline/tests/test_bulk.py
git commit -m "Add async bulk sender with retries and dead letters

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: End-to-end pipeline: reader, index admin, org load, person workers, orchestration

**Files:**
- Create: `pipeline/etl/reader.py`, `pipeline/etl/index_admin.py`, `pipeline/etl/procs.py`, `pipeline/etl/orgload.py`, `pipeline/etl/persons.py`, `pipeline/etl/run.py`
- Modify: `pipeline/main.py` (replace the stub)
- Test: `pipeline/tests/test_pipeline_e2e.py`

**Interfaces:**
- Consumes:
  - From Task 1: `Config`, `Counters`, `totals`, `ProgressReporter`, `peak_memory_bytes`, `log` and `DeadLetter`.
  - From Task 2: `build_document` and `referenced_org_ids`.
  - From Task 3: `create_store` and `open_store`.
  - From Task 4: `BulkSender`.
- Produces:
  - `run(cfg: Config) -> int`: the exit code. 0 only if the Elasticsearch count equals parsed persons and no document was permanently rejected.
  - `main() -> int`
  - `reader.read_lines(path) -> Iterator[tuple[int, bytes]]`
  - `reader.parse_envelope(line: bytes) -> tuple[int, dict]`, which raises `reader.MalformedRecord`
  - `index_admin.build_mappings(index_org_arrays: bool) -> dict`
  - `out/metrics.json` with the keys `persons_per_s`, `persons_phase_per_s`, `wall_clock_s`, `phase_s`, `es_count`, `expected_count`, `ok`, `peak_memory_bytes`, `peak_memory_source`, `counters` and `config`. `bench/perf.sh`, `scripts/verify_index.py` and `scripts/bench.py` read these exact keys.

- [ ] **Step 1: Write the failing end-to-end tests**

`pipeline/tests/test_pipeline_e2e.py`:
```python
import gzip
import json
import os
import time
from pathlib import Path

import orjson
import pytest
from elasticsearch import Elasticsearch

from etl.config import Config
from etl.run import run

INDEX = "persons_e2e"


def _org(org_id, name):
    return {"id": org_id, "date_updated": "2026-01-01 00:00:00.000 Z",
            "serialized_data": {"forager_id": org_id, "name": name, "linkedin_id": org_id * 10}}


def _person(person_id, roles, organizations=()):
    return {"id": person_id, "date_updated": "2026-01-01 00:00:00.000 Z",
            "serialized_data": {"forager_id": person_id, "first_name": f"P{person_id}",
                                "roles": [{"role_title": title, "organization_id": org_id} for title, org_id in roles],
                                "organizations": list(organizations)}}


def _write_gz(path: Path, lines: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        fh.writelines(line + b"\n" for line in lines)


def _fixture(tmp_path: Path, **overrides) -> Config:
    data = tmp_path / "data"
    _write_gz(data / "organization" / "orgs_0.json.gz",
              [orjson.dumps(_org(1, "Dell Technologies")), orjson.dumps(_org(2, "Acme")),
               orjson.dumps(_org(3, "Globex"))])
    _write_gz(data / "person" / "persons_0.json.gz", [
        orjson.dumps(_person(101, [("Project Manager", 1), ("Engineer", 2)])),
        orjson.dumps(_person(102, [("Project Manager", 1), ("Project Manager", 1)])),
        orjson.dumps(_person(103, [("Analyst", 999)], organizations=[{"name": "Chess Club"}])),
    ])
    _write_gz(data / "person" / "persons_1.json.gz", [
        orjson.dumps(_person(104, [("Project Manager", None)])),
        b"{not json",
        orjson.dumps(_person(105, [])),
        orjson.dumps(_person(106, [("Director", 3), ("Intern", 998)])),
    ])
    env = {**os.environ, "DATA_DIR": str(data), "OUT_DIR": str(tmp_path / "out"),
           "STAGE_DIR": str(tmp_path / "stage"), "INDEX_NAME": INDEX, "WORKERS": "2",
           "LOADERS": "1", "SHARDS": "1", "PROGRESS_INTERVAL_S": "60", **overrides}
    return Config.from_env(env)


@pytest.fixture
def es():
    client = Elasticsearch(Config.from_env().es_url)
    if not client.ping():
        pytest.skip("elasticsearch not reachable")
    yield client
    client.indices.delete(index=INDEX, ignore_unavailable=True)


def test_pipeline_indexes_joined_persons_and_reruns_cleanly(es, tmp_path):
    cfg = _fixture(tmp_path)

    assert run(cfg) == 0
    assert run(cfg) == 0  # second run on a populated index and org store gives the same result

    def count(query):
        return es.count(index=INDEX, query=query)["count"]

    assert count({"match_all": {}}) == 6
    assert count({"term": {"roles.role_title.keyword": "Project Manager"}}) == 3
    assert count({"term": {"organizations.name.keyword": "Dell Technologies"}}) == 2
    assert count({"exists": {"field": "unresolved_organization_ids"}}) == 2
    p101 = es.get(index=INDEX, id="101")["_source"]
    assert [(o["name"], o["linkedin_id"]) for o in p101["organizations"]] == [("Dell Technologies", 10), ("Acme", 20)]
    p103 = es.get(index=INDEX, id="103")["_source"]
    assert p103["affiliations"] == [{"name": "Chess Club"}]
    assert p103["unresolved_organization_ids"] == [999]
    assert p103["roles"][0]["organization_resolved"] is False
    dead = [json.loads(line) for line in (tmp_path / "out" / "dead_letter.ndjson").read_text().splitlines()]
    assert [(d["kind"], d["file"], d["line"]) for d in dead] == [("malformed_person", "persons_1.json.gz", 2)]
    metrics = json.loads((tmp_path / "out" / "metrics.json").read_text())
    assert (metrics["ok"], metrics["es_count"], metrics["counters"]["unresolved_refs"],
            metrics["counters"]["persons_with_affiliations"]) == (True, 6, 2, 1)


def test_run_exits_nonzero_when_org_store_unreachable(tmp_path):
    cfg = _fixture(tmp_path, ORG_STORE="redis", REDIS_URL="redis://127.0.0.1:1/0")

    started = time.monotonic()
    assert run(cfg) == 1
    assert time.monotonic() - started < 30
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `docker compose run --rm pipeline python -m pytest -q tests/test_pipeline_e2e.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'etl.run'`

- [ ] **Step 3: Implement `pipeline/etl/reader.py`**

```python
"""Streaming reader for gzipped NDJSON envelopes: {id, date_updated, serialized_data}."""
from __future__ import annotations

import gzip
from pathlib import Path
from typing import Iterator

import orjson


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


def malformed_fields(path: Path, lineno: int, line: bytes, err: Exception) -> dict:
    return {"file": path.name, "line": lineno, "error": str(err), "raw": line[:1000].decode("utf-8", "replace")}
```

- [ ] **Step 4: Implement `pipeline/etl/index_admin.py`**

```python
"""Index lifecycle: create with bulk-load settings and explicit mapping, then finalize."""
from __future__ import annotations

from elasticsearch import Elasticsearch

from etl.config import Config

_KEYWORD = {"type": "keyword", "ignore_above": 256}  # same limit as ES dynamic mapping
_TEXT_WITH_KEYWORD = {"type": "text", "fields": {"keyword": _KEYWORD}}
_DATE = {"type": "date", "ignore_malformed": True}  # one bad date must not reject the person


def build_mappings(index_org_arrays: bool) -> dict:
    """`dynamic: false`: every field stays in _source, only the fields below are indexed.
    `roles` / `organizations` are `object`, not `nested`: the correctness suite runs plain
    `term` queries on their subfields, which do not match inside `nested`."""
    org_properties = {
        "forager_id": {"type": "long"},
        "linkedin_id": {"type": "long"},
        "name": _TEXT_WITH_KEYWORD,
        "domain": _KEYWORD,
        "industry": _KEYWORD,
        "country": _KEYWORD,
    }
    if index_org_arrays:
        org_properties |= {"technologies": _KEYWORD, "keywords": _KEYWORD}
    return {
        "dynamic": False,
        "properties": {
            "forager_id": {"type": "long"},
            "linkedin_id": {"type": "long"},
            "first_name": {"type": "text"},
            "last_name": {"type": "text"},
            "headline": {"type": "text"},
            "country": _KEYWORD,
            "city": _KEYWORD,
            "industry": _KEYWORD,
            "skills": _KEYWORD,
            "linkedin_slug": {**_KEYWORD, "doc_values": False},  # exact lookup only
            "roles": {
                "properties": {
                    "role_title": _TEXT_WITH_KEYWORD,
                    "organization_id": {"type": "long"},
                    "organization_resolved": {"type": "boolean"},
                    "start_date": _DATE,
                    "end_date": _DATE,
                }
            },
            "organizations": {"properties": org_properties},
            "unresolved_organization_ids": {"type": "long"},
            "affiliations": {"type": "object", "enabled": False},
        },
    }


def create_index(es: Elasticsearch, cfg: Config) -> None:
    """Drops any previous index and creates it tuned for a one-off bulk load."""
    es.indices.delete(index=cfg.index_name, ignore_unavailable=True)
    es.indices.create(
        index=cfg.index_name,
        settings={
            "number_of_shards": cfg.shards,
            "number_of_replicas": 0,      # single node: a replica could never be assigned
            "refresh_interval": "-1",     # no searchable segments mid-load; refreshed once at the end
            "translog": {"durability": "async", "flush_threshold_size": "1gb"},
        },
        mappings=build_mappings(cfg.index_org_arrays),
    )


def finalize_index(es: Elasticsearch, cfg: Config) -> int:
    """Restores serving settings, makes all documents searchable, returns the document count."""
    es.indices.put_settings(
        index=cfg.index_name,
        settings={"index": {"refresh_interval": "1s", "translog.durability": "request"}},
    )
    es.indices.refresh(index=cfg.index_name)
    return es.count(index=cfg.index_name)["count"]
```

- [ ] **Step 5: Implement `pipeline/etl/procs.py`**

```python
"""Process helpers shared by both phases."""
from __future__ import annotations

from multiprocessing.connection import wait
from multiprocessing.process import BaseProcess
from pathlib import Path


def split_files(files: list[Path], n: int) -> list[list[Path]]:
    """Distributes files over n groups, largest first onto the lightest group."""
    groups: list[list[Path]] = [[] for _ in range(n)]
    sizes = [0] * n
    for path in sorted(files, key=lambda p: p.stat().st_size, reverse=True):
        i = sizes.index(min(sizes))
        groups[i].append(path)
        sizes[i] += path.stat().st_size
    return [g for g in groups if g]


def raise_if_failed(procs: list[BaseProcess]) -> None:
    failed = [p for p in procs if p.exitcode not in (None, 0)]
    if not failed:
        return
    for p in procs:
        if p.is_alive():
            p.terminate()
    for p in procs:
        p.join()
    raise RuntimeError(", ".join(f"{p.name} exited with code {p.exitcode}" for p in failed))


def wait_all(procs: list[BaseProcess]) -> None:
    """Waits for every process; the first non-zero exit (crash, OOM kill) aborts the rest."""
    alive = list(procs)
    while alive:
        wait([p.sentinel for p in alive])
        alive = [p for p in alive if p.exitcode is None]
        raise_if_failed(procs)
```

- [ ] **Step 6: Implement `pipeline/etl/orgload.py`**

```python
"""Phase 1: parse org files in parallel and fill a fresh org store."""
from __future__ import annotations

import queue as queue_module
from pathlib import Path

import orjson

from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters
from etl.orgstore import create_store, open_store
from etl.procs import raise_if_failed, split_files, wait_all
from etl.reader import MalformedRecord, malformed_fields, parse_envelope, read_lines

ORG_BATCH = 2000  # ~5 MB of org JSON per store write


def load_orgs(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    store = create_store(cfg)  # fails fast if the store is unreachable
    try:
        groups = split_files(files, cfg.loaders)
        if store.parallel_writes:
            procs = [ctx.Process(target=_load_files, args=(cfg, group, None, blocks[i], i), name=f"org-loader-{i}")
                     for i, group in enumerate(groups)]
            for p in procs:
                p.start()
            wait_all(procs)
            return
        # Single-writer backends: loaders parse, this process writes.
        batches = ctx.Queue(maxsize=cfg.loaders * 4)
        procs = [ctx.Process(target=_load_files, args=(cfg, group, batches, blocks[i], i), name=f"org-loader-{i}")
                 for i, group in enumerate(groups)]
        for p in procs:
            p.start()
        finished = 0
        while finished < len(procs):
            try:
                batch = batches.get(timeout=1.0)
            except queue_module.Empty:
                raise_if_failed(procs)
                continue
            if batch is None:
                finished += 1
            else:
                store.put_many(batch)
        wait_all(procs)
    finally:
        store.close()


def _load_files(cfg: Config, files: list[Path], batches, counters: Counters, loader_id: int) -> None:
    dead_letter = DeadLetter(cfg.out_dir / "dead_letter.ndjson")
    store = open_store(cfg) if batches is None else None
    sink = store.put_many if store is not None else batches.put
    batch: list[tuple[int, bytes]] = []
    try:
        for path in files:
            for lineno, line in read_lines(path):
                try:
                    org_id, data = parse_envelope(line)
                except MalformedRecord as err:
                    dead_letter.write("malformed_org", **malformed_fields(path, lineno, line, err))
                    counters.add("malformed")
                    continue
                batch.append((org_id, orjson.dumps(data)))
                if len(batch) >= ORG_BATCH:
                    sink(batch)
                    counters.add("orgs_loaded", len(batch))
                    batch = []
        if batch:
            sink(batch)
            counters.add("orgs_loaded", len(batch))
    finally:
        if batches is not None:
            batches.put(None)  # end-of-stream marker, also sent on failure
        if store is not None:
            store.close()
        dead_letter.close()
```

- [ ] **Step 7: Implement `pipeline/etl/persons.py`**

```python
"""Phase 2: one process per group of person files; each runs a uvloop event loop that overlaps
org lookups and bulk requests with the CPU-bound parse/transform work."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Iterator

import aiohttp
import uvloop

from etl.bulk import BulkSender
from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters, log
from etl.orgstore import open_store
from etl.procs import split_files, wait_all
from etl.reader import MalformedRecord, malformed_fields, parse_envelope, read_lines
from etl.transform import build_document, referenced_org_ids

Batch = list[tuple[int, dict]]


def run_persons(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    procs = [ctx.Process(target=run_worker, args=(cfg, group, blocks[i], i), name=f"person-worker-{i}")
             for i, group in enumerate(split_files(files, cfg.workers))]
    for p in procs:
        p.start()
    wait_all(procs)


def run_worker(cfg: Config, files: list[Path], counters: Counters, worker_id: int) -> None:
    try:
        uvloop.run(_run(cfg, files, counters, worker_id))
    except Exception as err:
        log("worker_failed", level="error", worker=worker_id, error=repr(err))
        raise SystemExit(1)


async def _run(cfg: Config, files: list[Path], counters: Counters, worker_id: int) -> None:
    dead_letter = DeadLetter(cfg.out_dir / "dead_letter.ndjson")
    store = open_store(cfg)
    lag_watch = asyncio.create_task(_watch_loop_lag(counters))
    try:
        connector = aiohttp.TCPConnector(limit=cfg.in_flight)
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            sender = BulkSender(session, cfg, dead_letter, counters)
            pending = None
            for batch in _batches(files, cfg.lookup_batch, dead_letter, counters):
                ids: set[int] = set()
                for _, person in batch:
                    ids |= referenced_org_ids(person)
                lookup = asyncio.ensure_future(store.get_many(ids))
                await asyncio.sleep(0)  # let the lookup go out before parsing the next batch
                if pending is not None:
                    await _emit(*pending, sender, counters)
                pending = (batch, lookup)
            if pending is not None:
                await _emit(*pending, sender, counters)
            await sender.flush()
    finally:
        lag_watch.cancel()
        await store.aclose()
        dead_letter.close()


def _batches(files: list[Path], size: int, dead_letter: DeadLetter, counters: Counters) -> Iterator[Batch]:
    batch: Batch = []
    for path in files:
        for lineno, line in read_lines(path):
            try:
                batch.append(parse_envelope(line))
            except MalformedRecord as err:
                dead_letter.write("malformed_person", **malformed_fields(path, lineno, line, err))
                counters.add("malformed")
                continue
            if len(batch) >= size:
                yield batch
                batch = []
    if batch:
        yield batch


async def _emit(batch: Batch, lookup: asyncio.Future, sender: BulkSender, counters: Counters) -> None:
    orgs = await lookup
    for person_id, person in batch:
        doc, unresolved = build_document(person, orgs)
        if unresolved:
            counters.add("unresolved_refs", unresolved)
            counters.add("persons_with_unresolved")
        if "affiliations" in person:
            counters.add("persons_with_affiliations")
        await sender.add(person_id, doc)
    counters.add("persons_read", len(batch))


async def _watch_loop_lag(counters: Counters, interval_s: float = 0.05) -> None:
    """Accumulates how long CPU work kept the event loop from servicing I/O."""
    loop = asyncio.get_running_loop()
    while True:
        started = loop.time()
        await asyncio.sleep(interval_s)
        counters.add("loop_blocked_s", max(0.0, loop.time() - started - interval_s))
```

- [ ] **Step 8: Implement `pipeline/etl/run.py`**

```python
"""Orchestration: org load -> index setup -> person workers -> finalize -> verify -> metrics."""
from __future__ import annotations

import multiprocessing as mp
import time
from contextlib import contextmanager

import orjson
from elasticsearch import Elasticsearch

from etl.config import Config
from etl.index_admin import create_index, finalize_index
from etl.metrics import Counters, ProgressReporter, log, peak_memory_bytes, totals
from etl.orgload import load_orgs
from etl.persons import run_persons


def main() -> int:
    return run(Config.from_env())


def run(cfg: Config) -> int:
    started = time.monotonic()
    log("run_start", config=cfg.as_log_fields())
    try:
        ok = _run(cfg, started)
    except Exception as err:
        log("run_failed", level="error", error=repr(err))
        return 1
    return 0 if ok else 1


@contextmanager
def _phase(name: str, timings: dict[str, float]):
    log("phase_start", phase=name)
    t = time.monotonic()
    yield
    timings[name] = round(time.monotonic() - t, 2)
    log("phase_end", phase=name, duration_s=timings[name])


def _run(cfg: Config, started: float) -> bool:
    org_files = sorted((cfg.data_dir / "organization").glob("*.json.gz"))
    person_files = sorted((cfg.data_dir / "person").glob("*.json.gz"))
    if cfg.person_files:
        person_files = person_files[: cfg.person_files]
    if not org_files or not person_files:
        raise RuntimeError(f"no input files under {cfg.data_dir}/organization or {cfg.data_dir}/person")
    (cfg.out_dir / "dead_letter.ndjson").unlink(missing_ok=True)

    ctx = mp.get_context("spawn")  # fresh interpreters: no inherited event loops or connections
    blocks = [Counters.shared(ctx) for _ in range(cfg.loaders + cfg.workers)]
    es = Elasticsearch(cfg.es_url, request_timeout=300)
    timings: dict[str, float] = {}
    reporter = ProgressReporter(blocks, cfg.progress_interval_s)
    reporter.start()
    try:
        with _phase("org_load", timings):
            load_orgs(cfg, org_files, ctx, blocks[: cfg.loaders])
        with _phase("index_setup", timings):
            create_index(es, cfg)
        with _phase("persons", timings):
            run_persons(cfg, person_files, ctx, blocks[cfg.loaders :])
        with _phase("finalize", timings):
            es_count = finalize_index(es, cfg)
    finally:
        reporter.stop()

    wall_clock_s = time.monotonic() - started
    counts = totals(blocks)
    expected = int(counts["persons_read"] - counts["failed_docs"])
    ok = es_count == expected and counts["failed_docs"] == 0
    peak, peak_source = peak_memory_bytes()
    metrics = {
        "persons_per_s": round(counts["docs_indexed"] / wall_clock_s, 1),
        "persons_phase_per_s": round(counts["docs_indexed"] / timings["persons"], 1),
        "wall_clock_s": round(wall_clock_s, 2),
        "phase_s": timings,
        "es_count": es_count,
        "expected_count": expected,
        "ok": ok,
        "peak_memory_bytes": peak,
        "peak_memory_source": peak_source,
        "counters": {k: round(v, 2) if k.endswith("_s") else int(v) for k, v in counts.items()},
        "config": cfg.as_log_fields(),
    }
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    (cfg.out_dir / "metrics.json").write_bytes(orjson.dumps(metrics, option=orjson.OPT_INDENT_2))
    log("summary", level="info" if ok else "error", **metrics)
    return ok
```

- [ ] **Step 9: Replace `pipeline/main.py`**

```python
"""Pipeline entry point: persons ⋈ organizations -> Elasticsearch `persons` index."""
import sys

from etl.run import main

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 10: Run the end-to-end tests and confirm they pass**

Run: `docker compose run --rm pipeline python -m pytest -q tests/test_pipeline_e2e.py`
Expected: `2 passed`

- [ ] **Step 11: Run the whole suite**

Run: `docker compose run --rm pipeline python -m pytest -q tests`
Expected: `9 passed`

- [ ] **Step 12: Commit**

```bash
git add pipeline/main.py pipeline/etl/ pipeline/tests/test_pipeline_e2e.py
git commit -m "Wire end-to-end pipeline: org load, person workers, index lifecycle, metrics

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: First full-data run: perf report and index verification

**Files:**
- Modify: `bench/perf.sh`
- Create: `scripts/verify_index.py`
- Modify: `EVALUATION.md` (Trade-offs section only, if numbers change a decision)

**Interfaces:**
- Consumes: the `out/metrics.json` keys from Task 5.
- Produces: `bench/perf.sh` output, and `python3 scripts/verify_index.py` (exit code 0 means pass).

- [ ] **Step 1: Write `bench/perf.sh`**

```bash
#!/usr/bin/env bash
# Performance report for the most recent run, read from out/metrics.json (written by the
# pipeline) plus the Elasticsearch container's cgroup peak memory.
set -euo pipefail
cd "$(dirname "$0")/.."

metrics=out/metrics.json
if [[ ! -f $metrics ]]; then
  echo "no $metrics — run 'docker compose up' and wait for the pipeline to finish" >&2
  exit 1
fi
es_peak=$(docker compose exec -T elasticsearch cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)

python3 - "$metrics" "$es_peak" <<'EOF'
import json, sys
m = json.load(open(sys.argv[1]))
es_peak = sys.argv[2].strip()
c = m["counters"]
gib = lambda b: f"{int(b) / 2**30:.2f} GiB"
print(f"persons indexed/s (full run):     {m['persons_per_s']:,.0f}")
print(f"persons indexed/s (person phase): {m['persons_phase_per_s']:,.0f}")
print(f"wall-clock total:                 {m['wall_clock_s']:.1f} s  ("
      + ", ".join(f"{k} {v:.1f}s" for k, v in m["phase_s"].items()) + ")")
print(f"documents in index:               {m['es_count']:,} (expected {m['expected_count']:,}, ok={m['ok']})")
print(f"peak memory, pipeline:            {gib(m['peak_memory_bytes'])} ({m['peak_memory_source']}, limit 2 GiB)")
print(f"peak memory, elasticsearch:       {gib(es_peak) if es_peak else 'n/a (container not running)'}")
print(f"unresolved org refs:              {c['unresolved_refs']:,} in {c['persons_with_unresolved']:,} persons")
print(f"retries / rejected / failed docs: {c['retries']} / {c['rejected_items']} / {c['failed_docs']}")
EOF
```
Run `chmod +x bench/perf.sh`.

- [ ] **Step 2: Write `scripts/verify_index.py`**

```python
#!/usr/bin/env python3
"""Full-data correctness check: the live index and out/metrics.json against numbers computed
independently by scripts/profile_data.py (see scripts/profile_report.json). Stdlib only.

Run after a full ingest:  python3 scripts/verify_index.py
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

ES_URL = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
INDEX = "persons"
METRICS = Path(__file__).resolve().parent.parent / "out" / "metrics.json"


def es_count(query: dict) -> int:
    req = urllib.request.Request(
        f"{ES_URL}/{INDEX}/_count", data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["count"]


def main() -> int:
    metrics = json.loads(METRICS.read_text())
    checks = [
        ("total persons", es_count({"match_all": {}}), 1_000_000),
        ('role title "Project Manager"', es_count({"term": {"roles.role_title.keyword": "Project Manager"}}), 7_081),
        ('org name "Dell Technologies"', es_count({"term": {"organizations.name.keyword": "Dell Technologies"}}), 647),
        ("persons with unresolved org refs", es_count({"exists": {"field": "unresolved_organization_ids"}}), 1_858),
        ("unresolved role references", metrics["counters"]["unresolved_refs"], 3_638),
        ("persons with affiliations", metrics["counters"]["persons_with_affiliations"], 17_302),
        ("malformed input lines", metrics["counters"]["malformed"], 0),
        ("peak pipeline memory < 1.9 GiB", metrics["peak_memory_bytes"] < int(1.9 * 2**30), True),
    ]
    failures = 0
    for name, actual, expected in checks:
        ok = actual == expected
        failures += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: got {actual}, expected {expected}")
    print("all checks passed." if not failures else f"{failures} check(s) failed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run the full ingest from a clean state**

```bash
docker compose down -v
docker compose up -d --build
docker compose wait pipeline
docker compose logs pipeline | tail -n 5
```
Expected: `docker compose wait` prints `0`, and the last log line is a `"event":"summary"` record with `"ok":true`.

- [ ] **Step 4: Verify the results**

```bash
bench/perf.sh
python3 scripts/verify_index.py
python3 bench/correctness.py
```
Expected:
- `perf.sh` prints every line, and the pipeline's peak memory is below 2 GiB.
- `verify_index.py` prints `all checks passed.`
- `correctness.py` passes the total count. Until the official `expected.json` arrives, its two term-query checks show `FAIL ... expected 0`; that's expected, because the placeholder file says 0.

If `verify_index.py` fails, stop and debug (superpowers:systematic-debugging) before going on to Task 7.

- [ ] **Step 5: Commit**

```bash
git add bench/perf.sh scripts/verify_index.py
git commit -m "Add perf report and full-data index verification

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Benchmark harness and tuning sweeps

**Files:**
- Create: `scripts/bench.py`
- Modify: `pipeline/etl/config.py` (defaults, if a sweep finds better values), `docker-compose.yml` (`ES_HEAP` / `ES_INDEX_BUFFER` defaults), `EVALUATION.md` (Trade-offs bullets)

**Interfaces:**
- Consumes: the `out/metrics.json` keys from Task 5, plus `scripts/verify_index.py`.
- Produces:
  - `python3 scripts/bench.py LABEL [-e KEY=VALUE ...] [--es-heap 4g] [--es-buffer 10%] [--runs N] [--verify]`
  - `python3 scripts/bench.py --table`
  - Results saved in `out/bench/<label>-<n>.json`

- [ ] **Step 1: Write `scripts/bench.py`**

```python
#!/usr/bin/env python3
"""Run one pipeline configuration under docker compose and record its metrics.

  python3 scripts/bench.py LABEL [-e KEY=VALUE ...] [--es-heap 4g] [--es-buffer 10%] [--runs N] [--verify]
  python3 scripts/bench.py --table

-e values are pipeline env overrides (see pipeline/etl/config.py). --es-heap/--es-buffer recreate
Elasticsearch with that heap / indices.memory.index_buffer_size. Each run starts from a fresh
index and org store (the pipeline resets both). Results: out/bench/<label>-<n>.json.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = ROOT / "out" / "bench"


def compose(*args: str, env: dict | None = None) -> int:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, env=env).returncode


def run_once(label: str, overrides: list[str], es_env: dict, n: int, verify: bool) -> None:
    env = {**os.environ, **es_env}
    if compose("up", "-d", "--wait", "elasticsearch", "redis", env=env) != 0:
        sys.exit("services failed to start")
    flags = [arg for kv in overrides for arg in ("-e", kv)]
    exit_code = compose("run", "--rm", *flags, "pipeline", env=env)
    result = json.loads((ROOT / "out" / "metrics.json").read_text()) if exit_code == 0 else {}
    result |= {"label": label, "run": n, "exit_code": exit_code, "overrides": overrides, "es_env": es_env}
    if verify and exit_code == 0:
        result["verified"] = subprocess.run([sys.executable, str(ROOT / "scripts" / "verify_index.py")]).returncode == 0
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    (BENCH_DIR / f"{label}-{n}.json").write_text(json.dumps(result, indent=2))


def print_table() -> None:
    rows = [json.loads(p.read_text()) for p in sorted(BENCH_DIR.glob("*.json"))]
    header = f"{'label':<28}{'run':>4}{'ok':>6}{'p/s full':>10}{'p/s phase':>11}{'wall s':>9}{'org s':>8}{'peak GiB':>10}  settings"
    print(header)
    print("-" * len(header))
    for r in rows:
        ok = r.get("ok", False) and r.get("verified", True)
        settings = " ".join(r["overrides"] + [f"{k}={v}" for k, v in r["es_env"].items()])
        print(f"{r['label']:<28}{r['run']:>4}{str(ok):>6}{r.get('persons_per_s', 0):>10,.0f}"
              f"{r.get('persons_phase_per_s', 0):>11,.0f}{r.get('wall_clock_s', 0):>9.1f}"
              f"{r.get('phase_s', {}).get('org_load', 0):>8.1f}{r.get('peak_memory_bytes', 0) / 2**30:>10.2f}  {settings}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("label", nargs="?")
    parser.add_argument("-e", dest="overrides", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--es-heap")
    parser.add_argument("--es-buffer")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--verify", action="store_true", help="run scripts/verify_index.py (full data only)")
    parser.add_argument("--table", action="store_true")
    args = parser.parse_args()
    if args.table:
        print_table()
        return
    if not args.label:
        parser.error("LABEL is required unless --table")
    es_env = {k: v for k, v in (("ES_HEAP", args.es_heap), ("ES_INDEX_BUFFER", args.es_buffer)) if v}
    for n in range(1, args.runs + 1):
        run_once(args.label, args.overrides, es_env, n, args.verify)
    print_table()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-test the harness**

Run: `python3 scripts/bench.py smoke -e PERSON_FILES=1`
Expected: the table prints one row, `smoke`, with `ok` = `True` and a non-zero `p/s phase`.

- [ ] **Step 3: Baseline (sweep #1)**

Run: `python3 scripts/bench.py baseline --runs 1 --verify`
Expected: one row with `ok` = `True`. Record the baseline `p/s full` in the commit message at Step 12.

- [ ] **Step 4: Org store (sweep #2, full data)**

```bash
python3 scripts/bench.py store-redis  -e ORG_STORE=redis  --verify
python3 scripts/bench.py store-lmdb   -e ORG_STORE=lmdb   --verify
python3 scripts/bench.py store-sqlite -e ORG_STORE=sqlite --verify
```
Decision rule: pick the row with the highest `p/s full` among `ok=True` rows. Set that value as the `ORG_STORE` default in `config.py`. If the winner isn't Redis, keep the Redis service in compose, since it's still a selectable backend. In `EVALUATION.md`, replace the store `_TBD_` with one bullet giving the three `p/s full` values and the `org s` (org load) times.

- [ ] **Step 5: Bulk size (sweep #3)**

```bash
for mb in 5 10 20; do python3 scripts/bench.py bulk-${mb}mb -e PERSON_FILES=2 -e BULK_BYTES=$((mb*1024*1024)); done
```
Decision rule: pick the highest `p/s phase` and set it as the `BULK_BYTES` default. This and the remaining sweeps use the winning store, which is now the default.

- [ ] **Step 6: Shards (sweep #4)**

```bash
for s in 2 4 6 8; do python3 scripts/bench.py shards-$s -e PERSON_FILES=2 -e SHARDS=$s; done
```
Decision rule: pick the highest `p/s phase` and set it as the `SHARDS` default.

- [ ] **Step 7: In-flight per worker (sweep #5)**

```bash
for n in 1 2 3 4; do python3 scripts/bench.py inflight-$n -e PERSON_FILES=2 -e IN_FLIGHT=$n; done
```
Decision rule: pick the highest `p/s phase`, but rule out any value whose run logged `rejected_items` > 0 in `out/bench/inflight-N-1.json` (`counters.rejected_items`). Set the result as the `IN_FLIGHT` default.

- [ ] **Step 8: Workers (sweep #6, conditional)**

Look at `counters.loop_blocked_s` and `counters.slot_wait_s` in the best run so far. If `slot_wait_s` is less than 10% of `phase_s.persons × WORKERS`, the workers are CPU-bound, so skip this step. Otherwise run:
```bash
for w in 5 6; do python3 scripts/bench.py workers-$w -e PERSON_FILES=2 -e WORKERS=$w; done
```
Decision rule: adopt a value only if `p/s phase` improves by more than 3% and `peak GiB` stays below 1.9.

- [ ] **Step 9: Mapping variant (sweep #7, full data)**

```bash
python3 scripts/bench.py mapping-lean --verify
python3 scripts/bench.py mapping-org-arrays -e INDEX_ORG_ARRAYS=1 --verify
```
Decision rule: if indexing the org arrays costs less than 10% of `p/s full`, set the `INDEX_ORG_ARRAYS` default to `"1"` (more useful for search). Otherwise keep `"0"`. Either way, replace the mapping `_TBD_` in `EVALUATION.md` with one bullet giving both `p/s full` values.

- [ ] **Step 10: Elasticsearch node (sweep #8)**

```bash
for h in 3g 4g 5g; do python3 scripts/bench.py heap-$h -e PERSON_FILES=2 --es-heap $h; done
python3 scripts/bench.py buffer-30 -e PERSON_FILES=2 --es-buffer 30%
```
Decision rule: pick the highest `p/s phase`, and set the winners as the `${ES_HEAP:-…}` / `${ES_INDEX_BUFFER:-…}` defaults in `docker-compose.yml`. Cap the heap at `4g`, even if `5g` wins, so the stack fits a 16 GB grader machine, and note any capped gain in `EVALUATION.md`.

- [ ] **Step 11: Final confirmation (3 full runs, page cache dropped)**

```bash
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
python3 scripts/bench.py final --runs 3 --verify
```
Expected: three rows, all `ok=True`. The **median** `p/s full` is the reported number.

- [ ] **Step 12: Commit**

```bash
git add scripts/bench.py pipeline/etl/config.py docker-compose.yml EVALUATION.md
git commit -m "Add benchmark harness; tune defaults from sweeps (median full-run p/s: <value from Step 11>)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Library pass and measurement-only experiments

**Files:**
- Modify: `pipeline/requirements.txt`, `pipeline/etl/reader.py`, `pipeline/etl/orgload.py`
- Test: existing suite (`docker compose run --rm pipeline python -m pytest -q tests`)

Each step is a separate experiment. In Steps 1–3, a change is kept only if `p/s full` (median of 2 runs) improves by more than 3% over `final` from Task 7. Otherwise revert that step with `git checkout -- <files>`.

- [ ] **Step 1: python-isal for gzip**

Add `isal==1.6.1` to `pipeline/requirements.txt`. In `pipeline/etl/reader.py`, replace `import gzip` with:
```python
from isal import igzip as gzip  # ISA-L inflate: same API as stdlib gzip, ~2-3x faster
```
Run:
```bash
docker compose build pipeline
docker compose run --rm pipeline python -m pytest -q tests
python3 scripts/bench.py lib-isal --runs 2 --verify
```
Expected: `9 passed`, then two `ok=True` rows. Keep or revert by the rule above.

- [ ] **Step 2: msgspec Raw for the org load**

Add `msgspec==0.18.6` to `pipeline/requirements.txt`. In `pipeline/etl/orgload.py`, add these imports and module-level decoder:
```python
import msgspec


class _OrgEnvelope(msgspec.Struct):
    id: int
    serialized_data: msgspec.Raw


_decode_org = msgspec.json.Decoder(_OrgEnvelope).decode
```
Replace the parse block in `_load_files`:
```python
                try:
                    envelope = _decode_org(line)
                except (msgspec.DecodeError, msgspec.ValidationError) as err:
                    dead_letter.write("malformed_org", **malformed_fields(path, lineno, line, err))
                    counters.add("malformed")
                    continue
                # serialized_data bytes are stored verbatim (no parse/re-serialize); the envelope
                # id equals serialized_data.forager_id for every org (scripts/profile_report.json).
                batch.append((envelope.id, bytes(envelope.serialized_data)))
```
Remove the now-unused `parse_envelope`, `MalformedRecord` and `orjson` imports from `orgload.py`. Then run:
```bash
docker compose build pipeline
docker compose run --rm pipeline python -m pytest -q tests
python3 scripts/bench.py lib-msgspec --runs 2 --verify
```
Expected: `9 passed`, then two `ok=True` rows. Keep or revert by the rule above, comparing `org s` as well.

- [ ] **Step 3: hiredis (only if the default `ORG_STORE` is `redis`)**

Add `hiredis==2.3.2` to `pipeline/requirements.txt`. redis-py picks it up automatically, with no code change. Run:
```bash
docker compose build pipeline
python3 scripts/bench.py lib-hiredis --runs 2 --verify
```
Keep or revert by the rule above.

- [ ] **Step 4: Measurement only: gzip-compressed bulk bodies (sweep #9)**

Temporarily patch `pipeline/etl/bulk.py`. Add `import gzip`, and in `_send` replace `body = _bulk_body(pending)` with:
```python
            body = gzip.compress(_bulk_body(pending), compresslevel=1)
```
Change the `session.post(...)` headers argument to `headers={**_HEADERS, "Content-Encoding": "gzip"}`. Then run:
```bash
python3 scripts/bench.py exp-bulk-gzip -e PERSON_FILES=2
git checkout -- pipeline/etl/bulk.py
```
Keep the change only if `p/s phase` beats the best Task 7 `PERSON_FILES=2` row by more than 3% (if it does, re-apply the patch). Either way, record the delta in the `EVALUATION.md` Trade-offs.

- [ ] **Step 5: Measurement only: auto-generated IDs (sweep #10)**

Temporarily patch `_bulk_body` in `pipeline/etl/bulk.py` to write `b'{"index":{}}\n'` instead of the `_id` action line. Then run:
```bash
python3 scripts/bench.py exp-auto-ids -e PERSON_FILES=2
git checkout -- pipeline/etl/bulk.py
```
Always revert: explicit `_id` is what makes retries idempotent. Record the measured cost in one Trade-offs bullet ("explicit `_id` costs X% vs auto IDs; kept for exactly-once counts under retries").

- [ ] **Step 6: Commit the kept changes**

```bash
git add pipeline/requirements.txt pipeline/etl/reader.py pipeline/etl/orgload.py EVALUATION.md
git commit -m "Library pass: adopt measured wins (<list kept libraries and their p/s deltas>)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Add one bullet per tried library to the `EVALUATION.md` Trade-offs, kept or rejected, with its delta.

---

### Task 9: Docs and final verification

**Files:**
- Modify: `CLAUDE.md`, `README.md`

- [ ] **Step 1: Update CLAUDE.md**

In the `## Commands` code block, add these lines:
```bash
docker compose run --rm pipeline python -m pytest -q tests   # all tests (unit + e2e; needs ES/Redis, compose starts them)
docker compose run --rm --no-deps pipeline python -m pytest -q tests/test_transform.py tests/test_bulk.py  # no services needed
python3 scripts/verify_index.py           # full-data check vs independent profiler numbers
python3 scripts/bench.py LABEL -e KEY=VAL # one benchmark config; --table to compare
```
Replace the sentence "Currently `pipeline/main.py` is a stub ... still to be built." with:
"Pipeline code lives in `pipeline/etl/` (entry `pipeline/main.py`). Design: `docs/superpowers/specs/2026-10-01-etl-pipeline-design.md`. All tunables are env vars read in `pipeline/etl/config.py`."

- [ ] **Step 2: Replace README.md**

Write a concise README covering:
1. How to run: `docker compose up --build`, then `bench/perf.sh` and `python3 bench/correctness.py`.
2. Architecture: copy the diagram from spec §3.
3. Configuration: a table of the env vars from `config.py` with their final defaults.
4. Schema rationale: link to `EVALUATION.md` Trade-offs.
5. Performance: the median from Task 7 Step 11.
6. Known limitations:
   - `object` flattening of roles
   - the single-node Elasticsearch (no replicas)
   - the 647 vs 570 Dell count question
   - pytest is installed in the runtime image

Keep the original brief available as `docs/BRIEF.md` (`git mv README.md docs/BRIEF.md` before writing the new README).

- [ ] **Step 3: Final clean run**

```bash
docker compose down -v
docker compose up -d --build
docker compose wait pipeline
bench/perf.sh
python3 scripts/verify_index.py
docker compose run --rm pipeline python -m pytest -q tests
```
Expected: `wait` prints `0`, `verify_index.py` prints `all checks passed.`, and the tests show `9 passed`.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md README.md docs/BRIEF.md
git commit -m "Docs: README, CLAUDE.md commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
