# Persons + organizations ETL into Elasticsearch

Streams two gzipped NDJSON feeds (1,000,000 persons, 529,041 organizations), joins
`person.roles[].organization_id` to the organization feed, and bulk-indexes one merged document per
person into the Elasticsearch index `persons`. The original brief is in [docs/BRIEF.md](docs/BRIEF.md).

## Run

```bash
docker compose up --build         # Elasticsearch 8.13 + Redis + pipeline; the pipeline exits when done
bench/perf.sh                     # persons/s, wall-clock, peak memory (reads out/metrics.json)
python3 bench/correctness.py      # gate tests against http://localhost:9200
docker compose down -v            # wipe the index for a fresh run
```

Input is expected in `data/person/*.json.gz` and `data/organization/*.json.gz`. The `pipeline` service
is limited to 2 GB and 4 CPUs.

## Architecture

```
pipeline container (2 GB, 4 CPU)
 Phase 1  org files -> 4 parse procs -> Redis, each proc writing in parallel
 Phase 2  4 worker procs, one asyncio loop each (uvloop), whole person files per worker:
          read+parse batch -> await get_many(ids) [prefetch next batch] -> transform (orjson.Fragment)
          -> acquire semaphore(N) -> aiohttp POST /_bulk (raw NDJSON bytes) -> item errors / retry
 Parent   index setup -> phase 1 -> phase 2 -> finalize (restore settings, refresh) -> verify -> metrics.json
```

- Processes for CPU (gunzip, parse, transform, serialize); asyncio inside each worker overlaps Redis
  lookups and bulk I/O with CPU work. A per-worker semaphore bounds in-flight bulk requests.
- Organizations are stored as compact JSON in Redis and spliced into each document with
  `orjson.Fragment`, so they are never re-parsed. `organizations[]` holds the full org-feed record.
- Unresolved org ids keep the role (`organization_resolved: false`) and are listed in
  `unresolved_organization_ids`.
- Code is in `pipeline/etl/`; design in
  [docs/superpowers/specs/2026-10-01-etl-pipeline-design.md](docs/superpowers/specs/2026-10-01-etl-pipeline-design.md).

## Configuration

Environment variables read in `pipeline/etl/config.py` (set in `docker-compose.yml` or with
`docker compose run -e`).

| Variable | Default | Meaning |
|---|---|---|
| `ES_URL` | `http://localhost:9200` (compose: `http://elasticsearch:9200`) | Elasticsearch endpoint |
| `REDIS_URL` | `redis://localhost:6379/0` (compose: `redis://redis:6379/0`) | Org store |
| `DATA_DIR` | `/data` | Input root with `person/` and `organization/` |
| `OUT_DIR` | `/out` | Output for `metrics.json` and `dead_letter.ndjson` |
| `INDEX_NAME` | `persons` | Target index (dropped and recreated at start) |
| `WORKERS` | 4 | Person-phase worker processes |
| `LOADERS` | 4 | Org-phase parser processes |
| `LOOKUP_BATCH` | 500 | Persons per Redis lookup |
| `BULK_BYTES` | 10485760 | Send a `_bulk` request once its documents reach this size |
| `IN_FLIGHT` | 2 | Concurrent `_bulk` requests per worker |
| `SHARDS` | 4 | Index shards |
| `PERSON_FILES` | 0 | Use only the first N person files (0 = all) |
| `MAX_RETRIES` | 8 | Attempts per bulk request or item for clean rejections |
| `RETRY_BACKOFF_S` | 0.5 | Base of the exponential backoff |
| `PROGRESS_INTERVAL_S` | 5 | Progress log interval |

Compose-level (Elasticsearch container): `ES_HEAP` (default `4g`), `ES_INDEX_BUFFER` (default `10%`).

## Schema and trade-offs

Mapping, join strategy and the other decisions are in [EVALUATION.md](EVALUATION.md#trade-offs).

## Performance

Full numbers and method in [EVALUATION.md](EVALUATION.md#performance). Final clean run on a 16 vCPU /
20 GB VM: 8,893 persons/s over the full run (9,754 in the person phase), 112.5 s wall-clock, pipeline
peak memory 0.64 GiB, Elasticsearch peak memory 11.90 GiB (cgroup, includes page cache). Run-to-run
variation on repeated full runs was about 10-15%.

## Verification

- `python3 bench/correctness.py` is the provided gate.
- `python3 scripts/verify_index.py` checks the index against the numbers from the independent data
  profiler (`scripts/profile_data.py`, `scripts/profile_report.json`): counts, unresolved references,
  affiliations, memory.
- `python3 scripts/verify_documents.py` rebuilds about 2,700 sampled documents from the raw files and
  compares them field by field with what is in Elasticsearch. Both scripts share no code with the
  pipeline.
- `docker compose run --rm pipeline python -m pytest -q tests` runs 8 tests (transform, Redis store,
  bulk retry and failure handling, end-to-end on a fixture). `--no-deps` with
  `tests/test_transform.py tests/test_bulk.py` runs the ones that need no services.

## Known limitations

- `roles` and `organizations` are `object`, not `nested`: "title X at org Y" can match across different
  roles of one person. The provided `term` queries require `object`.
- Single-node Elasticsearch, 0 replicas.
- Dell count: 647 persons by the org-feed name "Dell Technologies"; 570 if counted by
  `roles[].organization_name`. `bench/expected.json` in the repo is a placeholder (0 counts), so the
  two term checks of `bench/correctness.py` fail until the official file is available.
- Document ids are auto-generated. A network failure after a request may have reached Elasticsearch
  fails the run instead of retrying; only clean rejections are retried.
- `pytest` is installed in the runtime image (`pipeline/requirements-dev.txt`).
