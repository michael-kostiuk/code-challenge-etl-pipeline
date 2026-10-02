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
  `orjson.Fragment`, so they are never re-parsed.
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
| `LOG_LEVEL` | `INFO` | Level for all loggers, including library ones; JSON lines on stdout |

Compose-level (Elasticsearch container): `ES_HEAP` (default `4g`), `ES_INDEX_BUFFER` (default `10%`).

## Schema

One document per person: the unwrapped `serialized_data`, with `organizations[]` set to the full
org-feed records of the person's roles (deduplicated, in role order). Dangling references are flagged
with `roles[].organization_resolved: false` and `unresolved_organization_ids`. The input's own
`organizations[]` (LinkedIn affiliations, not employers) is moved to `affiliations`.

Malformed input is dead-lettered to `out/dead_letter.ndjson` (file, line, error, raw prefix) and
counted, never indexed: invalid JSON, a missing `serialized_data` or integer `forager_id`, an envelope
`id` that differs from `forager_id`, or `roles` the join can't read (not a list of objects, or a
non-integer `organization_id`). Documents Elasticsearch rejects are dead-lettered the same way. Neither
fails the run; it exits non-zero only when a phase fails or the index count differs from the accepted
documents (`persons_read - failed_docs`), i.e. documents were lost or duplicated.

Explicit mapping with `dynamic: false` (`pipeline/etl/index_admin.py`): every field stays in
`_source`, only the fields below are indexed.

| Fields | Mapping | Why |
|---|---|---|
| `forager_id`, `linkedin_id`, `roles.organization_id`, `organizations.forager_id` / `.linkedin_id`, `unresolved_organization_ids` | `keyword` | id lookups and joins back to the feeds; ids are never range-queried, and `term` on `keyword` is the faster lookup |
| `first_name`, `last_name` | `text` + `.keyword` | search, plus exact match, sorting and aggregations |
| `headline` | `text` | full-text search only |
| `date_updated`, `organizations.date_updated` | `date`, format `yyyy-MM-dd HH:mm:ss.SSS X`, `ignore_malformed` | "updated since" queries; the feed's own timestamp format, `Z` or an offset like `-0700` |
| `country`, `city`, `industry`, `skills`, `organizations.domain` / `.industry` / `.country` | `keyword`, `ignore_above: 256` | filters and facets; 256 is the dynamic-mapping default |
| `roles.role_title`, `organizations.name` | `text` + `.keyword` | search, plus the exact `term` queries of `bench/correctness.py` |
| `linkedin_slug` | `keyword`, `doc_values: false` | exact lookup only, never sorted or aggregated |
| `roles.start_date`, `roles.end_date` | `date`, `ignore_malformed` | one bad date must not reject the person |
| `roles.organization_resolved` | `boolean` | find unresolved references |
| `roles`, `organizations` | `object` | the `term` queries above do not match inside `nested` |
| `affiliations` | `enabled: false` | returned, never queried; not even parsed |
| org `technologies` / `keywords`, all other fields | unmapped | `_source` only |

Why these choices over the alternatives (`index: false`, `nested`, indexing all org fields), and the
join strategy: [EVALUATION.md](EVALUATION.md#trade-offs).

## Performance

Full numbers and method in [EVALUATION.md](EVALUATION.md#performance). Median of 3 clean runs on an
Apple M4 Pro (Docker Desktop VM: 12 CPUs, 8 GB): 27,383 persons/s over the full run (32,605 in the
person phase), 36.5 s wall-clock, pipeline peak memory 0.85 GiB, Elasticsearch peak memory 5.25 GiB
(cgroup, includes page cache). The three clean runs ranged 24,662–28,324 p/s; re-runs against warm
services run faster. The final mapping (keyword ids, name `.keyword`, `date_updated`) costs about 6%
against the previous one in interleaved A/B runs.

## Verification

- `python3 bench/correctness.py` is the provided gate.
- `python3 scripts/verify_index.py` checks the index against the numbers from the independent data
  profiler (`scripts/profile_data.py`, `scripts/profile_report.json`): counts, unresolved references,
  affiliations, memory.
- `python3 scripts/verify_documents.py` rebuilds about 2,700 sampled documents from the raw files and
  compares them field by field with what is in Elasticsearch. Both scripts share no code with the
  pipeline.
- `docker compose run --rm pipeline python -m pytest -q tests` runs 18 tests (envelope and role
  validation, transform, Redis store, bulk retry and failure handling, end-to-end on a fixture).
  `--no-deps` with `tests/test_reader.py tests/test_transform.py tests/test_bulk.py` runs the ones
  that need no services.

## Known limitations

- `roles` and `organizations` are `object`, not `nested`: "title X at org Y" can match across different
  roles of one person. The provided `term` queries require `object`.
- Single-node Elasticsearch, 0 replicas.
- Dell count: 647 persons by the org-feed name "Dell Technologies"; 570 if counted by
  `roles[].organization_name`. The official `bench/expected.json` was not in the data bundle, so the
  repo's copy holds our computed counts (7,081 / 647); the shipped placeholder is
  `bench/expected.original.json`.
- Document ids are auto-generated. A network failure after a request may have reached Elasticsearch
  fails the run instead of retrying; only clean rejections are retried.
- `pytest` is installed in the runtime image (`pipeline/requirements-dev.txt`).
