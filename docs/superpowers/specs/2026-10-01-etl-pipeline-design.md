# ETL Pipeline Design — persons ⋈ organizations → Elasticsearch

Date: 2026-10-01 · Status: approved in brainstorming, pending spec review

## 1. Goal and constraints

Stream 1,000,000 person records and 529,041 organization records (gzipped NDJSON envelopes), join
`person.roles[].organization_id` → `org.forager_id`, and bulk-index one document per person into
the Elasticsearch index `persons`. **Primary metric: persons indexed per second over the full run.**
Gate: `bench/correctness.py` passes.

Hard constraints: `pipeline` service `mem_limit: 2g`, `cpus: 4.0` (never raised); everything runs from
`docker compose up` on a fresh machine; no preprocessing outside the pipeline; `bench/correctness.py`
and `bench/expected.json` are not modified. ES and staging services are unconstrained.

Decisions taken: Python; org store chosen by benchmark among Redis / LMDB / SQLite; existing
`serialized_data.organizations` moved to `affiliations`; org heavy arrays indexed or not by benchmark;
the take-home's time budget is not a design constraint.

## 2. Data facts (from `scripts/profile_report.json`)

| | Value |
|---|---|
| Persons | 1,000,000, no duplicate ids, 0 malformed lines, 2.7 GB `serialized_data` |
| Orgs | 529,041, no duplicates, 0 malformed, 1.3 GB compact JSON (avg 2.5 KB, max 24 KB) |
| Orgs referenced by ≥1 person | 99.4% → pruning the org set saves nothing |
| Role→org references | 1,385,337; 99.74% resolve; 3,638 unresolved (1,858 persons) |
| Roles with null `organization_id` | 697,686 (33.5%) |
| Persons with non-empty input `organizations[]` | 17,302 (LinkedIn affiliations, not employers) |
| Merged doc size | avg 6.5 KB, p99 46 KB, max 182 KB; **6.1 GB total** |
| Max org name / role title length | 118 / 100 chars |
| Reference counts | "Project Manager" 7,081 · org-feed name "Dell Technologies" 647 (role-name variant: 570) |

Consequences: the org table does not fit comfortably in the 2 GB pipeline as Python objects → external
store; ES must absorb 6.1 GB of documents → ES indexing is the expected bottleneck.

## 3. Architecture

```
pipeline container (2 GB, 4 CPU)
 Phase 1  org files → 4 parse procs → OrgStore.put_many
          redis: each proc writes in parallel · lmdb/sqlite: parent is the single writer
 Phase 2  4 worker procs, one asyncio loop each (uvloop), whole person files per worker:
          read+parse batch → await get_many(ids) [prefetch next batch] → transform (orjson.Fragment)
          → acquire semaphore(N) → aiohttp POST /_bulk (raw NDJSON bytes) → item errors / retry
 Parent   index setup → phase 1 → phase 2 → finalize (restore settings, refresh) → verify → metrics.json
```

- **Processes for CPU** (gunzip, parse, transform, serialize); **asyncio inside each worker** to
  overlap org lookups and bulk I/O with CPU work. Backpressure: per-worker semaphore bounds
  in-flight bulk requests; a slow ES blocks the producer instead of buffering.
- No separate I/O-worker processes: moving 6.1 GB through IPC costs more than it saves. Revisit only
  if metrics show slot-wait time with idle ES (see §6).
- Bulk bodies are built by us as NDJSON bytes and POSTed with `aiohttp`; the `elasticsearch` client
  is used for index admin only (its bulk helpers re-serialize every doc).
- Org bytes are stored as compact `serialized_data` JSON and spliced into output via
  `orjson.Fragment` — never re-parsed.

### Modules (`pipeline/`)

| Module | Responsibility |
|---|---|
| `config.py` | All tunables from env: backend, workers, bulk bytes, in-flight per worker, shards, mapping variant, person file subset |
| `reader.py` | Stream one gz NDJSON file line by line (stdlib `gzip`); malformed lines → dead letter + counter |
| `orgstore/` | `OrgStore` interface: `put_many(pairs)`, `get_many(ids) -> {id: bytes}`; backends `redis`, `lmdb`, `sqlite`, `memory` (tests) |
| `transform.py` | Pure: person line + org lookup → document bytes |
| `indexer.py` | Index create/settings/mapping, async bulk send, retries, item-error handling, finalize |
| `metrics.py` | JSON-lines logging, shared counters, progress, `metrics.json` |
| `main.py` | Orchestration of phases, worker supervision, exit code |

Workers open their own store/HTTP connections after the process start. LMDB/SQLite files live on a
dedicated `stage` volume (not tmpfs — tmpfs counts against the 2 GB limit).

## 4. Document and mapping

Document `_id` = `forager_id`:

```jsonc
{
  ...person serialized_data (all fields kept in _source),
  "roles": [ { ...role, "organization_resolved": true | false } ],   // only when organization_id != null
  "organizations": [ <full org-feed record>, ... ],                   // resolved, deduped, role order
  "unresolved_organization_ids": [ ... ],                             // omitted when empty
  "affiliations": [ ...original serialized_data.organizations ]       // omitted when empty
}
```

Mapping: `dynamic: false` at root (all fields kept in `_source`, only mapped fields indexed; avoids
mid-load mapping updates and field explosion). Keyword fields use `ignore_above: 256` (the dynamic
default).

| Field | Mapping |
|---|---|
| `forager_id`, `linkedin_id` | `long` |
| `first_name`, `last_name`, `headline` | `text` |
| `country`, `city`, `industry`, `skills` | `keyword` |
| `linkedin_slug` | `keyword`, `doc_values: false` |
| `roles` | `object` (not `nested`: test `term` queries on subfields must match) |
| `roles.role_title` | `text` + `.keyword` |
| `roles.organization_id` | `long` |
| `roles.organization_resolved` | `boolean` |
| `roles.start_date`, `roles.end_date` | `date`, `ignore_malformed: true` |
| `organizations` | `object` |
| `organizations.forager_id`, `organizations.linkedin_id` | `long` |
| `organizations.name` | `text` + `.keyword` |
| `organizations.domain`, `.industry`, `.country` | `keyword` |
| `organizations.technologies`, `.keywords` | **variant**: `keyword` vs unmapped — chosen by benchmark (§6 #7) |
| `unresolved_organization_ids` | `long` |
| `affiliations` | `enabled: false` |
| everything else | unmapped (in `_source` only) |

Known trade-off: `object` flattens roles, so "title X at org Y" can match across different roles.
Production fix: a `nested` copy or a separate roles index. Documented in `EVALUATION.md`.

Index settings: during load `refresh_interval: -1`, `number_of_replicas: 0`,
`translog.durability: async`, larger `translog.flush_threshold_size`, shards from config. Finalize:
`refresh_interval: 1s`, `_refresh`. Replicas stay 0 (single node).

## 5. Error handling and observability

**Input:** malformed JSON, or missing `serialized_data` / `forager_id` → skip, count, append to
`out/dead_letter.ndjson` (file, line number, error, truncated raw). Unresolved org id → data, not error:
flagged and counted. Null `organization_id` → passed through, counted.

**Org store:** unreachable at startup → fail fast. Mid-run failure → bounded retries with backoff, then
abort (never index persons with silently missing orgs).

**ES bulk:** request-level failure (connection, 5xx, 429) → retry whole request, exponential backoff
with jitter, max N attempts. Item-level: 429 items → retry those items only; other 4xx → dead letter,
no retry. Retries are idempotent because `_id` = `forager_id`.

**Run:** `persons` index existing at start → deleted and recreated (logged). Worker non-zero exit →
abort, exit 1. Final verify: ES `_count` == successfully parsed persons − documents permanently rejected by ES
(malformed lines are never counted as persons); mismatch or any permanent
failure → exit non-zero.

**Observability:** JSON-lines logs to stdout (`ts`, `level`, `event`, fields; events `phase_start`,
`phase_end`, `progress`, `retry`, `dead_letter`, `summary`). Progress every 5 s from the parent via
shared counters: persons indexed, current and average persons/s, MB sent, retries, rejections,
unresolved refs, per-worker event-loop blocked time and semaphore wait time.
`out/metrics.json` at the end: phase timings (org load, persons, finalize), totals, **persons/s over
the full run** (pipeline start → final refresh) and phase-2-only persons/s, peak memory from cgroup
`memory.peak` (fallback: sum of worker RSS peaks), active config. `bench/perf.sh` prints from
`metrics.json` plus ES peak memory via `docker exec` reading its cgroup. `./out` is bind-mounted.

## 6. Benchmark and tuning plan

Harness `scripts/bench.py`: runs the pipeline in compose with env overrides against a fresh index, saves
each `metrics.json` to `out/bench/<label>.json`, records org-lookup latency, prints a comparison table.
Every run is followed by a quick correctness check (count + both term queries); a fast but wrong config
cannot win.

Method: one variable at a time, greedy. Quick sweeps on a subset of person files (org phase always
full); the top 2–3 configs are confirmed on full data, 3 runs each, median reported. Host page cache is
dropped before final runs.

| # | Knob | Values |
|---|---|---|
| 1 | Baseline | Redis, 4 workers, ~10 MB bulk, 2 in-flight/worker, 4 shards |
| 2 | Org store | redis / lmdb / sqlite (each with its best load strategy) |
| 3 | Bulk size (bytes-capped) | 5 / 10 / 20 MB |
| 4 | Shards | 2 / 4 / 6 / 8 |
| 5 | In-flight per worker | 1 / 2 / 3 / 4 (stop at 429s) |
| 6 | Workers | 4 / 5 / 6 (only if logs show I/O wait) |
| 7 | Mapping variant | lean vs `technologies` + `keywords` indexed |
| 8 | ES node | heap 3 / 4 / 5 GB; `indices.memory.index_buffer_size` 10% vs 30% |
| 9 | Minor | bulk gzip on/off |
| 10 | IDs (measurement only) | explicit `_id` vs auto id; explicit ships |

Compose changes: `redis` service (`--save "" --appendonly no`, healthcheck); ES heap via env with
default; tuning in `es-config/elasticsearch.yml`; volumes `out/` (bind) and `stage`. Pipeline limits
unchanged. Shipped defaults = best measured config, ES heap kept modest (~4 GB) so the stack fits a
16 GB grader machine; each tuned value commented.

## 7. Testing

Follows `.claude/skills/principle-test-behavior-not-implementation`: call the code as users do, assert
literal expected values; no call-assertions, no constant pins.

**Unit tests (5):**

1. `transform` with two roles at org 140717, lookup `{140717: b'{"forager_id":140717,"name":"Dell Technologies","technologies":["ASP.NET"]}'}`
   and input `organizations:[{"name":"KGI Club"}]` → output equals a handwritten dict: one org entry,
   `affiliations: [{"name":"KGI Club"}]`, both roles `organization_resolved: true`.
2. `transform` with a role at missing org 999 and a role with null `organization_id` → equals a
   handwritten dict: both roles kept, `organization_resolved: false` only on the 999 role,
   `unresolved_organization_ids: [999]`, `organizations: []`.
3. `OrgStore` parametrized over all backends: `put_many({1: b'{"a":1}'})`, `get_many([1, 2])` →
   `{1: b'{"a":1}'}`.
4. Fake bulk server rejects doc 2 with item-level 429 once; send docs 1, 2, 3 → server's stored ids
   `{1, 2, 3}`.
5. Fake bulk server returns 400 for doc 2 → dead-letter file has one line with `"_id": "2"` and the
   400 reason; failure count `1`; server stored `{1, 3}`.

**End-to-end:** fixture (`pipeline/tests/fixtures/`, ~20 persons, ~10 orgs, one malformed line)
through the real pipeline against compose ES + Redis; literal expectations hand-derived from the
fixture (count, both `.keyword` term counts, unresolved-person count, dead-letter lines = 1).

**Full-data:** `scripts/verify_index.py` asserts against the independent profiler's numbers
(1,000,000 · 7,081 · 647 · 3,638 unresolved refs · 1,858 persons with unresolved refs · 17,302 with
`affiliations`); then `bench/correctness.py` once the official `expected.json` arrives.

## 8. Deliverables touched

`pipeline/` (rewrite), `docker-compose.yml`, `es-config/elasticsearch.yml`, `bench/perf.sh`,
`scripts/bench.py`, `scripts/verify_index.py`, `README.md` (replaced), `EVALUATION.md`,
`AI_NOTES.md`, `CLAUDE.md` (updated with new commands once they exist).

## 9. Open items

- Official `bench/expected.json` requested from reviewers. If its Dell count is 570, it was derived from
  `roles[].organization_name`; our org-feed join yields 647 — to be raised with reviewers, not hacked
  around.
- Library pass after all benchmarks: `python-isal` (faster gunzip), `msgspec` with `Raw` (org load
  without parse/re-serialize), `hiredis` (only if Redis wins). Each is adopted only if it measurably
  improves throughput. `uvloop` is used from the start.
