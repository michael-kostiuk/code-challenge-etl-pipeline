# Pipeline Evaluation

Your own evaluation of your submission. Numbers, not adjectives.

## Performance

- Persons indexed per second (full-run average): 8,893 (person phase only: 9,754)
- Wall-clock total: 112.5 s (org load 7.1 s, index setup 0.4 s, persons 102.5 s, finalize 2.3 s)
- Peak memory (pipeline container): 0.64 GiB, cgroup `memory.peak` (limit 2 GiB)
- Peak memory (elasticsearch container): 11.90 GiB, cgroup, includes page cache (4 GiB heap)
- How you measured these: the pipeline writes `out/metrics.json` (phase timings, counts, persons/s,
  cgroup `memory.peak`); `bench/perf.sh` prints it and reads the Elasticsearch container's cgroup
  `memory.peak`. One clean run (`docker compose down -v`, then `up --build`) on a 16 vCPU / 20 GB VM.
  Across 22 verified full-data runs on this VM (`out/bench/`), throughput ranged 7,506–11,676 p/s; the figures above are one clean run.

## Correctness

What you verified, with counts:

- Person document count: 1,000,000 (`_count` after the final refresh; no failed or duplicated documents)
- Roles document count: there is no separate roles index, roles are embedded in person documents.
  Total roles: 2,083,023; role-to-organization references: 1,385,337 (`scripts/profile_report.json`)
- Unresolved-org-ref count and percentage: 3,638 of 1,385,337 references (0.26%), in 1,858 persons
- Sample queries you ran and what they returned:
  - `roles.role_title.keyword` = "Project Manager": 7,081
  - `organizations.name.keyword` = "Dell Technologies": 647. Counting by `roles[].organization_name`
    gives 570; the pipeline joins to the org feed record, so it uses the org-feed name. Pending the
    official `bench/expected.json` (the repo copy has 0 placeholders, so those two checks of
    `bench/correctness.py` report 0 expected; the total count check passes).
- `scripts/verify_index.py`: all 8 checks pass (counts above, 17,302 persons with `affiliations`,
  0 malformed input lines, pipeline peak memory under 1.9 GiB).
- `scripts/verify_documents.py`: rebuilds 2,759 sampled persons (2,000 uniform plus targeted samples:
  affiliations, unresolved refs, duplicate-org roles, null-org roles, the largest documents) from the
  raw files and compares them with the indexed documents: 2,759 matched, 0 mismatched, 0 missing,
  0 duplicated.
- The profiler (`scripts/profile_data.py`) and the document check use code separate from the
  pipeline. `pytest` (8 tests) passes.

## Trade-offs

- **Join: denormalized, app-side, Redis as the org store.** Full org-feed records are embedded in
  `organizations[]` at ingest. Orgs (1.3 GB uncompressed, 99.4% referenced) do not fit in a 2 GiB
  process as Python objects. Redis was about 4x faster than LMDB and SQLite (6,142 vs 1,620 and 1,350
  persons/s), and those two also exceeded the 2 GiB memory limit through page cache. Alternatives are
  kept in `scripts/reference/`.
- **`object`, not `nested`, for `roles` / `organizations`.** Required by the test `term` queries;
  cost: cross-role matches ("title X at org Y" can match different roles).
- **`dynamic: false`, lean mapping.** Everything stays in `_source`; only searched fields are
  indexed. Org `technologies` / `keywords` are kept in `_source` but not indexed, to keep indexing lean.
- **Input `organizations[]` -> `affiliations`.** 17,302 persons already carry LinkedIn affiliations
  there; moved, not overwritten, so `organizations[]` means "joined employers" only.
- **Unresolved refs flagged, not dropped.** `roles[].organization_resolved` plus
  `unresolved_organization_ids` (3,638 refs, 1,858 persons).
- **Auto-generated `_id`.** Retries only clean rejections (whole-request 429/503, item-level
  429/5xx, connection never established); ambiguous failures fail the run, and the final count check
  catches duplicates. Measured against explicit ids: no throughput difference (10,972 vs 11,151
  persons/s (median of 3 dedicated benchmark runs each, separate from the final run above)); auto ids kept by choice, with the exactly-once guard above.
- **Tuning.** Defaults: 4 workers, 4 shards, 10 MB bulk, 2 in-flight per worker, ES heap 4g, index
  buffer 10%, refresh disabled during load, async translog.
