# Pipeline Evaluation

Your own evaluation of your submission. Numbers, not adjectives.

## Performance

- Persons indexed per second (full-run average):
- Wall-clock total:
- Peak memory (pipeline container):
- Peak memory (elasticsearch container):
- How you measured these:

## Correctness

What you verified, with counts:

- Person document count:
- Roles document count:
- Unresolved-org-ref count and percentage:
- Sample queries you ran and what they returned:


## Trade-offs

Schema decisions, join strategy (denormalized vs. parent-child vs. app-side
join), why you picked what you picked.

- **Join: denormalized, app-side.** Full org-feed records are embedded in
  `organizations[]` at ingest. Orgs (1.3 GB uncompressed, 99.4% referenced)
  live in an external org store, not pipeline memory; backend (Redis / LMDB /
  SQLite) chosen by benchmark: Redis 6,142 p/s (org load 12.1 s), LMDB 1,620
  (76.1 s), SQLite 1,350 (206.0 s). LMDB and SQLite also hit the 2 GiB cgroup
  ceiling (page cache of the mapped/db file counts) and failed the memory gate.
- **`object`, not `nested`, for `roles` / `organizations`.** Required by the
  test `term` queries; cost: cross-role matches ("title X at org Y" can match
  different roles).
- **`dynamic: false`.** Everything stays in `_source`; only searched fields are
  indexed. Org `technologies` / `keywords` indexed: 5,941 p/s vs 6,471 p/s
  lean (-8.2%, under the 10% bar).
- **Input `organizations[]` → `affiliations`.** 17,302 persons already carry
  LinkedIn affiliations there; moved, not overwritten, so `organizations[]`
  means "joined employers" only.
- **Unresolved refs flagged, not dropped.** `roles[].organization_resolved`
  plus `unresolved_organization_ids` (3,638 refs, 1,858 persons).
- **Tuning sweeps** (`scripts/bench.py`; phase sweeps on `PERSON_FILES=2`, one run
  each, run-to-run noise about +/-20-35%, so small gaps are not significant):
  - **Bulk size:** 5 / 10 / 20 MB = 7,557 / 7,590 / 4,090 p/s (phase). Kept 10 MB.
  - **Shards:** 2 / 4 / 6 / 8 = 11,404 / 10,202 / 8,888 / 10,198 p/s (phase).
    Default changed 4 -> 2 per the highest-wins rule (gap to 4 is within noise).
  - **In-flight per worker:** 1 / 2 / 3 / 4 = 10,214 / 11,681 / 11,011 / 10,035
    p/s (phase), 0 rejected items everywhere. Kept 2.
  - **Workers:** 4 / 5 / 6 = 11,930 / 11,222 / 11,759 p/s (phase). No gain over
    4 (+3% bar not met); kept 4. Best run's `slot_wait_s` 7.7 s was under 10% of
    persons x workers (8.4 s), i.e. not ES-bound at this size.
  - **ES heap:** 3g / 4g / 5g = 6,695 / 9,420 / 7,557 p/s (phase). Kept 4g (the
    cap; 5g did not win).
  - **ES index buffer:** 10% -> 30% = 9,420 -> 9,970 p/s (phase, +5.8%, within
    noise). Default changed to 30%.
  - **Final (3 full runs, warm page cache, `sudo` unavailable):** 5,465 / 4,274 /
    5,799 p/s, median 5,465; all verified.
