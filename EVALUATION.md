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
  (76.1 s), SQLite 1,350 (206.0 s) (single runs, earlier loaded VM). LMDB and SQLite also hit the 2 GiB cgroup
  ceiling (page cache of the mapped/db file counts) and failed the memory gate.
  Code is Redis-only; the losers are kept unwired in `scripts/reference/alt_orgstores.py`.
- **Auto-generated `_id`** (append-only path). Interleaved A/B, 3 runs each, full
  data: auto median 10,972 p/s (10,610-10,997) vs explicit 11,151 (10,920-11,676),
  no measurable gain. Exactly-once is enforced by retrying only clean rejections;
  ambiguous failures fail the run; the final count check catches duplicates. Lookup
  by person goes through a `term` query on `forager_id`.
- **`object`, not `nested`, for `roles` / `organizations`.** Required by the
  test `term` queries; cost: cross-role matches ("title X at org Y" can match
  different roles).
- **`dynamic: false`.** Everything stays in `_source`; only searched fields are
  indexed. Org `technologies` / `keywords` are not indexed: indexing them
  cost 8.0% (full data, median of 3: 8,500 vs 9,238 p/s), under 10% but inside
  the 10-13% within-arm spread, so not separable from noise; the opt-in flag
  was removed.
- **Input `organizations[]` → `affiliations`.** 17,302 persons already carry
  LinkedIn affiliations there; moved, not overwritten, so `organizations[]`
  means "joined employers" only.
- **Unresolved refs flagged, not dropped.** `roles[].organization_resolved`
  plus `unresolved_organization_ids` (3,638 refs, 1,858 persons).
- **Tuning sweeps** (`scripts/bench.py`). The first single-run sweeps ran on an
  overloaded VM with +/-20-35% noise; decisions were redone with interleaved
  A/B runs, 3 per arm, on a 20 GB VM. Noise now: identical defaults
  (`ab2-noise`, 2 runs) 10,996 / 15,170 p/s phase (32% spread); arm
  spreads 10-16% (one 40%). Rule: adopt only if the median wins by >5%.
  - **Shards 2 vs 4:** subset (phase) 15,293 vs 13,518 (+13% for 2); full data
    (p/s, 3 runs each) 8,400 vs 9,104 (+8.4% for 4). Score is full-run p/s, so 4.
  - **ES index buffer 30% vs 10%** (2 files, phase): 12,264 vs 12,475 (-2%).
    Reverted to 10%.
  - **Org arrays indexed 1 vs 0** (full, p/s): 8,500 vs 9,238 (-8.0%, inside
    spread). Reverted to 0.
  - Bulk 10 MB, in-flight 2, workers 4, heap 4g: no single-run sweep beat them
    beyond noise.
  - **Net result:** final = the 4-shard `ab3` runs (3 full, verified): median
    9,104 p/s (7,566-9,469) vs 10,202 for one `ab2-startdefaults` run (same
    config). No gain from tuning; the defaults are the starting ones, and the
    gap between those two measurements is run-to-run noise.
