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
  SQLite) chosen by benchmark — _TBD_.
- **`object`, not `nested`, for `roles` / `organizations`.** Required by the
  test `term` queries; cost: cross-role matches ("title X at org Y" can match
  different roles).
- **`dynamic: false`.** Everything stays in `_source`; only searched fields are
  indexed. Org `technologies` / `keywords` indexing decided by measured cost —
  _TBD_.
- **Input `organizations[]` → `affiliations`.** 17,302 persons already carry
  LinkedIn affiliations there; moved, not overwritten, so `organizations[]`
  means "joined employers" only.
- **Unresolved refs flagged, not dropped.** `roles[].organization_resolved`
  plus `unresolved_organization_ids` (3,638 refs, 1,858 persons).

