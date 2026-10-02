# Pipeline Evaluation

Your own evaluation of your submission. Numbers, not adjectives.

## Performance

- Persons indexed per second (full-run average): 27,383 (person phase only: 32,605)
- Wall-clock total: 36.5 s (org load 2.9 s, index setup 0.1 s, persons 30.7 s, finalize 2.8 s)
- Peak memory (pipeline container): 0.85 GiB, cgroup `memory.peak` (limit 2 GiB)
- Peak memory (elasticsearch container): 5.25 GiB, cgroup, includes page cache (4 GiB heap)
- How you measured these: the pipeline writes `out/metrics.json` (phase timings, counts, persons/s,
  cgroup `memory.peak`); `bench/perf.sh` prints it and reads the Elasticsearch container's cgroup
  `memory.peak`. 

## Correctness

What you verified, with counts:

- Person document count: 1,000,000 (`_count` after the final refresh; no failed or duplicated documents)
- Roles document count: there is no separate roles index, roles are embedded in person documents.
  Total roles: 2,083,023; role-to-organization references: 1,385,337 (`scripts/profile_report.json`)
- Unresolved-org-ref count and percentage: 3,638 of 1,385,337 references (0.26%), in 1,858 persons
- Sample queries you ran and what they returned (against the index from the clean run above). The
  first three are the `_count` bodies `bench/correctness.py` sends:

  ```bash
  curl -s -H 'Content-Type: application/json' localhost:9200/persons/_count \
    -d '{"query":{"match_all":{}}}'
  # {"count":1000000,"_shards":{"total":4,"successful":4,"skipped":0,"failed":0}}

  curl -s -H 'Content-Type: application/json' localhost:9200/persons/_count \
    -d '{"query":{"term":{"roles.role_title.keyword":"Project Manager"}}}'
  # {"count":7081,"_shards":{"total":4,"successful":4,"skipped":0,"failed":0}}

  curl -s -H 'Content-Type: application/json' localhost:9200/persons/_count \
    -d '{"query":{"term":{"organizations.name.keyword":"Dell Technologies"}}}'
  # {"count":647,"_shards":{"total":4,"successful":4,"skipped":0,"failed":0}}

  # persons with at least one unresolved org reference (matches the profiler's 1,858)
  curl -s -H 'Content-Type: application/json' localhost:9200/persons/_count \
    -d '{"query":{"exists":{"field":"unresolved_organization_ids"}}}'
  # {"count":1858,"_shards":{"total":4,"successful":4,"skipped":0,"failed":0}}

  curl -s -H 'Content-Type: application/json' localhost:9200/persons/_count \
    -d '{"query":{"term":{"roles.organization_resolved":false}}}'
  # {"count":1858,"_shards":{"total":4,"successful":4,"skipped":0,"failed":0}}
  ```

  A joined document: "Project Manager" at "Dell Technologies" (25 persons; `_source` trimmed to the
  join fields). Role `organization_id` 140717 resolves to the org-feed record for Dell; the two roles
  at that org produce one `organizations[]` entry.

  ```bash
  curl -s -H 'Content-Type: application/json' \
    'localhost:9200/persons/_search?filter_path=hits.total,hits.hits._source' -d '{
      "size": 1, "track_total_hits": true,
      "query": {"bool": {"filter": [
        {"term": {"roles.role_title.keyword": "Project Manager"}},
        {"term": {"organizations.name.keyword": "Dell Technologies"}}]}},
      "_source": ["forager_id", "roles.role_title", "roles.organization_id", "roles.organization_resolved",
                  "organizations.forager_id", "organizations.name", "organizations.domain"]}'
  ```

  ```json
  {"hits": {"total": {"value": 25, "relation": "eq"}, "hits": [{"_source": {
    "forager_id": 481598581,
    "roles": [
      {"organization_id": 1639,   "role_title": "SAP Consultant", "organization_resolved": true},
      {"organization_id": 48,     "role_title": "Team Lead and Consultant", "organization_resolved": true},
      {"organization_id": 140717, "role_title": "Project Manager", "organization_resolved": true},
      {"organization_id": 132090, "role_title": "SAP Manager", "organization_resolved": true},
      {"organization_id": 140717, "role_title": "Project Manager | Technical Team Manager | Solution Architect", "organization_resolved": true},
      {"organization_id": 143969, "role_title": "Associate Director", "organization_resolved": true},
      {"organization_id": 273,    "role_title": "Leader of CGI SAP Innovation and COE Practice", "organization_resolved": true},
      {"organization_id": 69,     "role_title": "Director Strategic Solutions", "organization_resolved": true},
      {"organization_id": 69,     "role_title": "Chief Enterprise Architect", "organization_resolved": true}
    ],
    "organizations": [
      {"domain": "covansys.com",         "forager_id": 1639,   "name": "Covansys"},
      {"domain": "capgemini.com",        "forager_id": 48,     "name": "Capgemini Invent"},
      {"domain": "delltechnologies.com", "forager_id": 140717, "name": "Dell Technologies"},
      {"domain": "imcsgroup.net",        "forager_id": 132090, "name": "IMCS Group"},
      {"domain": "nttdata.com",          "forager_id": 143969, "name": "NTT DATA North America"},
      {"domain": "cgi.com",              "forager_id": 273,    "name": "CGI"},
      {"domain": "sap.com",              "forager_id": 69,     "name": "SAP"}
    ]}}]}}
  ```

  Dell: counting by `roles[].organization_name` gives 570; the pipeline joins to the org-feed record,
  so it uses the org-feed name (647). The official `bench/expected.json` was not in the data bundle;
  the repo copy holds these computed counts (placeholder kept in `bench/expected.original.json`), and
  `bench/correctness.py` passes all three checks.
- `scripts/verify_index.py`: all 8 checks pass (counts above, 17,302 persons with `affiliations`,
  0 malformed input lines, pipeline peak memory under 1.9 GiB).
- `scripts/verify_documents.py`: rebuilds 2,759 sampled persons (2,000 uniform plus targeted samples:
  affiliations, unresolved refs, duplicate-org roles, null-org roles, the largest documents) from the
  raw files and compares them with the indexed documents: 2,759 matched, 0 mismatched, 0 missing,
  0 duplicated.
- The profiler (`scripts/profile_data.py`) and the document check use code separate from the
  pipeline. `pytest` (18 tests) passes.

## Trade-offs

- **Join: denormalized, app-side, Redis as the org store.** Full org-feed records are embedded in
  `organizations[]` at ingest time.
  - Cost: network latency, one MGET round-trip per batch instead of an in-memory lookup.
  - But orgs are 1.3 GB uncompressed and 99.4% of them are referenced, so they don't fit in a
    2 GiB process as Python objects.
- **No in-process org cache.** Every lookup goes to Redis.
  - Org references are long-tailed (416K of 526K orgs referenced once), so a 500 MB LRU serves
    only 29% of lookups, still needs one MGET per batch, and adds 0.3 GiB pipeline peak memory.
  - ES is the bottleneck, so saving Redis time doesn't turn into throughput.
- **Explicit mapping with `dynamic: false`.** Only fields worth querying are indexed; everything
  else stays in `_source` but can't be queried without a mapping change and reindex.
  - Org `technologies` / `keywords` stay unindexed: indexing them was 8% slower.
  - Identifiers (`forager_id`, `linkedin_id`, `organization_id`, ...) are `keyword`, not `long`: they
    are only matched exactly, and `term` on `keyword` is the faster lookup. `first_name` / `last_name`
    get a `.keyword` for exact match, sorting and aggregations; person and org `date_updated` are
    indexed as dates in the feed's own format (`Z` or an offset like `-0700`), 0 `_ignored` values on
    the full data.
- **`object`, not `nested`, for `roles` / `organizations`.** Required by the `term` queries in the
  tests. Cost: cross-role matches ("title X at org Y" can match two different roles).
- **Input `organizations[]` → `affiliations`.** 17,302 persons already have LinkedIn affiliations
  there. Moved, not overwritten, so `organizations[]` means "joined employers" only.
- **Unresolved refs flagged, not dropped.** `roles[].organization_resolved` plus
  `unresolved_organization_ids` (3,638 refs in 1,858 persons).
- **Dead letters are reported, not fatal.** Malformed input (including an envelope `id` that differs
  from `forager_id`, or `roles` the join can't read) and documents Elasticsearch rejects go to
  `out/dead_letter.ndjson` and the counters, and the `summary` log is a warning. The run fails only when
  the index count differs from the accepted documents, i.e. something was lost or duplicated. One bad
  record shouldn't cost a 1M-document load; the dead-letter file is the replay list.
- **Auto-generated `_id`.** Retries aren't idempotent, so a retried request could create
  duplicates.
  - Only clean rejections are retried (whole-request 429/503, item-level 429/5xx, connection never
    established); ambiguous failures fail the run, and the final count check catches duplicates.
- **Refresh disabled and async translog during load.** Faster indexing, but documents aren't
  searchable until the final refresh and durability is weaker while loading.
- **2 in-flight bulk requests per worker.** Lowest concurrency that keeps ES saturated: 1 leaves
  ES write threads idle, 3 only queues requests inside ES.
