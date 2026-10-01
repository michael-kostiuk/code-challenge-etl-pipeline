# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Forager take-home: an ETL pipeline that streams two gzipped NDJSON feeds (1M persons, ~529K orgs), joins them, and bulk-indexes the merged documents into a single Elasticsearch index named `persons`. **Primary score is ingestion rate (persons/sec)**, gated on `bench/correctness.py` passing. Code quality and AI-workflow evidence (`AI_NOTES.md`, committed `.claude/` artifacts) are also graded. The full brief is in `README.md`; data schemas are in `data/README.md`.

Currently `pipeline/main.py` is a stub (connects to ES and lists data files) — the pipeline itself is still to be built.

## Commands

```bash
docker compose up --build                 # ES 8.13 + pipeline, runs ingest end-to-end
docker compose up -d elasticsearch        # ES only, for iterating on the pipeline
docker compose run --rm pipeline          # re-run the pipeline against a running ES
docker compose down -v                    # wipe the es-data volume for a fresh run
python3 bench/correctness.py              # gate tests (stdlib only; ES_URL defaults to localhost:9200)
python3 scripts/verify_documents.py    # full-document sample check vs raw data (independent rebuild)
bench/perf.sh                             # throughput report (stub — must be implemented)
```

`./pipeline` is bind-mounted at `/app`, so edits to Python files take effect on the next `docker compose run` without rebuilding; changes to `requirements.txt` or the Dockerfile need `--build`.

## Hard constraints (do not violate)

- `pipeline` service: `mem_limit: 2g`, `cpus: 4.0` — never raise these. ES and any added staging services (Redis, Postgres, etc.) are unconstrained and may be tuned freely in `docker-compose.yml` / `es-config/elasticsearch.yml`.
- Do not modify `bench/correctness.py` or `bench/expected.json`. (The `expected.json` in the repo has placeholder `0` counts; the real one ships in the data bundle.)
- No preprocessing of data outside the pipeline; everything must run from `docker compose up` on a fresh machine with only Docker.
- Any language is allowed; if switching from Python, update `pipeline/Dockerfile` and the compose `command`.

## Data and join semantics

- Input lives in `data/person/*.json.gz` and `data/organization/*.json.gz` (8 files each, gitignored, mounted read-only at `/data`). Files are **NDJSON**, not JSON arrays — stream line by line; never load a whole file or all 1M records into memory.
- Each line is an envelope `{id, date_updated, serialized_data}`; `serialized_data` becomes the ES document body.
- Join: `person.roles[].organization_id` → org `serialized_data.forager_id` (same as envelope `id`). Populate `organizations[]` (empty in the input) with the **full org record from the orgs feed** — not with data copied from `roles[]`, which costs code-quality points.
- Unresolved org IDs are expected: keep the role, flag the unresolved reference, don't crash.

## Mapping requirements imposed by the tests

`bench/correctness.py` runs `term` queries on `roles.role_title.keyword` and `organizations.name.keyword`, plus a `match_all` count expecting 1,000,000 docs. An explicit mapping must provide these `.keyword` multi-fields. Note that `term` on an inner field of a `nested` type would not match from a top-level query, so `roles`/`organizations` must stay `object` type (or the tests will fail). Ensure the index is refreshed before running tests.

## Deliverables to keep in sync

`README.md` (replace with own), `AI_NOTES.md` and `EVALUATION.md` (templates to fill in with real numbers), and `bench/perf.sh` (must print persons/sec, wall-clock total, peak pipeline memory — typically read from a metrics file the pipeline writes).
