# Forager Senior Full Stack Engineer — Take-Home

Welcome, and thanks for taking the time to do this.

**Budget**: aim for **1–2 hours** of your time. This is doable in that window
*if* you're fluent with AI coding tools. Hand-coded it would take 4–6+ hours.
That gap is the point — this exercise evaluates how you use AI tooling to
compress real engineering work.

**Submission window**: 5 calendar days from when this was sent to you.

**AI coding tools are expected, not optional.** Use Claude Code, Cursor, Copilot,
Codex, Aider, or whatever you actually use day-to-day. If you do not have a license 
we can offer you a temporary Claude Code license. We're going to read
[`AI_NOTES.md`](AI_NOTES.md) carefully — see *Deliverables* below.

---

## The problem

Forager delivers person and organization data to platform customers worldwide.
A typical pipeline takes raw person records (with employment roles referencing
organization IDs) and joins them against an organization dataset, then loads
the merged view into a search store powering our APIs.

Your job: build a high-performance pipeline that reads the two input
datasets, joins them, and indexes the merged result into a **single
Elasticsearch index** — inside a resource-constrained container
(**`mem_limit: 2g`**, **`cpus: 4.0`**).

You're free to add intermediate staging stores (bronze / silver / gold-style
layering) if it helps throughput — anything you can fit inside the
docker-compose environment.

**Your primary score is ingestion rate.** The faster your solution indexes
the full dataset, the higher you score.

### Inputs

Download the data bundle (`forager-takehome-data.zip`, ~1 GB):

**[https://drive.google.com/file/d/1EdnlIxk615H36rDw5NmLt_s-P4EK_0na/view?usp=sharing](https://drive.google.com/file/d/1EdnlIxk615H36rDw5NmLt_s-P4EK_0na/view?usp=sharing)**

Unzip it into [`./data/`](data/README.md). After extracting:

```
data/
├── person/data_0_N_0.json.gz         # 8 files, 1,000,000 person records total
└── organization/data_0_N_0.json.gz   # 8 files, ~529,000 organization records total
```

Records follow Forager's real data-feed format: each line is a JSON envelope
`{ id, date_updated, serialized_data: {...} }` where the rich record lives
inside `serialized_data`. **Files are NDJSON despite the `.json.gz`
extension** — one document per line, stream them; don't `json.load()` the
whole file.

Full schemas + example records are in [`data/README.md`](data/README.md).
Not every `roles[].organization_id` resolves in the orgs feed — handle
gracefully and flag unresolved references.

### Output

A single Elasticsearch index named **`persons`**, where each document is a
person record (the contents of `serialized_data`) with `organizations[]`
populated by joining the orgs feed on `roles[].organization_id`. The target
document shape is roughly:

```json
{
  "forager_id": 582216891,
  "first_name": "...",
  "last_name": "...",
  "headline": "...",
  "country": "...",
  "city": "...",
  "industry": "...",
  "linkedin_url": "...",
  "skills": ["..."],
  "...other top-level person fields from serialized_data...": "...",
  "roles": [
    {
      "role_title": "Information Technology Specialist",
      "organization_id": 140717,
      "organization_name": "Dell Technologies",
      "organization_domain": "delltechnologies.com",
      "organization_industry": "Computer Hardware",
      "...": "..."
    }
  ],
  "organizations": [
    {
      "forager_id": 140717,
      "name": "Dell Technologies",
      "linkedin_id": 15088102,
      "addresses": [], "locations": [], "keywords": [], "technologies": [],
      "...full org details from the orgs feed...": "..."
    }
  ]
}
```

Notes on the join:

- The envelope `{ id, date_updated, serialized_data }` must be unwrapped —
  `serialized_data` becomes the document body.
- `roles[].organization_id` joins against `id` in the organization dataset.
- The orgs feed contains fields **not** present in `roles[]` (`linkedin_id`,
  `addresses`, `keywords`, `technologies`, `funding_rounds`, etc.). A solution
  that ignores the orgs feed and copies role data into `organizations[]` will
  pass the basic tests but will lose code-quality points.
- Unresolved org references (role.organization_id not in the orgs feed) are
  expected — flag them, don't drop the role.

Mapping decisions (field types, nested vs. object on `roles` / `organizations`,
which fields to `index: false`, multi-fields) are yours to make — defend
them in [`EVALUATION.md`](EVALUATION.md). The pre-baked test suite in `bench/`
exercises the shape you ship (see *Evaluation* below).

You do **not** submit a populated index. The interviewer runs `docker compose up`
themselves against a fresh ES instance to reproduce your ingest. Report your
measured throughput in [`EVALUATION.md`](EVALUATION.md).

### Environment

`docker-compose.yml` is provided. It runs two services:

- `elasticsearch` — ES 8.13, single node, baseline config in `es-config/`.
  **No resource limits.** Give ES (and any other staging services you add)
  whatever CPU and memory you want — the grader matches whatever you specify
  in `docker-compose.yml`. Don't let the datastore be your bottleneck.
- `pipeline` — your container. Hard ceilings: **`mem_limit: 2g`**, **`cpus: 4.0`**.
  Do not raise these. The grader runs the pipeline against the same limits.

The compute constraint applies only to the `pipeline` service. If you add
staging services (Redis, Postgres, etc.), they're unconstrained too.

Below should build the boilerplate environment to start from. 

```bash
docker compose up --build
```

---

## What's already here

A boilerplate skeleton — feel free to rewrite any of it.

```
.
├── docker-compose.yml        # ES + pipeline services, resource limits set
├── es-config/
│   └── elasticsearch.yml     # baseline cluster config — tune this
├── pipeline/
│   ├── Dockerfile            # Python 3.12 slim
│   ├── requirements.txt      # elasticsearch + orjson, add what you need
│   └── main.py               # stub entrypoint with TODOs
├── data/                     # unzip the data bundle here
├── bench/
│   ├── correctness.py        # pre-baked tests — your pipeline must make these pass
│   ├── expected.json         # expected counts for the test suite
│   └── perf.sh               # stub — print your measured metrics
├── AI_NOTES.md               # template — graded
└── EVALUATION.md             # template — your own write-up
```

Rewrite the pipeline in any language you ship best in (Go, Rust, Node, TS,
etc.) — update the Dockerfile and compose command if you do. Python is
just the easy default.

---

## Deliverables

Submit a single git repo (GitHub link) containing:

1. **The working pipeline** — `docker compose up` runs end-to-end on a fresh
   machine with only Docker installed.
2. **Updated `README.md`** — how to run it, schema rationale, performance
   notes, known limitations. Replace this file with yours.
3. **[`AI_NOTES.md`](AI_NOTES.md)** — graded. Names the tools you used, the
   loops you ran, where you stepped in. See template.
4. **AI tooling artifacts committed to the repo** — if you wrote it to drive
   the work, ship it. Don't describe a custom skill in `AI_NOTES.md` without
   committing the actual file. Specifically:
   - `.claude/` — Claude Code skills, hooks, subagents, slash commands,
     `settings.json`, custom plugins
   - `CLAUDE.md` / `AGENTS.md` / equivalent project-level instructions
   - `.cursor/` — Cursor rules and settings
   - `.aider.conf.yml` / `.aiderignore` / Aider configs
   - `.mcp.json` or any MCP server configurations you wired up
   - Any custom system prompts, agent definitions, or scripts you authored
     for this task
   
   The interviewer reads these alongside `AI_NOTES.md` to verify the setup
   you described actually exists. Vague descriptions without committed
   artifacts score poorly on the AI fluency axis.
5. **[`EVALUATION.md`](EVALUATION.md)** — your evaluation of the pipeline.
6. **`bench/perf.sh`** — print the throughput numbers you measured. The
   interviewer runs this after a fresh `docker compose up` completes. Don't
   modify `bench/correctness.py` or `bench/expected.json` — those are
   pre-baked and ship as-is.

---

## Evaluation

Graded on four axes, weighted roughly equally.

### 1. Correctness (gate — must pass)

Run `python3 bench/correctness.py` after your ingest completes. The pre-baked
test suite checks:

1. **Total person doc count** — should be `1,000,000`
2. **Persons who have held a specific role title** — `term` query on
   `roles.role_title.keyword`
3. **Persons who have worked at a specific organization** — `term` query on
   `organizations.name.keyword`

The `.keyword` subfield gets created automatically by ES dynamic mapping
for string fields, so an explicit multi-field mapping is the safest choice.

Expected counts live in `bench/expected.json` (ships with the data bundle).
All three must pass. If they don't, the rest doesn't matter.

These same tests are what the interviewer runs — no separate internal grader.

### 2. Performance (objective metric)

Measured as **persons indexed per second** over the full run, on the constrained
container (`mem_limit: 2g`, `cpus: 4.0`).

### 3. Code quality

Reviewed as a PR. We look at:

- **Schema / mapping design** — explicit mapping vs. dynamic, field types,
  whether you used `index: false` / `doc_values: false` where appropriate
- **Streaming hygiene** — bounded memory reads from gzipped NDJSON; no
  "load all 1M records into a list"
- **Bulk ingest strategy** — batch sizing, concurrency, backpressure
- **Error handling at boundaries** — dangling refs, malformed rows, ES retries,
  bulk error responses
- **Observability** — structured logs, progress reporting, throughput metrics
- **Readability** — a senior engineer should be able to land this PR without
  rewriting it

### 4. AI workflow fluency (`AI_NOTES.md` + repo evidence)

We're explicitly evaluating *how* you used AI tooling. The bar:

- **Specificity** — named tools, real configs, concrete loops (not "I prompted
  it well")
- **Judgment** — clear thinking about where AI helped vs. where you stepped in
- **Leverage** — evidence the AI multiplied your output (the 4–6h gap should
  be visible in what you got done)
- **Control** — verify-loops, structured outputs, tests-as-rails, anything
  that kept the agent honest

A candidate who hand-wrote everything will not pass this section, even with
a fast pipeline. A candidate who blindly accepted agent output will not pass
it either.

---

## Ground rules

- **Any language, any libraries.** Update Dockerfile/compose if you switch.
- **AI tools expected.** Document them in `AI_NOTES.md`.
- **Everything runs inside docker-compose.** You can add staging services
  (Redis, Postgres, RabbitMQ, etc.) as additional compose services if they
  help your throughput. No managed/external services, no cloud functions,
  no remote queues.
- **Don't pre-process data outside the pipeline.** The grader runs your
  `docker compose up` against the raw NDJSON files in `./data/`.
- **Be honest in `AI_NOTES.md`.** We'll dig into it in the live technical
  (Stage 3).
- **If you get stuck**, document where and why in `EVALUATION.md`. A partial
  submission with a clear-eyed retro is more useful than one that hides gaps.

---

Good luck. We're excited to see what you do with this.

Forager
