# AI Usage Notes

> Be concrete. "I used Cursor" scores zero. Real configs, real loops, and real
> stories of where you stepped in score high. This file is graded.

## Tools used

List each tool with the model + version where relevant.

- Claude Code (Opus 5.5, 1M context) + Sonnet and Haiku subagents

## Configuration set up for this task

- **Custom skills / slash commands:**
  - `.claude/skills/thermo-nuclear-code-quality-review/SKILL.md` — copied from Cursor's
    cursor-team-kit plugin
    (https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md).
    Strict maintainability review (abstraction quality, file size, condition sprawl); run in a
    loop after implementation until it approved. Manual-only (`disable-model-invocation: true`).
  - `.claude/skills/principle-test-behavior-not-implementation/SKILL.md` — rule for writing or
    keeping tests: call code the way users do and assert on literal expected values.
    Manual-only. Also from the cursor-team-kit plugin.
- **Hooks:** none; for production there should be a linter at least.
- **MCP servers:** none.
- **Subagent configs:** none of my own. Subagent prompts and roles (implementer, spec reviewer,
  code reviewer) come from the superpowers plugin's subagent-driven-development flow, run on
  Claude Code's built-in subagents (Sonnet / Haiku).
- **Plugins:** superpowers (installed user-level, not in the repo). Used for brainstorm → spec →
  plan → subagent execution. Its outputs are committed:
  - `docs/superpowers/specs/2026-10-01-etl-pipeline-design.md` — design spec, including which
    alternatives to benchmark
  - `docs/superpowers/plans/2026-10-01-etl-pipeline.md` — step-by-step implementation and
    benchmark plan
- **System prompts / instructions:**
  - `CLAUDE.md` — generated with `/init`, then edited by hand: commands, hard constraints
    (2 GB / 4 CPU limit, don't touch `bench/correctness.py`, no preprocessing), join semantics,
    mapping requirements from the tests, deliverables to keep in sync.

## Agentic loops

1. `claude /init`
2. Evaluation of the main challenges before we see the data.
3. Profile the data to see metrics.
4. **superpowers: brainstorm** — investigate candidates and approaches before we jump to the
   actual spec.
5. **superpowers: spec** — created a spec with what we are actually trying to build here and
   which approaches we selected to benchmark for the final implementation.
6. **superpowers: plan** — created and reviewed a plan for what actual architecture will be
   introduced and how benchmarks will be resolved.
7. Subagent-driven plan execution.
8. `/thermo-nuclear-code-quality-review`
   ([.claude/skills/thermo-nuclear-code-quality-review/SKILL.md](.claude/skills/thermo-nuclear-code-quality-review/SKILL.md))
   loop until approved.
9. Spot-check indexed documents to verify integrity.
10. Custom agent runs to verify my expectations — whether asyncio is used correctly, whether the
    load on ES is applied the way I expect, etc.

## Where the agent helped

- Did full data profiling before we jumped to implementation, so I knew what I was dealing with.
- Brainstormed multiple ideas that I didn't consider from the start.
- Configured ES and Redis.
- Implemented, reviewed and tested the code :)
- Ran benchmarks to select the best approach and configuration options, and produced the final
  evaluation data.

## Where you stepped in

- Agent planned one (file-based) DLQ per process — overridden with a lock on a single DLQ.
- Agent kept focusing on local organization storage and lookup, suggesting LMDB and
  alternatives. I suggested Redis, but compromised by letting the agent benchmark both
  solutions; as expected, Redis performed better.
- I suggested using asyncio for workers to efficiently utilize concurrency and the waiting time
  for ES; the agent had planned threads.
- Agent created a custom logging implementation — overridden with the built-in `logging` module.

## Time leverage

- **Wall-clock time spent:** 2h 32m human time
- **Rough % of code AI-generated vs. you-wrote:** 100% AI
- **What would the 4–6 hour hand-coded version have done differently (or not at all)?**
  No benchmarks, and no org storage adapter unless required. Use pydantic-settings for more
  convenient configurability. I would have had a relatively simple setup of 4 processes that
  read chunks of data, join them and write to ES. I would avoid floating functions and use a
  more class-based approach, keeping only the required helpers in utils.

## Retro

- I would not let the agent sidetrack me into benchmarking solutions that are clearly worse
  than what I originally considered. As with Redis — we could try other "remote" storages like
  PostgreSQL, but doing it inside the process with constrained resources was not an option from
  the start.
- To fit the 1–2h expectation, I could do the superpowers flow
  brainstorm → spec → plan → implement → validate → review, skip benchmarking different
  approaches, and put sane defaults everywhere.
