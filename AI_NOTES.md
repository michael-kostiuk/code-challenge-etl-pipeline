# AI Usage Notes

> Be concrete. "I used Cursor" scores zero. Real configs, real loops, and real
> stories of where you stepped in score high. This file is graded.

## Tools used

List each tool with the model + version where relevant.

- Claude Code (Opus 5.5, 1M context) + Sonnet and Haiku subagents


## Configuration set up for this task

- Custom skills / slash commands:
  - `.claude/skills/thermo-nuclear-code-quality-review/SKILL.md` — copied from Cursor's
    cursor-team-kit plugin
    (https://github.com/cursor/plugins/blob/main/cursor-team-kit/skills/thermo-nuclear-code-quality-review/SKILL.md).
    Strict maintainability review (abstraction quality, file size, condition sprawl); run in a
    loop after implementation until it approved. Manual-only (`disable-model-invocation: true`).
  - `.claude/skills/principle-test-behavior-not-implementation/SKILL.md` — rule for writing or
    keeping tests: call code the way users do and assert on literal expected values. Manual-only. Also cursor-team-kit plugin.
- Hooks: none, for production there should be linter at least.
- MCP servers: none.
- Subagent configs: none of my own. Subagent prompts and roles (implementer, spec reviewer,
  code reviewer) come from the superpowers plugin's subagent-driven-development flow, run on
  Claude Code's built-in subagents (Sonnet / Haiku).
- Plugins: superpowers (installed user-level, not in the repo). Used for brainstorm → spec → plan
  → subagent execution. Its outputs are committed:
  - `docs/superpowers/specs/2026-10-01-etl-pipeline-design.md` — design spec, including which
    alternatives to benchmark
  - `docs/superpowers/plans/2026-10-01-etl-pipeline.md` — step-by-step implementation and
    benchmark plan
- System prompts / instructions:
  - `CLAUDE.md` — generated with `/init`, then edited by hand: commands, hard constraints
    (2 GB / 4 CPU limit, don't touch `bench/correctness.py`, no preprocessing), join semantics,
    mapping requirements from the tests, deliverables to keep in sync.

## Agentic loops

claude init
evaluation of main challenges before we see data
profile data to see metrics
superpowers: brainstorm - investigate candidates and approaches before we jump to actual spec
superpowers: spec - created spec with we are actually trying to build here, what approaches we selected for benchmark to be selected for final impolementation
superpowers: plan - created and reviewed plan what actual architecture will be introduced, and how benchmarks will be resolved
subagent driven plan execution
/thermo-nuclear code review[.claude/skills/thermo-nuclear-code-quality-review/SKILL.md] loop until approved
spot check indexed  documents to verify integrity
custom agent runs to verify my expectation - if asycio used correctly, if load on ES is given the way I expect it, etc.

## Where the agent helped

Done full data profiling before we jump to implementation so I know what I'm dealing with
Brainstormed multiple ideas that I didn't consider from the start
Configured ES and Redis
Implemented, reviewed and tested the code :)
Calculated benchmarks to select best approach and configuration options, and final evaluation data

## Where you stepped in

agent planned one [file-based] dlq per process - overriden with lock on single dql
agent keep focusing on local organization storage and lookup suggesting LMDB and alternatives, I suggested Redis but compromized to let agent benchmark both solutions, as expected redis performed better
suggested using asyncio for workers to efficiently utilize concurrency and waiting time for ES, agent planned threads
agent created custom logging implementation - overriden with use of built in logging module

## Time leverage

- Wall-clock time spent: 2:32m human time
- Rough % of code AI-generated vs. you-wrote: 100% AI
- What would the 4–6 hour hand-coded version have done differently (or not at all)? 
No benchmarks, no org storage adapter unless required. Utilize pydantic-settings for more conviniet configurability. I would have relatively simple 4 processes that reads chunks of data, joins and writes to ES. I would avoid floating functions and utilize more class based approach, having only required helpers in utils.

## Retro

I would not let agent sidetrack me considering benmarking solutions that are clearly workse that I considered originally, like with redis, we could try other "remote" storages like postgresql, but doing it inside process with constrained resources was not an option from the start. 
To fit 1-2h expectation I could do superpowers flow brainstorm-spec-plan-implement-validate-review, and not focus on benchmarking different approaches, put sane defaults everywhere.
