# AI Usage Notes

> Be concrete. "I used Cursor" scores zero. Real configs, real loops, and real
> stories of where you stepped in score high. This file is graded.

## Tools used

List each tool with the model + version where relevant.

- e.g. Claude Code (Opus 4.7, 1M context)
- e.g. Cursor (composer mode)
- e.g. Codex CLI, Aider, etc.

## Configuration set up for this task

**Commit the actual files to the repo** (`.claude/`, `CLAUDE.md`, `.cursor/`,
`.aider.conf.yml`, `.mcp.json`, etc.) — see Deliverable #4 in the brief. List
them here with a one-line description of what each does. Don't paraphrase a
config and skip committing it; the interviewer reads the real files.

- Custom skills / slash commands (e.g. `.claude/skills/...`):
- Hooks (e.g. `.claude/settings.json`):
- MCP servers (e.g. `.mcp.json`):
- Subagent configs (e.g. `.claude/agents/...`):
- Plugins (e.g. `.claude/plugins/...`):
- System prompts / `CLAUDE.md` / `AGENTS.md` / instructions files:

## Agentic loops

claude init
evaluation of main challenges before we see data
profile data to see metrics
superpowers: brainstorm - investigate candidates and approaches before we jump to actual spec
superpowers: spec - created spec with we are actually trying to build here, what approaches we selected for benchmark to be selected for final impolementation
superpowers: plan - created and reviewed plan what actual architecture will be introduced, and how benchmarks will be resolved
subagent driven plan execution
/thermo-nuclear code review loop until approved
spot check indexed  documents

<!-- Describe the loop structures you ran — not "I prompted X" but the shape.

- Research → plan → implement → verify?
- Multi-agent fan-out for parallel exploration?
- Test-driven loop with auto-fix?
- Structured-output validators? -->

## Where the agent helped

Specific examples. "Claude generated the bulk-index batching in one shot;
I accepted it after reading the diff."

## Where you stepped in

Specific examples — agent decisions you overrode and why. This is the most
important section.

agent planned one [file-based] dlq per process - overriden with lock on single dql

## Time leverage

- Wall-clock time spent:
- Rough % of code AI-generated vs. you-wrote:
- What would the 4–6 hour hand-coded version have done differently (or not at all)?

## Retro

What would you do differently next time?
