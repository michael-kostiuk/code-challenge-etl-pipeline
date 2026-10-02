#!/usr/bin/env bash
# Performance report for the most recent run, read from out/metrics.json (written by the
# pipeline) plus the Elasticsearch container's cgroup peak memory.
set -euo pipefail
cd "$(dirname "$0")/.."

metrics=out/metrics.json
if [[ ! -f $metrics ]]; then
  echo "no $metrics — run 'docker compose up' and wait for the pipeline to finish" >&2
  exit 1
fi
es_peak=$(docker compose exec -T elasticsearch cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)

python3 - "$metrics" "$es_peak" <<'PYEOF'
import json, sys
m = json.load(open(sys.argv[1]))
es_peak = sys.argv[2].strip()
c = m["counters"]
gib = lambda b: f"{int(b) / 2**30:.2f} GiB"
print(f"persons indexed/s (full run):     {m['persons_per_s']:,.0f}")
print(f"persons indexed/s (person phase): {m['persons_phase_per_s']:,.0f}")
print(f"wall-clock total:                 {m['wall_clock_s']:.1f} s  ("
      + ", ".join(f"{k} {v:.1f}s" for k, v in m["phase_s"].items()) + ")")
print(f"documents in index:               {m['es_count']:,} (expected {m['expected_count']:,}, ok={m['ok']})")
print(f"peak memory, pipeline:            {gib(m['peak_memory_bytes'])} ({m['peak_memory_source']}, limit 2 GiB)")
print(f"peak memory, elasticsearch:       {gib(es_peak) if es_peak else 'n/a (container not running)'}")
print(f"unresolved org refs:              {c['unresolved_refs']:,} in {c['persons_with_unresolved']:,} persons")
print(f"retries / rejected / failed docs: {c['retries']} / {c['rejected_items']} / {c['failed_docs']}")
print(f"malformed input lines:            {c['malformed']} (dead_letter.ndjson)")
PYEOF
