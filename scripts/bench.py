#!/usr/bin/env python3
"""Run one pipeline configuration under docker compose and record its metrics.

  python3 scripts/bench.py LABEL [-e KEY=VALUE ...] [--es-heap 4g] [--es-buffer 10%] [--runs N] [--verify]
  python3 scripts/bench.py --table

-e values are pipeline env overrides (see pipeline/etl/config.py). --es-heap/--es-buffer recreate
Elasticsearch with that heap / indices.memory.index_buffer_size. Each run starts from a fresh
index and org store (the pipeline resets both). Results: out/bench/<label>-<n>.json.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = ROOT / "out" / "bench"


def compose(*args: str, env: dict | None = None) -> int:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, env=env).returncode


def run_once(label: str, overrides: list[str], es_env: dict, n: int, verify: bool) -> None:
    env = {**os.environ, **es_env}
    if compose("up", "-d", "--wait", "elasticsearch", "redis", env=env) != 0:
        sys.exit("services failed to start")
    flags = [arg for kv in overrides for arg in ("-e", kv)]
    exit_code = compose("run", "--rm", *flags, "pipeline", env=env)
    result = json.loads((ROOT / "out" / "metrics.json").read_text()) if exit_code == 0 else {}
    result |= {"label": label, "run": n, "exit_code": exit_code, "overrides": overrides, "es_env": es_env}
    if verify and exit_code == 0:
        result["verified"] = subprocess.run([sys.executable, str(ROOT / "scripts" / "verify_index.py")]).returncode == 0
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    (BENCH_DIR / f"{label}-{n}.json").write_text(json.dumps(result, indent=2))


def print_table() -> None:
    rows = [json.loads(p.read_text()) for p in sorted(BENCH_DIR.glob("*.json"))]
    header = f"{'label':<28}{'run':>4}{'ok':>6}{'p/s full':>10}{'p/s phase':>11}{'wall s':>9}{'org s':>8}{'peak GiB':>10}  settings"
    print(header)
    print("-" * len(header))
    for r in rows:
        ok = r.get("ok", False) and r.get("verified", True)
        settings = " ".join(r["overrides"] + [f"{k}={v}" for k, v in r["es_env"].items()])
        print(f"{r['label']:<28}{r['run']:>4}{str(ok):>6}{r.get('persons_per_s', 0):>10,.0f}"
              f"{r.get('persons_phase_per_s', 0):>11,.0f}{r.get('wall_clock_s', 0):>9.1f}"
              f"{r.get('phase_s', {}).get('org_load', 0):>8.1f}{r.get('peak_memory_bytes', 0) / 2**30:>10.2f}  {settings}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("label", nargs="?")
    parser.add_argument("-e", dest="overrides", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--es-heap")
    parser.add_argument("--es-buffer")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--verify", action="store_true", help="run scripts/verify_index.py (full data only)")
    parser.add_argument("--table", action="store_true")
    args = parser.parse_args()
    if args.table:
        print_table()
        return
    if not args.label:
        parser.error("LABEL is required unless --table")
    es_env = {k: v for k, v in (("ES_HEAP", args.es_heap), ("ES_INDEX_BUFFER", args.es_buffer)) if v}
    for n in range(1, args.runs + 1):
        run_once(args.label, args.overrides, es_env, n, args.verify)
    print_table()


if __name__ == "__main__":
    main()
