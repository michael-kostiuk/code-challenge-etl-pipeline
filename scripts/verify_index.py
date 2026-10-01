#!/usr/bin/env python3
"""Full-data correctness check: the live index and out/metrics.json against numbers computed
independently by scripts/profile_data.py (see scripts/profile_report.json). Stdlib only.

Run after a full ingest:  python3 scripts/verify_index.py
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

ES_URL = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
INDEX = "persons"
METRICS = Path(__file__).resolve().parent.parent / "out" / "metrics.json"


def es_count(query: dict) -> int:
    req = urllib.request.Request(
        f"{ES_URL}/{INDEX}/_count", data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["count"]


def main() -> int:
    metrics = json.loads(METRICS.read_text())
    checks = [
        ("total persons", es_count({"match_all": {}}), 1_000_000),
        ('role title "Project Manager"', es_count({"term": {"roles.role_title.keyword": "Project Manager"}}), 7_081),
        ('org name "Dell Technologies"', es_count({"term": {"organizations.name.keyword": "Dell Technologies"}}), 647),
        ("persons with unresolved org refs", es_count({"exists": {"field": "unresolved_organization_ids"}}), 1_858),
        ("unresolved role references", metrics["counters"]["unresolved_refs"], 3_638),
        ("persons with affiliations", metrics["counters"]["persons_with_affiliations"], 17_302),
        ("malformed input lines", metrics["counters"]["malformed"], 0),
        ("peak pipeline memory < 1.9 GiB", metrics["peak_memory_bytes"] < int(1.9 * 2**30), True),
    ]
    failures = 0
    for name, actual, expected in checks:
        ok = actual == expected
        failures += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: got {actual}, expected {expected}")
    print("all checks passed." if not failures else f"{failures} check(s) failed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
