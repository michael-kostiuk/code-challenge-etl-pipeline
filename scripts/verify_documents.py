#!/usr/bin/env python3
"""Full-document check of the live index against an independent rebuild from the raw feeds.

Samples persons (uniform + edge-case categories), rebuilds each expected _source from the
specification (not from pipeline/etl), fetches them from Elasticsearch and deep-compares.
Stdlib only.

Run after a full ingest:  python3 scripts/verify_documents.py [--n 2000] [--seed 1]
"""
import argparse
import glob
import gzip
import heapq
import json
import os
import random
import sys
import time
import urllib.request
from multiprocessing import Pool
from pathlib import Path

ES_URL = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
DATA = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
CAP = 200
LARGEST = 5
BATCH = 200
MAX_SHOWN = 20
CATEGORIES = ("affiliations", "unresolved", "dup_org_roles", "null_org_role")


def lines(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        yield from f


def org_ids_of_file(path):
    return {json.loads(line)["serialized_data"]["forager_id"] for line in lines(path)}


def categories(sd, org_ids):
    """Edge-case categories a person belongs to."""
    found = set()
    if isinstance(sd.get("organizations"), list) and sd["organizations"]:
        found.add("affiliations")
    seen, dup = set(), False
    for role in sd.get("roles") or []:
        oid = role.get("organization_id")
        if oid is None:
            found.add("null_org_role")
            continue
        if oid not in org_ids:
            found.add("unresolved")
        if oid in seen:
            dup = True
        seen.add(oid)
    if dup:
        found.add("dup_org_roles")
    return found


def sample_person_file(args):
    """One pass over a person file: n smallest random keys (uniform), per-category caps, largest lines."""
    path, org_ids, n, seed = args
    rng = random.Random(seed)
    uniform, largest = [], []  # heaps; uniform keeps the n smallest keys via negated key
    cats = {c: [] for c in CATEGORIES}
    for i, line in enumerate(lines(path)):
        key = rng.random()
        if len(uniform) < n or -uniform[0][0] > key:
            item = (-key, i, line)
            heapq.heappush(uniform, item) if len(uniform) < n else heapq.heapreplace(uniform, item)
        if len(largest) < LARGEST or largest[0][0] < len(line):
            item = (len(line), i, line)
            heapq.heappush(largest, item) if len(largest) < LARGEST else heapq.heapreplace(largest, item)
        for c in categories(json.loads(line)["serialized_data"], org_ids):
            if len(cats[c]) < CAP:
                cats[c].append(line)
    # keep key with each uniform line so the merge stays exactly uniform
    return ([(-k, line) for k, _, line in uniform], cats, [(size, line) for size, _, line in largest])


def collect_org_records(args):
    path, wanted = args
    out = {}
    for line in lines(path):
        sd = json.loads(line)["serialized_data"]
        if sd["forager_id"] in wanted:
            out[sd["forager_id"]] = sd
    return out


def expected_document(sd, org_ids, org_records):
    doc = {k: v for k, v in sd.items() if k not in ("roles", "organizations")}
    roles, orgs, unresolved = [], [], []
    for role in sd.get("roles") or []:
        role = dict(role)
        oid = role.get("organization_id")
        if oid is not None:
            resolved = oid in org_ids
            role["organization_resolved"] = resolved
            if resolved and org_records[oid] not in orgs:
                orgs.append(org_records[oid])
            if not resolved and oid not in unresolved:
                unresolved.append(oid)
        roles.append(role)
    if "roles" in sd:
        doc["roles"] = roles
    doc["organizations"] = orgs
    if isinstance(sd.get("organizations"), list) and sd["organizations"]:
        doc["affiliations"] = sd["organizations"]
    if unresolved:
        doc["unresolved_organization_ids"] = unresolved
    return doc


def diff(a, b, path="$"):
    """Yield JSON paths where expected (a) and actual (b) differ."""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                yield f"{path}.{k}: unexpected in index"
            elif k not in b:
                yield f"{path}.{k}: missing from index"
            else:
                yield from diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            yield f"{path}: length expected {len(a)}, got {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            yield from diff(x, y, f"{path}[{i}]")
    elif a != b or type(a) is not type(b):
        yield f"{path}: expected {json.dumps(a)[:80]}, got {json.dumps(b)[:80]}"


def es_fetch(ids):
    body = {"query": {"terms": {"forager_id": ids}}, "size": len(ids) + 50}
    req = urllib.request.Request(
        f"{ES_URL}/persons/_search", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return [h["_source"] for h in json.loads(resp.read())["hits"]["hits"]]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--n", type=int, default=2000, help="uniform sample size")
    ap.add_argument("--seed", type=int, default=1)
    opts = ap.parse_args()
    t0 = time.time()
    person_files = sorted(glob.glob(str(DATA / "person" / "*.json.gz")))
    org_files = sorted(glob.glob(str(DATA / "organization" / "*.json.gz")))

    with Pool(len(org_files)) as pool:  # pass A
        org_ids = set().union(*pool.map(org_ids_of_file, org_files))
    print(f"pass A: {len(org_ids)} org ids ({time.time() - t0:.0f}s)", flush=True)

    jobs = [(p, org_ids, opts.n, opts.seed * 1000 + i) for i, p in enumerate(person_files)]
    with Pool(len(person_files)) as pool:  # pass B
        results = pool.map(sample_person_file, jobs)
    uniform = heapq.nsmallest(opts.n, (x for r in results for x in r[0]), key=lambda x: x[0])
    sampled = {}  # forager_id -> (sd, set of category labels)
    def add(line, label):
        sd = json.loads(line)["serialized_data"]
        sampled.setdefault(sd["forager_id"], (sd, set()))[1].add(label)
    for _, line in uniform:
        add(line, "uniform")
    for c in CATEGORIES:
        for line in [l for r in results for l in r[1][c]][:CAP]:
            add(line, c)
    for _, line in heapq.nlargest(LARGEST, (x for r in results for x in r[2]), key=lambda x: x[0]):
        add(line, "largest")
    print(f"pass B: {len(sampled)} distinct sampled persons ({time.time() - t0:.0f}s)", flush=True)

    wanted = {role["organization_id"] for sd, _ in sampled.values() for role in sd.get("roles") or []
              if role.get("organization_id") in org_ids}
    with Pool(len(org_files)) as pool:  # pass C
        org_records = {}
        for part in pool.map(collect_org_records, [(p, wanted) for p in org_files]):
            org_records.update(part)
    print(f"pass C: {len(org_records)}/{len(wanted)} org records ({time.time() - t0:.0f}s)", flush=True)

    ids = sorted(sampled)
    compared = matched = missing = duplicated = 0
    shown = 0
    mismatches = []
    for start in range(0, len(ids), BATCH):
        batch = ids[start:start + BATCH]
        hits = {}
        for src in es_fetch(batch):
            hits.setdefault(src.get("forager_id"), []).append(src)
        for fid in batch:
            found = hits.get(fid, [])
            if not found:
                missing += 1
                print(f"MISSING forager_id={fid}")
                continue
            if len(found) > 1:
                duplicated += 1
                print(f"DUPLICATED forager_id={fid} ({len(found)} hits)")
                continue
            compared += 1
            expected = expected_document(sampled[fid][0], org_ids, org_records)
            if expected == found[0] and json.dumps(expected, sort_keys=True) == json.dumps(found[0], sort_keys=True):
                matched += 1
            else:
                mismatches.append(fid)
                if shown < MAX_SHOWN:
                    shown += 1
                    paths = list(diff(expected, found[0]))[:5]
                    print(f"MISMATCH forager_id={fid}: " + "; ".join(paths))

    print("\nsampled per category (a person may be in several):")
    for label in ("uniform", *CATEGORIES, "largest"):
        print(f"  {label}: {sum(label in cs for _, cs in sampled.values())}")
    print(f"distinct sampled: {len(sampled)}  compared: {compared}  matched: {matched}  "
          f"mismatched: {len(mismatches)}  missing: {missing}  duplicated: {duplicated}")
    print(f"runtime: {time.time() - t0:.0f}s")
    ok = matched == len(sampled) and not (mismatches or missing or duplicated)
    print("ALL MATCH" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
