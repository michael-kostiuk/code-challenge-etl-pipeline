#!/usr/bin/env python3
"""Profile the person/organization NDJSON feeds to ground pipeline design in real numbers.

Usage: python3 scripts/profile_data.py [--data DIR] [--out FILE] [--workers N]

Streams every .json.gz file line by line (never holds a whole file), in two
parallel phases:
  1. organizations: one process per file -> per-org compact size + name
  2. persons: one process per file, with the org size/name maps inherited via
     fork, so the merged-document size is computed exactly while streaming.
Standard library only; uses orjson for parsing if installed.

Byte sizes are UTF-8 bytes. "Compact" means serialized_data re-serialized with
no whitespace and non-ASCII kept as UTF-8 (what orjson / ensure_ascii=False emit).
"""
import argparse
import gzip
import json
import multiprocessing as mp
import os
import sys
import time
from array import array
from collections import Counter
from pathlib import Path

try:
    import orjson

    loads = orjson.loads

    def compact_size(obj):
        return len(orjson.dumps(obj))

    PARSER = "orjson"
except ImportError:
    loads = json.loads
    _enc = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False)

    def compact_size(obj):
        return len(_enc.encode(obj).encode("utf-8"))

    PARSER = "json"

KEYWORD_IGNORE_ABOVE = 256
TARGET_ROLE_TITLE = "Project Manager"
TARGET_ORG_NAME = "Dell Technologies"
MAX_BAD_SAMPLES = 5

# Org lookup maps for the person phase; populated in the parent before the
# person pool forks so workers inherit them copy-on-write.
ORG_SIZE = {}
ORG_NAME = {}


# ---------------------------------------------------------------- helpers

def iter_lines(path):
    """Yield (line_no, raw_bytes) for each non-empty line; counts blanks via 0-len yield."""
    with gzip.open(path, "rb") as fh:
        for n, line in enumerate(fh, 1):
            yield n, line.rstrip(b"\r\n")


def new_field_stats():
    return {"present": 0, "types": Counter(), "list_nonempty": 0, "list_len_sum": 0,
            "list_len_max": 0, "str_empty": 0}


def update_field_stats(stats, sd):
    for key, val in sd.items():
        st = stats.get(key)
        if st is None:
            st = stats[key] = new_field_stats()
        st["present"] += 1
        st["types"][type(val).__name__] += 1
        if isinstance(val, list):
            n = len(val)
            if n:
                st["list_nonempty"] += 1
                st["list_len_sum"] += n
                if n > st["list_len_max"]:
                    st["list_len_max"] = n
        elif isinstance(val, str) and not val.strip():
            st["str_empty"] += 1


def merge_field_stats(dst, src):
    for key, s in src.items():
        d = dst.setdefault(key, new_field_stats())
        d["present"] += s["present"]
        d["types"].update(s["types"])
        d["list_nonempty"] += s["list_nonempty"]
        d["list_len_sum"] += s["list_len_sum"]
        d["list_len_max"] = max(d["list_len_max"], s["list_len_max"])
        d["str_empty"] += s["str_empty"]


def finalize_field_stats(stats, total):
    out = {}
    for key, s in sorted(stats.items(), key=lambda kv: (-kv[1]["present"], kv[0])):
        row = {"present": s["present"], "present_pct": pct(s["present"], total),
               "types": dict(s["types"])}
        if s["types"].get("list"):
            nl = s["types"]["list"]
            row["nonempty"] = s["list_nonempty"]
            row["nonempty_pct"] = pct(s["list_nonempty"], total)
            row["avg_len_all"] = round(s["list_len_sum"] / nl, 3) if nl else 0
            row["avg_len_nonempty"] = (round(s["list_len_sum"] / s["list_nonempty"], 3)
                                       if s["list_nonempty"] else 0)
            row["max_len"] = s["list_len_max"]
        if s["types"].get("str"):
            row["blank_str"] = s["str_empty"]
        out[key] = row
    return out


def pct(a, b):
    return round(100.0 * a / b, 4) if b else 0.0


def dist(arr):
    """Exact summary of an iterable of ints (sorted copy)."""
    vals = sorted(arr)
    n = len(vals)
    if not n:
        return {"n": 0}
    total = sum(vals)

    def q(p):
        return vals[min(n - 1, int(p * n))]

    return {"n": n, "total": total, "avg": round(total / n, 1), "min": vals[0],
            "p50": q(0.50), "p95": q(0.95), "p99": q(0.99), "max": vals[-1]}


def hist_dist(counter):
    """Summary of a Counter {value: frequency}."""
    n = sum(counter.values())
    if not n:
        return {"n": 0}
    total = sum(v * c for v, c in counter.items())
    keys = sorted(counter)

    def q(p):
        target = int(p * n)
        acc = 0
        for k in keys:
            acc += counter[k]
            if acc > target:
                return k
        return keys[-1]

    return {"n": n, "avg": round(total / n, 3), "min": keys[0], "p50": q(0.5),
            "p95": q(0.95), "p99": q(0.99), "max": keys[-1]}


# ---------------------------------------------------------------- org phase

def profile_org_file(path):
    r = {"file": os.path.basename(path), "lines": 0, "blank": 0, "records": 0,
         "bad": 0, "bad_samples": [], "id_mismatch": 0, "id_mismatch_samples": [],
         "fields": {}, "name_missing": 0, "name_blank": 0, "name_max_chars": 0,
         "name_over_256": 0, "name_over_256_samples": []}
    line_sizes = array("I")
    sd_sizes = array("I")
    # id -> (date_updated, compact_size, name); every occurrence kept for dup analysis
    seen = []
    for n, raw in iter_lines(path):
        r["lines"] += 1
        if not raw.strip():
            r["blank"] += 1
            continue
        try:
            env = loads(raw)
            sd = env["serialized_data"]
            oid = env["id"]
        except Exception as e:  # malformed JSON or missing envelope keys
            r["bad"] += 1
            if len(r["bad_samples"]) < MAX_BAD_SAMPLES:
                r["bad_samples"].append({"line": n, "error": repr(e)[:200],
                                         "head": raw[:200].decode("utf-8", "replace")})
            continue
        r["records"] += 1
        line_sizes.append(len(raw))
        size = compact_size(sd)
        sd_sizes.append(size)
        if sd.get("forager_id") != oid:
            r["id_mismatch"] += 1
            if len(r["id_mismatch_samples"]) < MAX_BAD_SAMPLES:
                r["id_mismatch_samples"].append([oid, sd.get("forager_id")])
        update_field_stats(r["fields"], sd)
        name = sd.get("name")
        if name is None:
            r["name_missing"] += 1
        elif not isinstance(name, str) or not name.strip():
            r["name_blank"] += 1
        if isinstance(name, str):
            ln = len(name)
            if ln > r["name_max_chars"]:
                r["name_max_chars"] = ln
            if ln > KEYWORD_IGNORE_ABOVE:
                r["name_over_256"] += 1
                if len(r["name_over_256_samples"]) < MAX_BAD_SAMPLES:
                    r["name_over_256_samples"].append(name[:300])
        seen.append((oid, env.get("date_updated"), size, name if isinstance(name, str) else None))
    r["line_sizes"] = line_sizes
    r["sd_sizes"] = sd_sizes
    r["seen"] = seen
    return r


# ---------------------------------------------------------------- person phase

def profile_person_file(path):
    org_size, org_name = ORG_SIZE, ORG_NAME
    r = {"file": os.path.basename(path), "lines": 0, "blank": 0, "records": 0,
         "bad": 0, "bad_samples": [], "id_mismatch": 0, "id_mismatch_samples": [],
         "fields": {}, "organizations_nonempty": 0,
         "roles_per_person": Counter(), "distinct_orgs_per_person": Counter(),
         "roles_total": 0, "roles_no_org_id": 0, "org_id_types": Counter(),
         "refs_total": 0, "refs_resolved": 0, "refs_unresolved": 0,
         "persons_with_refs": 0, "persons_with_unresolved": 0,
         "persons_all_unresolved": 0,
         "name_cmp_roles": 0, "name_mismatch_roles": 0, "name_mismatch_samples": [],
         "name_mismatch_case_only": 0, "name_mismatch_role_name_null": 0,
         "existing_orgs_persons": 0, "existing_orgs_items": 0, "existing_orgs_bytes": 0,
         "existing_orgs_in_feed": 0, "existing_orgs_overlap_roles": 0,
         "existing_orgs_keys": Counter(),
         "role_title_max_chars": 0, "role_title_over_256": 0, "role_title_types": Counter(),
         "pm_ids": set(), "dell_feed_ids": set(), "dell_role_ids": set(),
         "dell_feed_not_role": 0, "dell_role_not_feed": 0}
    line_sizes = array("I")
    sd_sizes = array("I")
    merged_sizes = array("Q")
    ids = []  # (id, date_updated)
    ref_ids = set()
    unresolved_ids = set()
    for n, raw in iter_lines(path):
        r["lines"] += 1
        if not raw.strip():
            r["blank"] += 1
            continue
        try:
            env = loads(raw)
            sd = env["serialized_data"]
            pid = env["id"]
        except Exception as e:
            r["bad"] += 1
            if len(r["bad_samples"]) < MAX_BAD_SAMPLES:
                r["bad_samples"].append({"line": n, "error": repr(e)[:200],
                                         "head": raw[:200].decode("utf-8", "replace")})
            continue
        r["records"] += 1
        ids.append((pid, env.get("date_updated")))
        line_sizes.append(len(raw))
        size = compact_size(sd)
        sd_sizes.append(size)
        if sd.get("forager_id") != pid:
            r["id_mismatch"] += 1
            if len(r["id_mismatch_samples"]) < MAX_BAD_SAMPLES:
                r["id_mismatch_samples"].append([pid, sd.get("forager_id")])
        update_field_stats(r["fields"], sd)
        existing = sd.get("organizations") or []
        if existing:
            r["organizations_nonempty"] += 1

        roles = sd.get("roles") or []
        r["roles_per_person"][len(roles)] += 1
        person_orgs = []  # distinct org ids in role order
        person_org_set = set()
        has_unresolved = False
        has_pm = has_dell_feed = has_dell_role = False
        for role in roles:
            r["roles_total"] += 1
            title = role.get("role_title")
            r["role_title_types"][type(title).__name__] += 1
            if isinstance(title, str):
                if len(title) > r["role_title_max_chars"]:
                    r["role_title_max_chars"] = len(title)
                if len(title) > KEYWORD_IGNORE_ABOVE:
                    r["role_title_over_256"] += 1
                if title == TARGET_ROLE_TITLE:
                    has_pm = True
            role_org_name = role.get("organization_name")
            if role_org_name == TARGET_ORG_NAME:
                has_dell_role = True
            oid = role.get("organization_id")
            r["org_id_types"][type(oid).__name__] += 1
            if oid is None:
                r["roles_no_org_id"] += 1
                continue
            r["refs_total"] += 1
            ref_ids.add(oid)
            if oid not in person_org_set:
                person_org_set.add(oid)
                person_orgs.append(oid)
            feed_name = org_name.get(oid, _MISSING)
            if oid not in org_size:
                r["refs_unresolved"] += 1
                unresolved_ids.add(oid)
                has_unresolved = True
                continue
            r["refs_resolved"] += 1
            if feed_name == TARGET_ORG_NAME:
                has_dell_feed = True
            r["name_cmp_roles"] += 1
            if role_org_name != feed_name:
                r["name_mismatch_roles"] += 1
                if role_org_name is None:
                    r["name_mismatch_role_name_null"] += 1
                if (isinstance(role_org_name, str) and isinstance(feed_name, str)
                        and role_org_name.strip().casefold() == feed_name.strip().casefold()):
                    r["name_mismatch_case_only"] += 1
                if len(r["name_mismatch_samples"]) < 10:
                    r["name_mismatch_samples"].append(
                        {"org_id": oid, "role_name": role_org_name, "feed_name": feed_name})

        r["distinct_orgs_per_person"][len(person_orgs)] += 1
        if person_orgs:
            r["persons_with_refs"] += 1
        if has_unresolved:
            r["persons_with_unresolved"] += 1
            if not any(o in org_size for o in person_orgs):
                r["persons_all_unresolved"] += 1
        if has_pm:
            r["pm_ids"].add(pid)
        if has_dell_feed:
            r["dell_feed_ids"].add(pid)
        if has_dell_role:
            r["dell_role_ids"].add(pid)
        if has_dell_feed and not has_dell_role:
            r["dell_feed_not_role"] += 1
        if has_dell_role and not has_dell_feed:
            r["dell_role_not_feed"] += 1

        # Pre-existing organizations[] entries (LinkedIn "Organizations" section).
        for item in existing:
            r["existing_orgs_items"] += 1
            r["existing_orgs_bytes"] += compact_size(item)
            if isinstance(item, dict):
                r["existing_orgs_keys"].update(item.keys())
                fid = item.get("forager_id")
                if fid in org_size:
                    r["existing_orgs_in_feed"] += 1
                if fid in person_org_set:
                    r["existing_orgs_overlap_roles"] += 1

        # Exact merged size, keeping any pre-existing entries and appending the
        # resolved feed orgs: [e1,...,o1,o2,...]
        resolved_sizes = [org_size[o] for o in person_orgs if o in org_size]
        extra = sum(resolved_sizes) + len(resolved_sizes) - (0 if existing else min(1, len(resolved_sizes)))
        merged_sizes.append(size + extra)

    r["line_sizes"] = line_sizes
    r["sd_sizes"] = sd_sizes
    r["merged_sizes"] = merged_sizes
    r["ids"] = ids
    r["ref_ids"] = ref_ids
    r["unresolved_ids"] = unresolved_ids
    return r


_MISSING = object()


# ---------------------------------------------------------------- driver

def run_orgs(files, workers):
    with mp.get_context("fork").Pool(min(workers, len(files))) as pool:
        results = pool.map(profile_org_file, files)
    fields = {}
    line_sizes, sd_sizes = array("I"), array("I")
    agg = Counter()
    bad_samples, mism_samples, long_names = [], [], []
    name_max = 0
    occurrences = {}  # id -> list of (date_updated, size)
    for r in results:
        for k in ("lines", "blank", "records", "bad", "id_mismatch", "name_missing",
                  "name_blank", "name_over_256"):
            agg[k] += r[k]
        name_max = max(name_max, r["name_max_chars"])
        bad_samples += [dict(s, file=r["file"]) for s in r["bad_samples"]]
        mism_samples += r["id_mismatch_samples"]
        long_names += r["name_over_256_samples"]
        merge_field_stats(fields, r["fields"])
        line_sizes.extend(r["line_sizes"])
        sd_sizes.extend(r["sd_sizes"])
        for oid, du, size, name in r["seen"]:
            prev = ORG_SIZE.get(oid)
            if prev is not None:
                occurrences.setdefault(oid, [(ORG_DATE[oid], prev)]).append((du, size))
            # On duplicate ids keep the most recently updated record.
            if prev is None or (du or "") >= (ORG_DATE.get(oid) or ""):
                ORG_SIZE[oid] = size
                ORG_NAME[oid] = name
                ORG_DATE[oid] = du
        r["seen"] = None
    dup_ids = len(occurrences)
    dup_extra = sum(len(v) - 1 for v in occurrences.values())
    dup_date_diff = sum(1 for v in occurrences.values() if len({d for d, _ in v}) > 1)
    dup_size_diff = sum(1 for v in occurrences.values() if len({s for _, s in v}) > 1)
    total = agg["records"]
    report = {
        "files": len(files),
        "lines": agg["lines"], "blank_lines": agg["blank"], "records": total,
        "malformed_lines": agg["bad"], "malformed_samples": bad_samples,
        "distinct_ids": len(ORG_SIZE),
        "duplicate_ids": dup_ids, "duplicate_extra_records": dup_extra,
        "duplicate_ids_with_differing_date_updated": dup_date_diff,
        "duplicate_ids_with_differing_content_size": dup_size_diff,
        "duplicate_id_samples": [[k, v] for k, v in list(occurrences.items())[:5]],
        "envelope_id_ne_forager_id": agg["id_mismatch"], "id_mismatch_samples": mism_samples,
        "line_bytes": dist(line_sizes),
        "serialized_data_compact_bytes": dist(sd_sizes),
        "name": {"missing": agg["name_missing"], "missing_pct": pct(agg["name_missing"], total),
                 "blank_or_nonstring": agg["name_blank"],
                 "blank_or_nonstring_pct": pct(agg["name_blank"], total),
                 "max_chars": name_max, "over_256_chars": agg["name_over_256"],
                 "over_256_samples": long_names},
        "fields": finalize_field_stats(fields, total),
    }
    return report


ORG_DATE = {}


def run_persons(files, workers):
    with mp.get_context("fork").Pool(min(workers, len(files))) as pool:
        results = pool.map(profile_person_file, files)
    fields = {}
    line_sizes, sd_sizes, merged_sizes = array("I"), array("I"), array("Q")
    agg = Counter()
    roles_pp, orgs_pp, oid_types, title_types = Counter(), Counter(), Counter(), Counter()
    bad_samples, mism_samples, name_samples = [], [], []
    title_max = 0
    existing_keys = Counter()
    ref_ids, unresolved_ids = set(), set()
    pm_ids, dell_feed_ids, dell_role_ids = set(), set(), set()
    id_dates = {}
    dup = {}
    sum_keys = ("lines", "blank", "records", "bad", "id_mismatch", "organizations_nonempty",
                "roles_total", "roles_no_org_id", "refs_total", "refs_resolved",
                "refs_unresolved", "persons_with_refs", "persons_with_unresolved",
                "persons_all_unresolved", "name_cmp_roles", "name_mismatch_roles",
                "name_mismatch_case_only", "name_mismatch_role_name_null",
                "existing_orgs_items", "existing_orgs_bytes", "existing_orgs_in_feed",
                "existing_orgs_overlap_roles", "role_title_over_256", "dell_feed_not_role",
                "dell_role_not_feed")
    for r in results:
        for k in sum_keys:
            agg[k] += r[k]
        title_max = max(title_max, r["role_title_max_chars"])
        bad_samples += [dict(s, file=r["file"]) for s in r["bad_samples"]]
        mism_samples += r["id_mismatch_samples"]
        name_samples += r["name_mismatch_samples"]
        merge_field_stats(fields, r["fields"])
        existing_keys.update(r["existing_orgs_keys"])
        roles_pp.update(r["roles_per_person"])
        orgs_pp.update(r["distinct_orgs_per_person"])
        oid_types.update(r["org_id_types"])
        title_types.update(r["role_title_types"])
        line_sizes.extend(r["line_sizes"])
        sd_sizes.extend(r["sd_sizes"])
        merged_sizes.extend(r["merged_sizes"])
        ref_ids |= r["ref_ids"]
        unresolved_ids |= r["unresolved_ids"]
        pm_ids |= r["pm_ids"]
        dell_feed_ids |= r["dell_feed_ids"]
        dell_role_ids |= r["dell_role_ids"]
        for pid, du in r["ids"]:
            if pid in id_dates:
                dup.setdefault(pid, [id_dates[pid]]).append(du)
            else:
                id_dates[pid] = du
        r["ids"] = None
    total = agg["records"]
    resolved_ids = ref_ids - unresolved_ids
    referenced_org_bytes = sum(ORG_SIZE[o] for o in resolved_ids)
    all_org_bytes = sum(ORG_SIZE.values())
    md = dist(merged_sizes)
    report = {
        "files": len(files),
        "lines": agg["lines"], "blank_lines": agg["blank"], "records": total,
        "malformed_lines": agg["bad"], "malformed_samples": bad_samples,
        "distinct_ids": len(id_dates),
        "duplicate_ids": len(dup), "duplicate_extra_records": sum(len(v) - 1 for v in dup.values()),
        "duplicate_ids_with_differing_date_updated": sum(1 for v in dup.values() if len(set(v)) > 1),
        "duplicate_id_samples": [[k, v] for k, v in list(dup.items())[:5]],
        "envelope_id_ne_forager_id": agg["id_mismatch"], "id_mismatch_samples": mism_samples,
        "line_bytes": dist(line_sizes),
        "serialized_data_compact_bytes": dist(sd_sizes),
        "organizations_nonempty_in_input": agg["organizations_nonempty"],
        "existing_organizations": {
            "items": agg["existing_orgs_items"],
            "compact_bytes": agg["existing_orgs_bytes"],
            "item_keys": dict(existing_keys),
            "forager_id_in_org_feed": agg["existing_orgs_in_feed"],
            "forager_id_also_in_persons_roles": agg["existing_orgs_overlap_roles"],
        },
        "fields": finalize_field_stats(fields, total),
        "roles": {
            "total": agg["roles_total"],
            "per_person": hist_dist(roles_pp),
            "persons_with_zero_roles": roles_pp.get(0, 0),
            "without_organization_id": agg["roles_no_org_id"],
            "without_organization_id_pct": pct(agg["roles_no_org_id"], agg["roles_total"]),
            "organization_id_types": dict(oid_types),
            "role_title_types": dict(title_types),
            "role_title_max_chars": title_max,
            "role_title_over_256_chars": agg["role_title_over_256"],
        },
        "distinct_orgs_per_person": hist_dist(orgs_pp),
        "join": {
            "references_total": agg["refs_total"],
            "references_resolved": agg["refs_resolved"],
            "references_resolved_pct": pct(agg["refs_resolved"], agg["refs_total"]),
            "references_unresolved": agg["refs_unresolved"],
            "references_unresolved_pct": pct(agg["refs_unresolved"], agg["refs_total"]),
            "distinct_org_ids_referenced": len(ref_ids),
            "distinct_org_ids_resolved": len(resolved_ids),
            "distinct_org_ids_unresolved": len(unresolved_ids),
            "distinct_org_ids_unresolved_pct": pct(len(unresolved_ids), len(ref_ids)),
            "persons_with_any_reference": agg["persons_with_refs"],
            "persons_with_unresolved_reference": agg["persons_with_unresolved"],
            "persons_with_unresolved_reference_pct": pct(agg["persons_with_unresolved"], total),
            "persons_with_only_unresolved_references": agg["persons_all_unresolved"],
            "org_feed_ids_never_referenced": len(ORG_SIZE) - len(resolved_ids),
            "referenced_org_compact_bytes": referenced_org_bytes,
            "all_org_compact_bytes": all_org_bytes,
            "referenced_org_bytes_pct": pct(referenced_org_bytes, all_org_bytes),
            "referenced_org_count_pct": pct(len(resolved_ids), len(ORG_SIZE)),
        },
        "org_name_vs_role_name": {
            "roles_compared": agg["name_cmp_roles"],
            "mismatched": agg["name_mismatch_roles"],
            "mismatched_pct": pct(agg["name_mismatch_roles"], agg["name_cmp_roles"]),
            "mismatched_case_or_whitespace_only": agg["name_mismatch_case_only"],
            "mismatched_because_role_name_null": agg["name_mismatch_role_name_null"],
            "mismatched_different_string": agg["name_mismatch_roles"] - agg["name_mismatch_role_name_null"],
            "samples": name_samples[:10],
        },
        "merged_document_bytes": md,
        "merged_total_vs_input_sd_total_ratio": round(md["total"] / sd_sizes_total(sd_sizes), 3),
        "candidate_expected": {
            "role_title_exact": TARGET_ROLE_TITLE,
            "persons_with_role_title": len(pm_ids),
            "org_name_exact": TARGET_ORG_NAME,
            "persons_at_org_by_feed_name": len(dell_feed_ids),
            "persons_at_org_by_role_organization_name": len(dell_role_ids),
            "persons_feed_match_but_not_role_name": agg["dell_feed_not_role"],
            "persons_role_name_match_but_not_feed": agg["dell_role_not_feed"],
            "note": "Counts are distinct person ids (matches ES doc count when _id = person id).",
        },
    }
    return report


def sd_sizes_total(arr):
    return sum(arr) or 1


# ---------------------------------------------------------------- output

def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,} B"
        n /= 1024


def print_summary(rep):
    o, p = rep["organizations"], rep["persons"]
    j = p["join"]

    def dline(label, d):
        print(f"  {label:<34} n={d['n']:>9,}  avg={d['avg']:>9,}  p50={d['p50']:>8,}  "
              f"p95={d['p95']:>8,}  p99={d['p99']:>8,}  max={d['max']:>10,}"
              + (f"  total={fmt_bytes(d['total'])}" if "total" in d else ""))

    def fields(fs):
        for k, v in fs.items():
            extra = ""
            if "nonempty" in v:
                extra = (f"  nonempty={v['nonempty_pct']:6.2f}%  avg_len(nonempty)="
                         f"{v['avg_len_nonempty']:.2f}  max_len={v['max_len']}")
            types = ",".join(f"{t}:{c}" for t, c in v["types"].items())
            print(f"    {k:<26} present={v['present_pct']:6.2f}%  [{types}]{extra}")

    print(f"\n=== ORGANIZATIONS ({o['files']} files) ===")
    print(f"  records={o['records']:,}  distinct ids={o['distinct_ids']:,}  "
          f"duplicate ids={o['duplicate_ids']:,} (extra records {o['duplicate_extra_records']:,}, "
          f"date differs {o['duplicate_ids_with_differing_date_updated']:,})")
    print(f"  malformed lines={o['malformed_lines']}  blank lines={o['blank_lines']}  "
          f"envelope id != forager_id: {o['envelope_id_ne_forager_id']}")
    dline("line bytes", o["line_bytes"])
    dline("serialized_data compact bytes", o["serialized_data_compact_bytes"])
    n = o["name"]
    print(f"  name: missing={n['missing']} ({n['missing_pct']}%)  blank={n['blank_or_nonstring']} "
          f"({n['blank_or_nonstring_pct']}%)  max_chars={n['max_chars']}  >256 chars={n['over_256_chars']}")
    print("  fields:")
    fields(o["fields"])

    print(f"\n=== PERSONS ({p['files']} files) ===")
    print(f"  records={p['records']:,}  distinct ids={p['distinct_ids']:,}  "
          f"duplicate ids={p['duplicate_ids']:,} (extra {p['duplicate_extra_records']:,}, "
          f"date differs {p['duplicate_ids_with_differing_date_updated']:,})")
    print(f"  malformed lines={p['malformed_lines']}  blank lines={p['blank_lines']}  "
          f"envelope id != forager_id: {p['envelope_id_ne_forager_id']}  "
          f"organizations[] non-empty in input: {p['organizations_nonempty_in_input']:,}")
    eo = p["existing_organizations"]
    print(f"  pre-existing organizations[] items={eo['items']:,} ({fmt_bytes(eo['compact_bytes'])}), "
          f"forager_id in org feed={eo['forager_id_in_org_feed']:,}, also a role org of same person="
          f"{eo['forager_id_also_in_persons_roles']:,}, keys={eo['item_keys']}")
    dline("line bytes", p["line_bytes"])
    dline("serialized_data compact bytes", p["serialized_data_compact_bytes"])
    print("  fields:")
    fields(p["fields"])
    ro = p["roles"]
    rp = ro["per_person"]
    print(f"  roles: total={ro['total']:,}  per person avg={rp['avg']} p50={rp['p50']} "
          f"p99={rp['p99']} max={rp['max']}  zero-role persons={ro['persons_with_zero_roles']:,}")
    print(f"         without organization_id={ro['without_organization_id']:,} "
          f"({ro['without_organization_id_pct']}%)  org_id types={ro['organization_id_types']}")
    print(f"         role_title max chars={ro['role_title_max_chars']}  >256={ro['role_title_over_256_chars']}")
    dp = p["distinct_orgs_per_person"]
    print(f"  distinct orgs/person: avg={dp['avg']} p50={dp['p50']} p99={dp['p99']} max={dp['max']}")

    print("\n=== JOIN ===")
    print(f"  references: total={j['references_total']:,}  resolved={j['references_resolved']:,} "
          f"({j['references_resolved_pct']}%)  unresolved={j['references_unresolved']:,} "
          f"({j['references_unresolved_pct']}%)")
    print(f"  distinct org ids referenced={j['distinct_org_ids_referenced']:,}  "
          f"resolved={j['distinct_org_ids_resolved']:,}  unresolved={j['distinct_org_ids_unresolved']:,} "
          f"({j['distinct_org_ids_unresolved_pct']}%)")
    print(f"  persons with >=1 unresolved ref={j['persons_with_unresolved_reference']:,} "
          f"({j['persons_with_unresolved_reference_pct']}%)  only-unresolved="
          f"{j['persons_with_only_unresolved_references']:,}")
    print(f"  org feed ids never referenced={j['org_feed_ids_never_referenced']:,}")
    print(f"  referenced org bytes={fmt_bytes(j['referenced_org_compact_bytes'])} of "
          f"{fmt_bytes(j['all_org_compact_bytes'])} ({j['referenced_org_bytes_pct']}% bytes, "
          f"{j['referenced_org_count_pct']}% of orgs)")
    nm = p["org_name_vs_role_name"]
    print(f"  role.organization_name != feed name: {nm['mismatched']:,} / {nm['roles_compared']:,} "
          f"({nm['mismatched_pct']}%): role name null {nm['mismatched_because_role_name_null']:,}, "
          f"different string {nm['mismatched_different_string']:,} "
          f"(case/whitespace-only {nm['mismatched_case_or_whitespace_only']:,})")
    dline("merged document bytes", p["merged_document_bytes"])
    print(f"  merged/input serialized_data size ratio = {p['merged_total_vs_input_sd_total_ratio']}")

    c = p["candidate_expected"]
    print("\n=== CANDIDATE EXPECTED COUNTS ===")
    print(f"  role_title == {c['role_title_exact']!r}: {c['persons_with_role_title']:,}")
    print(f"  org feed name == {c['org_name_exact']!r}: {c['persons_at_org_by_feed_name']:,}")
    print(f"  roles[].organization_name == {c['org_name_exact']!r}: "
          f"{c['persons_at_org_by_role_organization_name']:,}")
    print(f"\n  parser={rep['parser']}  wall time={rep['wall_seconds']}s "
          f"(orgs {rep['org_phase_seconds']}s, persons {rep['person_phase_seconds']}s)")


def main():
    root = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", default=str(root / "data"))
    ap.add_argument("--out", default=str(root / "scripts" / "profile_report.json"))
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 4)
    args = ap.parse_args()

    org_files = sorted(str(p) for p in Path(args.data, "organization").glob("*.json.gz"))
    person_files = sorted(str(p) for p in Path(args.data, "person").glob("*.json.gz"))
    if not org_files or not person_files:
        sys.exit(f"no input files under {args.data}")

    t0 = time.perf_counter()
    orgs = run_orgs(org_files, args.workers)
    t1 = time.perf_counter()
    persons = run_persons(person_files, args.workers)
    t2 = time.perf_counter()

    rep = {"parser": PARSER, "wall_seconds": round(t2 - t0, 1),
           "org_phase_seconds": round(t1 - t0, 1), "person_phase_seconds": round(t2 - t1, 1),
           "organizations": orgs, "persons": persons}
    Path(args.out).write_text(json.dumps(rep, indent=2, ensure_ascii=False, default=str))
    print_summary(rep)
    print(f"  report written to {args.out}")


if __name__ == "__main__":
    main()
