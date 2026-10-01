"""Turn one person record plus its looked-up orgs into the indexed document."""
from __future__ import annotations

from typing import Mapping

import orjson


def referenced_org_ids(person: dict) -> set[int]:
    return {
        role["organization_id"]
        for role in person.get("roles") or ()
        if role.get("organization_id") is not None
    }


def build_document(person: dict, orgs: Mapping[int, bytes]) -> tuple[bytes, int]:
    """Mutates `person` into the document and returns (JSON bytes, unresolved reference count).

    - `organizations` becomes the full org-feed records for the person's roles, deduplicated, in
      role order. Org bytes are spliced in verbatim (orjson.Fragment), never re-parsed.
    - Input `organizations` (LinkedIn affiliations, not employers) moves to `affiliations`.
    - Each role with an organization_id gets `organization_resolved`; unknown ids are also listed
      in `unresolved_organization_ids`. Roles are never dropped.
    """
    joined: list[orjson.Fragment] = []
    seen: set[int] = set()
    unresolved: list[int] = []
    unresolved_refs = 0

    for role in person.get("roles") or ():
        org_id = role.get("organization_id")
        if org_id is None:
            continue
        raw = orgs.get(org_id)
        role["organization_resolved"] = raw is not None
        if raw is None:
            unresolved_refs += 1
            if org_id not in unresolved:
                unresolved.append(org_id)
        elif org_id not in seen:
            seen.add(org_id)
            joined.append(orjson.Fragment(raw))

    affiliations = person.pop("organizations", None)
    if affiliations:
        person["affiliations"] = affiliations
    person["organizations"] = joined
    if unresolved:
        person["unresolved_organization_ids"] = unresolved
    return orjson.dumps(person), unresolved_refs
