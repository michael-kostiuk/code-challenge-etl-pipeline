"""Turn one person record plus its looked-up orgs into the indexed document."""
from __future__ import annotations

from typing import Iterator, Mapping, NamedTuple

import orjson

from etl.reader import MalformedRecord


class Document(NamedTuple):
    body: bytes            # the JSON document
    unresolved_refs: int   # roles whose organization_id is not in the org feed
    has_affiliations: bool


def check_person(person: dict) -> None:
    """Raises MalformedRecord unless `roles` is absent or a list of objects whose `organization_id`
    is an integer or null: the shape the join below relies on."""
    roles = person.get("roles")
    if roles is None:
        return
    if not isinstance(roles, list):
        raise MalformedRecord("roles is not a list")
    for i, role in enumerate(roles):
        if not isinstance(role, dict):
            raise MalformedRecord(f"roles[{i}] is not an object")
        org_id = role.get("organization_id")
        if org_id is not None and (not isinstance(org_id, int) or isinstance(org_id, bool)):
            raise MalformedRecord(f"roles[{i}].organization_id is not an integer")


def _org_refs(person: dict) -> Iterator[tuple[dict, int]]:
    """(role, organization_id) for every role that references an organization; `person` has passed
    `check_person`."""
    for role in person.get("roles") or ():
        if (org_id := role.get("organization_id")) is not None:
            yield role, org_id


def referenced_org_ids(person: dict) -> set[int]:
    return {org_id for _, org_id in _org_refs(person)}


def build_document(person: dict, orgs: Mapping[int, bytes]) -> Document:
    """Builds the indexed document from `person` (which it mutates) and the orgs looked up for it.

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

    for role, org_id in _org_refs(person):
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
    return Document(orjson.dumps(person), unresolved_refs, "affiliations" in person)
