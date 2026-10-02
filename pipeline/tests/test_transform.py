import orjson
import pytest

from etl.reader import MalformedRecord
from etl.transform import build_document, check_person

DELL = b'{"forager_id":140717,"name":"Dell Technologies","technologies":["ASP.NET"]}'


def test_joins_full_org_record_once_and_moves_input_organizations_to_affiliations():
    person = {
        "forager_id": 1,
        "first_name": "Ann",
        "roles": [
            {"role_title": "Engineer", "organization_id": 140717},
            {"role_title": "Manager", "organization_id": 140717},
        ],
        "organizations": [{"name": "KGI Club"}],
    }

    doc = build_document(person, {140717: DELL})

    assert orjson.loads(doc.body) == {
        "forager_id": 1,
        "first_name": "Ann",
        "roles": [
            {"role_title": "Engineer", "organization_id": 140717, "organization_resolved": True},
            {"role_title": "Manager", "organization_id": 140717, "organization_resolved": True},
        ],
        "organizations": [
            {"forager_id": 140717, "name": "Dell Technologies", "technologies": ["ASP.NET"]}
        ],
        "affiliations": [{"name": "KGI Club"}],
    }
    assert (doc.unresolved_refs, doc.has_affiliations) == (0, True)


def test_keeps_and_flags_roles_whose_organization_is_unknown_or_missing():
    person = {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
    }

    doc = build_document(person, {140717: DELL})

    assert orjson.loads(doc.body) == {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999, "organization_resolved": False},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
        "unresolved_organization_ids": [999],
    }
    assert (doc.unresolved_refs, doc.has_affiliations) == (1, False)


def test_accepts_persons_whose_roles_are_absent_or_well_formed():
    check_person({"forager_id": 1})
    check_person({"forager_id": 1, "roles": None})
    check_person({"forager_id": 1, "roles": [{"organization_id": 140717}, {"organization_id": None}, {}]})


@pytest.mark.parametrize("roles, error", [
    ("Engineer", "roles is not a list"),
    ([None], r"roles\[0\] is not an object"),
    ([{"organization_id": 1}, {"organization_id": "140717"}], r"roles\[1\].organization_id is not an integer"),
    ([{"organization_id": True}], r"roles\[0\].organization_id is not an integer"),
])
def test_rejects_persons_whose_roles_cannot_be_joined(roles, error):
    with pytest.raises(MalformedRecord, match=error):
        check_person({"forager_id": 1, "roles": roles})
