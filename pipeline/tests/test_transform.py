import orjson

from etl.transform import build_document

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

    doc, unresolved = build_document(person, {140717: DELL})

    assert orjson.loads(doc) == {
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
    assert unresolved == 0


def test_keeps_and_flags_roles_whose_organization_is_unknown_or_missing():
    person = {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
    }

    doc, unresolved = build_document(person, {140717: DELL})

    assert orjson.loads(doc) == {
        "forager_id": 2,
        "roles": [
            {"role_title": "Analyst", "organization_id": 999, "organization_resolved": False},
            {"role_title": "Founder", "organization_id": None},
        ],
        "organizations": [],
        "unresolved_organization_ids": [999],
    }
    assert unresolved == 1
