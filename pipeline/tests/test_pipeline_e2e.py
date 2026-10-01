import gzip
import json
import os
import time
from pathlib import Path

import orjson
import pytest
from elasticsearch import Elasticsearch

from etl.config import Config
from etl.run import run

INDEX = "persons_e2e"


def _org(org_id, name):
    return {"id": org_id, "date_updated": "2026-01-01 00:00:00.000 Z",
            "serialized_data": {"forager_id": org_id, "name": name, "linkedin_id": org_id * 10}}


def _person(person_id, roles, organizations=()):
    return {"id": person_id, "date_updated": "2026-01-01 00:00:00.000 Z",
            "serialized_data": {"forager_id": person_id, "first_name": f"P{person_id}",
                                "roles": [{"role_title": title, "organization_id": org_id} for title, org_id in roles],
                                "organizations": list(organizations)}}


def _source(es, forager_id):
    [hit] = es.search(index=INDEX, query={"term": {"forager_id": forager_id}})["hits"]["hits"]
    return hit["_source"]


def _write_gz(path: Path, lines: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        fh.writelines(line + b"\n" for line in lines)


def _fixture(tmp_path: Path, **overrides) -> Config:
    data = tmp_path / "data"
    _write_gz(data / "organization" / "orgs_0.json.gz",
              [orjson.dumps(_org(1, "Dell Technologies")), orjson.dumps(_org(2, "Acme")),
               orjson.dumps(_org(3, "Globex"))])
    _write_gz(data / "person" / "persons_0.json.gz", [
        orjson.dumps(_person(101, [("Project Manager", 1), ("Engineer", 2)])),
        orjson.dumps(_person(102, [("Project Manager", 1), ("Project Manager", 1)])),
        orjson.dumps(_person(103, [("Analyst", 999)], organizations=[{"name": "Chess Club"}])),
    ])
    _write_gz(data / "person" / "persons_1.json.gz", [
        orjson.dumps(_person(104, [("Project Manager", None)])),
        b"{not json",
        orjson.dumps(_person(105, [])),
        orjson.dumps(_person(106, [("Director", 3), ("Intern", 998)])),
    ])
    env = {**os.environ, "DATA_DIR": str(data), "OUT_DIR": str(tmp_path / "out"),
           "STAGE_DIR": str(tmp_path / "stage"), "INDEX_NAME": INDEX, "WORKERS": "2",
           "LOADERS": "1", "SHARDS": "1", "PROGRESS_INTERVAL_S": "60", **overrides}
    return Config.from_env(env)


@pytest.fixture
def es():
    client = Elasticsearch(Config.from_env().es_url)
    if not client.ping():
        pytest.skip("elasticsearch not reachable")
    yield client
    client.indices.delete(index=INDEX, ignore_unavailable=True)


@pytest.mark.parametrize("backend", ["redis", "lmdb", "sqlite"])
def test_pipeline_indexes_joined_persons_and_reruns_cleanly(es, tmp_path, backend):
    cfg = _fixture(tmp_path, ORG_STORE=backend)

    assert run(cfg) == 0
    assert run(cfg) == 0  # second run on a populated index and org store gives the same result

    def count(query):
        return es.count(index=INDEX, query=query)["count"]

    assert count({"match_all": {}}) == 6
    assert count({"term": {"roles.role_title.keyword": "Project Manager"}}) == 3
    assert count({"term": {"organizations.name.keyword": "Dell Technologies"}}) == 2
    assert count({"exists": {"field": "unresolved_organization_ids"}}) == 2
    p101 = _source(es, 101)
    assert [(o["name"], o["linkedin_id"]) for o in p101["organizations"]] == [("Dell Technologies", 10), ("Acme", 20)]
    p103 = _source(es, 103)
    assert p103["affiliations"] == [{"name": "Chess Club"}]
    assert p103["unresolved_organization_ids"] == [999]
    assert p103["roles"][0]["organization_resolved"] is False
    dead = [json.loads(line) for line in (tmp_path / "out" / "dead_letter.ndjson").read_text().splitlines()]
    assert [(d["kind"], d["file"], d["line"]) for d in dead] == [("malformed_person", "persons_1.json.gz", 2)]
    metrics = json.loads((tmp_path / "out" / "metrics.json").read_text())
    assert (metrics["ok"], metrics["es_count"], metrics["counters"]["unresolved_refs"],
            metrics["counters"]["persons_with_affiliations"]) == (True, 6, 2, 1)


def test_run_exits_nonzero_when_org_store_unreachable(tmp_path):
    cfg = _fixture(tmp_path, ORG_STORE="redis", REDIS_URL="redis://127.0.0.1:1/0")

    started = time.monotonic()
    assert run(cfg) == 1
    assert time.monotonic() - started < 30
