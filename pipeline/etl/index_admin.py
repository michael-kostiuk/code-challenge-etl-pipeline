"""Index lifecycle: create with bulk-load settings and explicit mapping, then finalize."""
from __future__ import annotations

from elasticsearch import Elasticsearch

from etl.config import Config

_KEYWORD = {"type": "keyword", "ignore_above": 256}  # same limit as ES dynamic mapping
_TEXT_WITH_KEYWORD = {"type": "text", "fields": {"keyword": _KEYWORD}}
_DATE = {"type": "date", "ignore_malformed": True}  # one bad date must not reject the person


# `dynamic: false`: every field stays in _source, only the fields below are indexed.
# `roles` / `organizations` are `object`, not `nested`: the correctness suite runs plain
# `term` queries on their subfields, which do not match inside `nested`.
# Org `technologies` / `keywords` are left unindexed (indexing them measured 8% slower; EVALUATION.md).
MAPPINGS = {
    "dynamic": False,
    "properties": {
        "forager_id": {"type": "long"},
        "linkedin_id": {"type": "long"},
        "first_name": {"type": "text"},
        "last_name": {"type": "text"},
        "headline": {"type": "text"},
        "country": _KEYWORD,
        "city": _KEYWORD,
        "industry": _KEYWORD,
        "skills": _KEYWORD,
        "linkedin_slug": {**_KEYWORD, "doc_values": False},  # exact lookup only
        "roles": {
            "properties": {
                "role_title": _TEXT_WITH_KEYWORD,
                "organization_id": {"type": "long"},
                "organization_resolved": {"type": "boolean"},
                "start_date": _DATE,
                "end_date": _DATE,
            }
        },
        "organizations": {
            "properties": {
                "forager_id": {"type": "long"},
                "linkedin_id": {"type": "long"},
                "name": _TEXT_WITH_KEYWORD,
                "domain": _KEYWORD,
                "industry": _KEYWORD,
                "country": _KEYWORD,
            }
        },
        "unresolved_organization_ids": {"type": "long"},
        "affiliations": {"type": "object", "enabled": False},
    },
}


def create_index(es: Elasticsearch, cfg: Config) -> None:
    """Drops any previous index and creates it tuned for a one-off bulk load."""
    es.indices.delete(index=cfg.index_name, ignore_unavailable=True)
    es.indices.create(
        index=cfg.index_name,
        settings={
            "number_of_shards": cfg.shards,
            "number_of_replicas": 0,      # single node: a replica could never be assigned
            "refresh_interval": "-1",     # no searchable segments mid-load; refreshed once at the end
            "translog": {"durability": "async", "flush_threshold_size": "1gb"},
        },
        mappings=MAPPINGS,
    )


def finalize_index(es: Elasticsearch, cfg: Config) -> int:
    """Restores serving settings, makes all documents searchable, returns the document count."""
    es.indices.put_settings(
        index=cfg.index_name,
        settings={"index": {"refresh_interval": "1s", "translog.durability": "request"}},
    )
    es.indices.refresh(index=cfg.index_name)
    return es.count(index=cfg.index_name)["count"]
