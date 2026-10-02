"""Index lifecycle: create with bulk-load settings and explicit mapping, then finalize."""
from __future__ import annotations

from elasticsearch import Elasticsearch

from etl.config import Config

_ID = {"type": "keyword"}  # identifiers: exact lookups and joins, never ranges, so keyword over long
_KEYWORD = {"type": "keyword", "ignore_above": 256}  # same limit as ES dynamic mapping
_TEXT_WITH_KEYWORD = {"type": "text", "fields": {"keyword": _KEYWORD}}
_DATE = {"type": "date", "ignore_malformed": True}  # one bad date must not reject the person
# Feed timestamps: "2026-03-29 02:32:15.459 Z", and some orgs carry an offset ("... -0700").
_UPDATED = {**_DATE, "format": "yyyy-MM-dd HH:mm:ss.SSS X||strict_date_optional_time||epoch_millis"}


# `dynamic: false`: every field stays in _source, only the fields below are indexed.
# `roles` / `organizations` are `object`, not `nested`: the correctness suite runs plain
# `term` queries on their subfields, which do not match inside `nested`.
# Org `technologies` / `keywords` are left unindexed (indexing them measured 8% slower; EVALUATION.md).
MAPPINGS = {
    "dynamic": False,
    "properties": {
        "forager_id": _ID,
        "linkedin_id": _ID,
        "date_updated": _UPDATED,
        "first_name": _TEXT_WITH_KEYWORD,
        "last_name": _TEXT_WITH_KEYWORD,
        "headline": {"type": "text"},
        "country": _KEYWORD,
        "city": _KEYWORD,
        "industry": _KEYWORD,
        "skills": _KEYWORD,
        "linkedin_slug": {**_KEYWORD, "doc_values": False},  # exact lookup only
        "roles": {
            "properties": {
                "role_title": _TEXT_WITH_KEYWORD,
                "organization_id": _ID,
                "organization_resolved": {"type": "boolean"},
                "start_date": _DATE,
                "end_date": _DATE,
            }
        },
        "organizations": {
            "properties": {
                "forager_id": _ID,
                "linkedin_id": _ID,
                "date_updated": _UPDATED,
                "name": _TEXT_WITH_KEYWORD,
                "domain": _KEYWORD,
                "industry": _KEYWORD,
                "country": _KEYWORD,
            }
        },
        "unresolved_organization_ids": _ID,
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
