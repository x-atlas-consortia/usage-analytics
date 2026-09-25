#!/usr/bin/env python3
"""
Originally built to test the hypothesis that dataset_type='UNRESOLVED'
records were disproportionately non-Dataset entities (Upload/Publication)
rather than genuine Datasets whose type failed to resolve for some other
reason. That's since been fixed directly: augment_with_entity_info.py now
gates dataset_type on entity_type=='Dataset', giving non-Dataset entities
'NA' instead of 'UNRESOLVED'.

REPURPOSED AGAIN: real data showed entity_type itself resolves to
'UNRESOLVED' for a large share of NA-dataset_type records (60% in one
production sample -- not a rounding error, a real, substantial
population). "UNRESOLVED" there means entity_uuid wasn't found under any
of entity_info_provider.py's three matched labels (Dataset|Upload|
Publication) at all -- collapsed into the same 'NA' dataset_type outcome
as a confirmed Upload/Publication, even though it's a different, more
concerning situation: we don't actually know what these entities are.

This version samples entity_type='UNRESOLVED' records specifically (NOT
dataset_type='UNRESOLVED' -- that condition can no longer occur under the
new gating logic, querying for it would silently return nothing) and
queries Neo4j WITHOUT restricting to those three labels, fetching
labels(d) as a fallback alongside d.entity_type -- if these uuids exist
in Neo4j under some other label entirely (a Collection, a Sample, or
anything else with downloadable files that
entity_info_provider.py's crosswalk was never built to cover),
labels(d) should reveal it even if entity_type itself is unset on
those nodes.

Run from analytics-platform-loading/queries/ (or anywhere the shared
logProcessingProject.ini is reachable via the usual candidate paths) --
needs the same Neo4j access entity_info_provider.py already has.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

from log_extract_xfer_utils import LogExtractXferUtils

_SHARED_INI_CANDIDATES = [
    Path('logProcessingProject.ini'),
    Path('../src/logProcessingProject.ini'),      # cwd = portfolio-monitoring/ (confirmed working)
    Path('../../src/logProcessingProject.ini'),   # cwd = analytics-platform-loading/queries/
]

# uuids from our own database, not arbitrary external input, but still
# validated before being inlined into a Cypher query string -- cheap
# insurance, and query_neo4j()'s own signature (single string, no
# parameter binding visible from entity_info_provider.py's usage) doesn't
# obviously support bound parameters to use instead.
_UUID_RE = re.compile(r"^[0-9a-fA-F]{32}$")


def resolve_shared_ini(explicit_path: str | None = None) -> Path:
    # An explicit --shared-ini always wins, and skips guessing entirely --
    # this script gets run from more than one directory in practice (at
    # least queries/ and portfolio-monitoring/ so far), so a candidate
    # list alone keeps needing a new entry added after the fact.
    if explicit_path is not None:
        p = Path(explicit_path)
        if not p.is_file():
            raise FileNotFoundError(f"--shared-ini path does not exist: {explicit_path}")
        return p.resolve()
    for candidate in _SHARED_INI_CANDIDATES:
        if candidate.is_file():
            return candidate.resolve()
    raise FileNotFoundError(
        f"Unable to find logProcessingProject.ini in any expected location: "
        f"{[str(c) for c in _SHARED_INI_CANDIDATES]}. Pass --shared-ini "
        f"<path> to skip guessing entirely."
    )


def main(duckdb_path: str, sample_size: int, shared_ini: str | None = None) -> None:
    con = duckdb.connect(duckdb_path, read_only=True)
    rows = con.execute(
        "SELECT DISTINCT entity_uuid FROM file_download "
        "WHERE entity_type = 'UNRESOLVED' AND entity_uuid IS NOT NULL "
        f"LIMIT {int(sample_size)}"
    ).fetchall()
    con.close()

    sample_uuids = [row[0] for row in rows if _UUID_RE.match(row[0] or "")]
    skipped = len(rows) - len(sample_uuids)
    print(f"Sampled {len(rows)} distinct entity_type='UNRESOLVED' entity_uuid values"
          f" ({skipped} skipped for not looking like a 32-char hex uuid).")
    if not sample_uuids:
        print("Nothing to look up.")
        return

    portfolio_utils = LogExtractXferUtils(config_file_name=str(resolve_shared_ini(shared_ini)))
    uuid_list = ", ".join(f"'{u}'" for u in sample_uuids)
    # No label restriction -- deliberately broader than
    # entity_info_provider.py's own Dataset|Upload|Publication-only query,
    # since the whole point here is checking whether these uuids exist
    # under some OTHER label entirely. labels(d) fetched as a fallback
    # alongside d.entity_type, in case entity_type itself isn't set on
    # whatever these nodes turn out to be.
    query = (
        f"MATCH (d) WHERE d.uuid IN [{uuid_list}] "
        "RETURN d.uuid AS uuid, d.entity_type AS entity_type, labels(d) AS labels"
    )
    neo4j_rows = portfolio_utils.query_neo4j(query)

    found_by_uuid = {r["uuid"]: (r["entity_type"], r["labels"]) for r in neo4j_rows}
    entity_type_counts: dict[str, int] = {}
    labels_for_missing_entity_type: dict[str, int] = {}
    not_found_count = 0
    for u in sample_uuids:
        if u not in found_by_uuid:
            not_found_count += 1
            continue
        entity_type, labels = found_by_uuid[u]
        if entity_type is not None:
            entity_type_counts[entity_type] = entity_type_counts.get(entity_type, 0) + 1
        else:
            labels_key = ",".join(sorted(labels)) if labels else "(no labels)"
            labels_for_missing_entity_type[labels_key] = labels_for_missing_entity_type.get(labels_key, 0) + 1

    print(f"\nOf {len(sample_uuids)} sampled entity_type='UNRESOLVED' entity_uuid values:")
    for entity_type, count in sorted(entity_type_counts.items(), key=lambda kv: -kv[1]):
        pct = 100 * count / len(sample_uuids)
        print(f"  {count:>6} ({pct:5.1f}%)  entity_type property = {entity_type!r}")
    for labels_key, count in sorted(labels_for_missing_entity_type.items(), key=lambda kv: -kv[1]):
        pct = 100 * count / len(sample_uuids)
        print(f"  {count:>6} ({pct:5.1f}%)  found, but entity_type property unset -- Neo4j labels(d) = {labels_key}")
    if not_found_count:
        pct = 100 * not_found_count / len(sample_uuids)
        print(f"  {not_found_count:>6} ({pct:5.1f}%)  not found in Neo4j under ANY label at all")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("duckdb_path")
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument("--shared-ini", default=None,
                         help="Explicit path to logProcessingProject.ini, "
                              "skipping the candidate-path guessing entirely.")
    args = parser.parse_args()
    main(args.duckdb_path, args.sample_size, args.shared_ini)
