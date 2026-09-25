#!/usr/bin/env python3
"""
Analyzes file_download records whose entity_type is 'UNRESOLVED' (the
entity_uuid extracted at Phase 1 doesn't match any node under Neo4j's
Dataset|Upload|Publication labels -- see check_unresolved_entity_types.py,
which confirmed via a 200-record sample that these come back 0% found
under any label at all, not merely under an unexpected one).

Per Karl's principle: the presence of a logged transfer in file_download
should never depend on whether its entity identifier currently resolves
to anything in Neo4j -- this script is purely investigative, not a QA
gate that flags rows for removal or exclusion.

Each analysis is its own function, called in sequence from main() --
meant to grow as this investigation continues. Add a new function and a
call to it in main() rather than folding new logic into an existing one.

Opened read_only=True: never writes, safe to run alongside a live loader.
"""
import argparse
from urllib.parse import unquote

import duckdb

# The filter every analysis in this script starts from. entity_type, not
# dataset_type -- the latter is 'NA' for this population under the
# current gating logic, not 'UNRESOLVED'; that condition can no longer
# occur for entity_type != 'Dataset' records (see augment_with_entity_info.py).
UNRESOLVED_FILTER = "entity_type = 'UNRESOLVED'"

# relative_file_path's leading-slash convention differs by protocol: HTTP's
# own value comes straight from the request line's path (e.g.
# "/protected/TMC/uuid/file" -- always leading-slash'd, since HTTP request
# paths always start with '/'), while GridFTP's comes from stripping
# ABS_PATH_BASE_TO_REMOVE off an absolute filesystem path, which in
# production consumes the leading slash too, leaving "protected/TMC/uuid/file"
# with NO leading slash. Confirmed directly: every gridftp-protocol sample
# this script pulled came back with no leading slash, every http-protocol
# sample had one. ltrim() normalizes both to the same shape before
# splitting -- without it, split_part(path, '/', 2) silently misreads
# every GridFTP path's scope position, since its first split token isn't
# the empty string an HTTP path's leading slash would produce.
_NORMALIZED_PATH_SQL = "ltrim(relative_file_path, '/')"
_SCOPE_SQL = f"split_part({_NORMALIZED_PATH_SQL}, '/', 1)"
_TMC_NAME_SQL = f"split_part({_NORMALIZED_PATH_SQL}, '/', 2)"


def sample_raw_paths(con: duckdb.DuckDBPyConnection, limit: int) -> None:
    """
    Prints a sample of raw paths behind UNRESOLVED records -- useful for
    eyeballing whether the path shape looks like a genuine entity
    directory with an unexpected/provisional identifier, or something
    else entirely (e.g. a different path convention Phase 1 mis-parsed).
    """
    print(f"--- sample raw paths (limit {limit}) ---")
    rows = con.execute(f"""
        SELECT entity_uuid, relative_file_path, source_node, source_date, protocol
        FROM file_download
        WHERE {UNRESOLVED_FILTER}
        LIMIT {int(limit)}
    """).fetchall()
    for entity_uuid, path, node, date, protocol in rows:
        print(f"\nuuid={entity_uuid}")
        print(f"  path={path}")
        print(f"  node={node}  date={date}  protocol={protocol}")


def date_node_clustering(con: duckdb.DuckDBPyConnection, limit: int) -> None:
    """
    Groups UNRESOLVED records by (source_node, source_date) -- a heavily
    top-loaded result (a handful of node/date pairs accounting for most
    of the total) points at one or a few specific incidents; an even
    spread across history would point at something structural instead.
    """
    print(f"\n--- date/node clustering, all UNRESOLVED records (top {limit}) ---")
    rows = con.execute(f"""
        SELECT source_node, source_date, COUNT(*) AS cnt
        FROM file_download
        WHERE {UNRESOLVED_FILTER}
        GROUP BY source_node, source_date
        ORDER BY cnt DESC
        LIMIT {int(limit)}
    """).fetchall()
    for node, date, cnt in rows:
        print(f"  {node} {date}: {cnt}")


def sample_non_tmc_paths(con: duckdb.DuckDBPyConnection, limit: int) -> None:
    """
    sample_raw_paths() pulls an arbitrary, unordered slice of ALL
    UNRESOLVED records -- which in practice surfaced only /protected/ and
    /consortium/ paths, even though tmc_breakdown() shows 99.9% of the
    population falls outside those two scopes entirely (genuine public
    scope, or some other, unrecognized path shape). Samples specifically
    from that dominant bucket instead, to see what it actually looks like
    rather than continuing to reason from the unrepresentative slice.
    """
    print(f"\n--- sample paths, public-scope/unrecognized bucket (limit {limit}) ---")
    rows = con.execute(f"""
        SELECT entity_uuid, relative_file_path, source_node, source_date, protocol
        FROM file_download
        WHERE {UNRESOLVED_FILTER}
          AND relative_file_path IS NOT NULL
          AND {_SCOPE_SQL} NOT IN ('protected', 'consortium')
        LIMIT {int(limit)}
    """).fetchall()
    for entity_uuid, path, node, date, protocol in rows:
        print(f"\nuuid={entity_uuid}")
        print(f"  path={path}")
        print(f"  node={node}  date={date}  protocol={protocol}")


def tmc_breakdown(con: duckdb.DuckDBPyConnection, limit: int) -> None:
    """
    Groups UNRESOLVED records by TMC name -- the path segment right after
    /protected/ or /consortium/ (e.g. "Beth Israel Deaconess Medical
    Center TMC"). A heavily top-loaded result would point at a specific
    TMC's ingest practices or a particular batch; an even spread would
    suggest something more general, not TMC-specific. Public-scope paths
    (a bare uuid at the top level, no TMC segment at all) are grouped
    separately under a fixed label, since there's no TMC name to extract
    there.

    Extraction and grouping happen in SQL, not by pulling every
    relative_file_path into Python -- the UNRESOLVED population is large
    (hundreds of thousands of rows), so this scales far better; only the
    small number of distinct TMC names, already counted, ever reaches
    Python.

    No SQL-level LIMIT here, deliberately: GridFTP TMC names never had
    percent-encoding to begin with (literal spaces from the filesystem),
    while HTTP's always do (e.g. "Beth%20Israel..."), so the same real
    TMC can come back as two distinct SQL-level groups (confirmed in
    production: "Beth Israel Deaconess Medical Center TMC" appeared
    twice, once from each protocol, with different counts). Decoding
    has to happen BEFORE merging those two groups' counts together, not
    after -- so every (small number of) distinct group is fetched, all
    decoded and merged in Python, and only then sorted and cut to
    `limit`. Cutting to `limit` before merging could silently rank a
    TMC's real, combined total wrong, or drop it from the printed list
    entirely, if its count happened to be split close to the cutoff.
    """
    print(f"\n--- TMC breakdown, all UNRESOLVED records (top {limit}) ---")
    rows = con.execute(f"""
        SELECT
            CASE
                WHEN {_SCOPE_SQL} IN ('protected', 'consortium')
                THEN {_TMC_NAME_SQL}
                ELSE NULL
            END AS tmc_name_encoded,
            COUNT(*) AS cnt
        FROM file_download
        WHERE {UNRESOLVED_FILTER} AND relative_file_path IS NOT NULL
        GROUP BY tmc_name_encoded
    """).fetchall()

    merged_counts: dict[str, int] = {}
    for tmc_name_encoded, cnt in rows:
        label = unquote(tmc_name_encoded) if tmc_name_encoded is not None \
            else "(public scope, or unrecognized path shape -- no TMC segment)"
        merged_counts[label] = merged_counts.get(label, 0) + cnt

    for label, cnt in sorted(merged_counts.items(), key=lambda kv: -kv[1])[:limit]:
        print(f"  {cnt:>10}  {label}")


def main(duckdb_path: str, sample_limit: int, cluster_limit: int, tmc_limit: int) -> None:
    con = duckdb.connect(duckdb_path, read_only=True)

    sample_raw_paths(con, sample_limit)
    sample_non_tmc_paths(con, sample_limit)
    date_node_clustering(con, cluster_limit)
    tmc_breakdown(con, tmc_limit)

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("duckdb_path")
    parser.add_argument("--sample-limit", type=int, default=15,
                         help="Rows to show in the raw-path sample.")
    parser.add_argument("--cluster-limit", type=int, default=15,
                         help="Rows to show in the date/node clustering breakdown.")
    parser.add_argument("--tmc-limit", type=int, default=15,
                         help="Rows to show in the TMC breakdown.")
    args = parser.parse_args()
    main(args.duckdb_path, args.sample_limit, args.cluster_limit, args.tmc_limit)
