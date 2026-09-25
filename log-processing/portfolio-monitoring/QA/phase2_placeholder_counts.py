#!/usr/bin/env python3
"""
Aggregate placeholder counts for Phase 2's three augmented fields
(entity_type, dataset_type, application_id) across both file_download
and hot_file_download -- a quick health snapshot, not an investigation
tool (see analyze_unresolved_entity_types.py for that).

'PENDING' is flagged as an error in BOTH tables, not just file_download:
both tables only ever load from a sentinel whose phase sequence includes
Phase 2 (.DONE.1.2.3.4 or .LOADED.1.2.3.4) -- the .2 means Phase 2 has
already run regardless of which table the file ends up in, so none of
these three fields should ever still be 'PENDING' in either one.

'NA' only ever occurs for dataset_type in practice (the entity_type !=
'Dataset' gate) -- checked for all three fields anyway, for a single,
uniform placeholder list rather than a different one per field; it's
harmless to check for a value that just never occurs for a given field.

Opened read_only=True: never writes, safe to run alongside a live loader.
"""
import sys

import duckdb

AUGMENTED_FIELDS = ("entity_type", "dataset_type", "application_id")
PLACEHOLDER_VALUES = ("PENDING", "UNTRACKED", "UNRESOLVED", "NA")
TABLES = ("file_download", "hot_file_download")

_PLACEHOLDER_LIST_SQL = ", ".join(f"'{v}'" for v in PLACEHOLDER_VALUES)


def main(db_path: str) -> None:
    con = duckdb.connect(db_path, read_only=True)

    table_totals = {
        table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in TABLES
    }

    for table in TABLES:
        for field in AUGMENTED_FIELDS:
            rows = con.execute(
                f"SELECT {field}, COUNT(*) AS cnt FROM {table} "
                f"WHERE {field} IN ({_PLACEHOLDER_LIST_SQL}) "
                f"GROUP BY {field} ORDER BY cnt DESC"
            ).fetchall()

            print(f"\n--- {table}.{field} placeholder counts ---")
            total = table_totals[table]
            if not rows:
                print("  (none -- every row has a real, resolved value)")
            for value, cnt in rows:
                pct = 100 * cnt / total if total else 0
                print(f"  {cnt:>10} ({pct:5.1f}%)  {value}")

            pending_count = next((cnt for v, cnt in rows if v == "PENDING"), 0)
            if pending_count > 0:
                print(
                    f"  !!! {pending_count} 'PENDING' rows in {table}.{field} -- "
                    "should be impossible; both file_download and "
                    "hot_file_download only ever load from sentinels where "
                    "Phase 2 has already run."
                )

    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 phase2_placeholder_counts.py <duckdb_path>")
        sys.exit(1)
    main(sys.argv[1])
