#!/usr/bin/env python3
"""
QA check against a DuckDB file produced by duckdb_loader.py:
  1. Value distribution for both dataset_type and application_id, in both
     file_download and hot_file_download -- flags if 'PENDING' shows up
     in file_download for either field, which shouldn't be possible once
     a file has been through all four phases. Extended from an earlier,
     dataset_type-only version once application_id was added -- both
     fields share the same resolution states (PENDING/UNRESOLVED/
     UNTRACKED/real value), so the same check applies to each.
  2. file_download_ledger's key count, to compare against a fresh
     `find ... -name 'g*.json.DONE.*' | wc -l` on disk.

Opened read_only=True: never writes, safe to run alongside a live loader.
"""
import sys
import duckdb

FIELDS_TO_CHECK = ("dataset_type", "application_id")
TABLES_TO_CHECK = ("file_download", "hot_file_download")


def main(db_path: str) -> None:
    con = duckdb.connect(db_path, read_only=True)

    for field in FIELDS_TO_CHECK:
        for table in TABLES_TO_CHECK:
            print(f"\n--- {table}: {field} distribution ---")
            rows = con.execute(
                f"SELECT {field}, COUNT(*) AS cnt FROM {table} "
                f"GROUP BY {field} ORDER BY cnt DESC"
            ).fetchall()
            for value, cnt in rows:
                print(f"{cnt:>10}  {value}")

            if table == "file_download":
                pending_count = next((cnt for v, cnt in rows if v == "PENDING"), 0)
                if pending_count > 0:
                    print(
                        f"\n  !!! {pending_count} 'PENDING' rows for {field} in "
                        f"{table} -- should be impossible; a file only reaches "
                        "DuckDB after all four phases, and Phase 2 never leaves "
                        "a record at 'PENDING'."
                    )

    ledger_keys = con.execute(
        "SELECT node, prefix, date FROM file_download_ledger"
    ).fetchall()
    print(f"\n--- file_download_ledger ---")
    print(f"{len(ledger_keys)} keys in the ledger")
    print(
        "Compare against: find <PIPELINE_OUTPUT_DIR> -name 'g*.json.DONE.*' | wc -l"
    )

    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 qa_dataset_type.py <duckdb_path>")
        sys.exit(1)
    main(sys.argv[1])
