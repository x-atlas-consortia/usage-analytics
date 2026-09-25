#!/usr/bin/env python3
"""
Match records between two DuckDB builds using a composite key built only
from fields present in BOTH schemas (destination_ip, relative_file_path,
download_date_time, bytes_transferred, source_node, source_prefix,
source_date) -- deliberately excluding dataset_type (only in the new
build) and dataset_uuid (can be NULL for UNTRACKED records, which would
make NULL-vs-NULL key comparisons unreliable in SQL).

Unions file_download AND hot_file_download on BOTH sides before comparing
-- a real record found missing from file_download alone turned out to be
sitting in hot_file_download instead (a file whose sentinel is still
.LOADED, correctly, because it has an unresolved PENDING user_info value
pending that month's usage-CSV delivery). An earlier version of this
script assumed the OLD build predated the file_download/hot_file_download
split and left it un-unioned -- that assumption was wrong to make without
checking: _ensure_schema() creates both tables unconditionally on every
run regardless of build date, so any build could have rows in either one,
and there's no principled reason to treat the two databases differently.

CAVEAT: any record with a NULL value in one of the key fields above will
not match itself across builds, since SQL NULL <> NULL. Should be rare --
these fields are core to what makes a record "interesting" in Phase 1 --
but not impossible; treat a nonzero old_only/new_only count as an upper
bound on genuine gaps, not an exact one, until spot-checked.

Reports:
  - records only in the OLD build (present before, missing now) -- count
    only; dataset_type isn't available for these from either database.
  - records only in the NEW build (added since the old build) -- broken
    down by dataset_type, since that IS available for these.

UNTESTED against a real DuckDB engine (no install available where this was
written) -- verify on a small scale before trusting the full output.
"""
import sys
import duckdb

KEY_COLUMNS = (
    "source_node", "source_prefix", "source_date",
    "destination_ip", "relative_file_path", "download_date_time", "bytes_transferred",
)
KEY_COLUMNS_SQL = ", ".join(KEY_COLUMNS)


def main(old_db_path: str, new_db_path: str) -> None:
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{old_db_path}' AS old_db (READ_ONLY)")
    con.execute(f"ATTACH '{new_db_path}' AS new_db (READ_ONLY)")

    # Unioned on BOTH sides: a record can legitimately sit in
    # hot_file_download instead of file_download if its file's sentinel is
    # still .LOADED (unresolved PENDING user_info, pending that month's
    # usage-CSV delivery) -- not missing, just in the other table. True for
    # either database, not just the newer one.
    con.execute(f"""
        CREATE TEMP TABLE old_keys AS
        SELECT {KEY_COLUMNS_SQL} FROM old_db.file_download
        UNION
        SELECT {KEY_COLUMNS_SQL} FROM old_db.hot_file_download
    """)
    con.execute(f"""
        CREATE TEMP TABLE new_keys AS
        SELECT {KEY_COLUMNS_SQL} FROM new_db.file_download
        UNION
        SELECT {KEY_COLUMNS_SQL} FROM new_db.hot_file_download
    """)

    old_total = con.execute("SELECT COUNT(*) FROM old_keys").fetchone()[0]
    new_total = con.execute("SELECT COUNT(*) FROM new_keys").fetchone()[0]
    print(f"Old build (file_download + hot_file_download, unioned): {old_total} records")
    print(f"New build (file_download + hot_file_download, unioned): {new_total} records")

    con.execute("""
        CREATE TEMP TABLE old_only AS
        SELECT * FROM old_keys EXCEPT SELECT * FROM new_keys
    """)
    con.execute("""
        CREATE TEMP TABLE new_only AS
        SELECT * FROM new_keys EXCEPT SELECT * FROM old_keys
    """)

    old_only_count = con.execute("SELECT COUNT(*) FROM old_only").fetchone()[0]
    new_only_count = con.execute("SELECT COUNT(*) FROM new_only").fetchone()[0]

    print(f"\nIn old build only (present before, missing now): {old_only_count}")
    print("  (dataset_type unavailable for these -- not in old schema, not in new build)")

    print(f"\nIn new build only (added since the old build): {new_only_count}")
    if new_only_count > 0:
        print("  Broken down by dataset_type:")
        join_cond = " AND ".join(f'n."{c}" = f."{c}"' for c in KEY_COLUMNS)
        rows = con.execute(f"""
            SELECT f.dataset_type, COUNT(*) AS cnt
            FROM new_only n
            JOIN (
                SELECT * FROM new_db.file_download
                UNION ALL
                SELECT * FROM new_db.hot_file_download
            ) f ON {join_cond}
            GROUP BY f.dataset_type
            ORDER BY cnt DESC
        """).fetchall()
        for dataset_type, cnt in rows:
            print(f"    {cnt:>10}  {dataset_type}")

    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 compare_duckdb_builds.py <old_db_path> <new_db_path>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
    
