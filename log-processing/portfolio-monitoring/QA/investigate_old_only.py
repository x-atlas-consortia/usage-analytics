#!/usr/bin/env python3
"""
Follow-up to compare_duckdb_builds.py's "old build only" result. Two
competing explanations, each with a distinct, checkable signature:

  (a) NULL-key artifact: a meaningful fraction of the 1,454,785 "old-only"
      records have a NULL in one of the composite-key columns (most likely
      destination_ip), so they never match themselves across builds even
      though the same underlying event may genuinely exist in both. If
      this is the story, most old-only records should have a NULL key
      field, and they should be spread fairly evenly across
      source_node/source_prefix/source_date rather than clustering.

  (b) Genuine loss tied to a specific incident: old-only records cluster
      heavily around specific (source_node, source_prefix, source_date)
      combinations -- e.g. dates affected by this week's race condition --
      with few or no NULL key fields, pointing at a real, identifiable
      cause rather than a matching-key quirk.

UNTESTED against a real DuckDB engine -- verify the two counts below sum
to something close to the 1,454,785 total before trusting the breakdown.
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

    con.execute(f"""
        CREATE TEMP TABLE old_keys AS
        SELECT {KEY_COLUMNS_SQL} FROM old_db.file_download
    """)
    con.execute(f"""
        CREATE TEMP TABLE new_keys AS
        SELECT {KEY_COLUMNS_SQL} FROM new_db.file_download
    """)
    con.execute("""
        CREATE TEMP TABLE old_only AS
        SELECT * FROM old_keys EXCEPT SELECT * FROM new_keys
    """)

    total_old_only = con.execute("SELECT COUNT(*) FROM old_only").fetchone()[0]
    print(f"Total old-only records: {total_old_only}")

    # Test (a): how many old-only records have a NULL in any key column?
    null_check = " OR ".join(f'"{c}" IS NULL' for c in KEY_COLUMNS)
    null_count = con.execute(
        f"SELECT COUNT(*) FROM old_only WHERE {null_check}"
    ).fetchone()[0]
    print(f"\nOld-only records with a NULL in at least one key column: {null_count} "
          f"({null_count / total_old_only:.1%} of old-only)")

    # Which specific column is NULL most often, among old-only records
    print("\nNULL breakdown by column (old-only records):")
    for col in KEY_COLUMNS:
        cnt = con.execute(f'SELECT COUNT(*) FROM old_only WHERE "{col}" IS NULL').fetchone()[0]
        if cnt > 0:
            print(f"  {col}: {cnt}")

    # Test (b): do old-only records (the non-NULL ones) cluster by date?
    print("\nOld-only records with NO NULL key fields, by (source_node, source_date):")
    rows = con.execute(f"""
        SELECT source_node, source_date, COUNT(*) AS cnt
        FROM old_only
        WHERE NOT ({null_check})
        GROUP BY source_node, source_date
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    if not rows:
        print("  (none -- every old-only record has a NULL key field; "
          "points toward explanation (a))")
    for node, date, cnt in rows:
        print(f"  {node} {date}: {cnt}")

    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 investigate_old_only.py <old_db_path> <new_db_path>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
    
