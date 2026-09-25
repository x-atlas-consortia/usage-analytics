#!/usr/bin/env python3
"""
Samples records present in the old DuckDB build but not the new one, with
enough fields to locate the source JSON file (source_node + source_prefix
+ source_date) and to jq-filter for the specific record within it (no
line number available -- provenance.source_log_line was never loaded into
DuckDB, and these files are single-line JSON anyway, so line numbers
wouldn't help even if they were).

For each sampled record, also prints a ready-to-run jq command that checks
whether a matching record still exists in the CURRENT JSON file on disk.
That result distinguishes two very different explanations:
  - still present on disk  -> a duckdb_loader.py loading gap, not data loss
  - absent from the current file -> re-extraction hasn't recovered it,
    worth checking next whether the raw .gz source log for that
    node/date still exists at all (rotation would mean permanent loss)

UNTESTED against a real DuckDB engine -- inspect the sample output before
trusting it at scale.
"""
import sys
import duckdb

KEY_COLUMNS = (
    "source_node", "source_prefix", "source_date",
    "destination_ip", "relative_file_path", "download_date_time", "bytes_transferred",
)
KEY_COLUMNS_SQL = ", ".join(KEY_COLUMNS)

# Extra fields, beyond the matching key itself, useful for a human looking
# at the sample and for building a precise jq filter.
#
# NOTE: this reads specifically from old_db (see the JOIN below). If
# old_db is ever a database that predates the dataset_uuid -> entity_uuid
# rename, this column reference will need reverting to dataset_uuid for
# that specific comparison.
DETAIL_COLUMNS = ("entity_uuid", "dataset_type", "globus_task_id", "protocol")


def main(old_db_path: str, new_db_path: str, sample_size: int = 20) -> None:
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

    # dataset_type isn't in old_db's schema at all (it's the field that
    # didn't exist yet when this build ran), so it's excluded here --
    # there's nowhere to pull it from for an old-only record.
    detail_cols = [c for c in DETAIL_COLUMNS if c != "dataset_type"]
    all_cols = list(KEY_COLUMNS) + detail_cols
    select_cols_sql = ", ".join(f"o.{c}" for c in all_cols)

    join_cond = " AND ".join(f'oo."{c}" = o."{c}"' for c in KEY_COLUMNS)
    rows = con.execute(f"""
        SELECT {select_cols_sql}
        FROM old_only oo
        JOIN old_db.file_download o ON {join_cond}
        ORDER BY o.source_date DESC
        LIMIT {sample_size}
    """).fetchall()

    for row in rows:
        record = dict(zip(all_cols, row))
        print("\n" + "=" * 60)
        for k, v in record.items():
            print(f"  {k}: {v}")

        print(f"  -> source file: node={record['source_node']!r} "
              f"prefix={record['source_prefix']!r} date={record['source_date']!r} "
              "(confirm exact filename pattern against what's on disk)")

        jq_filter = (
            f'.[] | select(.destination_ip == "{record["destination_ip"]}" '
            f'and .relative_file_path == "{record["relative_file_path"]}" '
            f'and .bytes_transferred == {record["bytes_transferred"]})'
        )
        print(f"  -> jq check against the CURRENT file for this node/date:")
        print(f'     jq \'{jq_filter}\' <path_to_current_json_file>')

    con.close()


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python3 sample_old_only.py <old_db_path> <new_db_path> [sample_size]")
        sys.exit(1)
    n = int(sys.argv[3]) if len(sys.argv) == 4 else 20
    main(sys.argv[1], sys.argv[2], n)
