#!/usr/bin/env python3
"""
Read-only diagnostic: for every table in a DuckDB file, print its columns
(name + type), row count, and -- for any column whose type looks like a
date/timestamp -- its min and max value.

Opened read_only=True deliberately: DuckDB uses a single-writer file lock,
so this either connects safely alongside a live writer (if any) or fails
with a clear lock error, rather than risking a hang or writing anything.
"""
import sys
import duckdb

DATE_TYPE_MARKERS = ("DATE", "TIMESTAMP")


def inspect_database(db_path: str) -> None:
    print(f"\n{'=' * 60}\n{db_path}\n{'=' * 60}")
    con = duckdb.connect(db_path, read_only=True)

    tables = con.execute("SHOW TABLES").fetchall()
    if not tables:
        print("  (no tables found)")
        con.close()
        return

    for (table_name,) in tables:
        print(f"\n--- {table_name} ---")

        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        print("Columns:")
        for col in columns:
            # DESCRIBE's column order: column_name, column_type, null, key, default, extra
            print(f"  {col[0]}: {col[1]}")

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Row count: {row_count}")

        date_like_columns = [
            col[0] for col in columns
            if any(marker in col[1].upper() for marker in DATE_TYPE_MARKERS)
        ]
        for date_col in date_like_columns:
            oldest, newest = con.execute(
                f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM {table_name}'
            ).fetchone()
            print(f"  {date_col}: oldest={oldest}  newest={newest}")

    con.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 duckdb_diagnostic.py <db_path> [<db_path> ...]")
        sys.exit(1)
    for path in sys.argv[1:]:
        try:
            inspect_database(path)
        except Exception as e:
            print(f"\nFailed to inspect {path}: {e}")
            
