"""
Compare record count and total bytes_transferred between two JSON files
representing the same underlying source (same node/date), but produced by
two different, independent pipelines:
  - globus-downloads-to-JSON (feeds DuckDB) -- schema known with confidence
  - file-downloads-to-S3 (feeds ES, via S3 -> index-JSON-to-ES) -- schema
    only INFERRED here (assumed to also use a top-level 'bytes_transferred'
    field, since that's the field name the ES index built from it uses, and
    nothing else in the ES mapping suggests a rename happened during
    indexing). The printed sample record and missing-field count below
    exist specifically so that assumption can be checked against the real
    data, not just trusted.

Usage:
    python3 compare_source_files.py FILE_A FILE_B
"""

import json
import sys


def summarize(path: str) -> None:
    with open(path) as f:
        data = json.load(f)

    print(f"\n{path}")

    if not isinstance(data, list):
        print(f"  top-level JSON is a {type(data).__name__}, not a list --"
              f" stopping here rather than guessing how to unwrap it.")
        return

    total_bytes = 0
    missing_field_count = 0
    for record in data:
        if isinstance(record, dict) and "bytes_transferred" in record:
            total_bytes += record["bytes_transferred"] or 0
        else:
            missing_field_count += 1

    print(f"  record count: {len(data)}")
    print(f"  total bytes_transferred: {total_bytes}")
    print(f"  records missing 'bytes_transferred' entirely: {missing_field_count}")
    if data:
        print(f"  sample record (first): {json.dumps(data[0], indent=2)}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 compare_source_files.py FILE_A FILE_B")
        sys.exit(1)
    summarize(sys.argv[1])
    summarize(sys.argv[2])
