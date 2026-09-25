"""
Join two JSON source files on (dataset_uuid, relative_file_path,
download_date_time) -- precise enough to identify the same real transfer
event across two independent pipelines parsing the same raw log line --
and classify every record as matched (in both), A-only, or B-only.

For matched records, also reports how many disagree on bytes_transferred
despite matching on identity, which would point to a value-level bug
distinct from a coverage/inclusion difference.

destination_ip == 127.0.0.1 records are excluded from FILE_B (assumed to
be the ES-feeding file) before joining, per Karl's finding that the
DuckDB-feeding pipeline never includes loopback transfers at all --
without this exclusion, every loopback record would show up as a
misleading "B-only" entry.

Usage:
    python3 join_source_files.py FILE_A FILE_B
"""

import json
import sys


def load_keyed(path: str, exclude_loopback: bool = False) -> dict:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        print(f"{path}: top-level JSON is a {type(data).__name__}, not a list -- stopping.")
        sys.exit(1)

    keyed = {}
    excluded = 0
    for record in data:
        if not isinstance(record, dict):
            continue
        if exclude_loopback and record.get("destination_ip") == "127.0.0.1":
            excluded += 1
            continue
        key = (
            record.get("dataset_uuid"),
            record.get("relative_file_path"),
            record.get("download_date_time"),
        )
        keyed.setdefault(key, []).append(record.get("bytes_transferred"))

    if exclude_loopback:
        print(f"{path}: excluded {excluded} loopback (127.0.0.1) record(s)")
    return keyed


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python3 join_source_files.py FILE_A FILE_B")
        return 1
    path_a, path_b = sys.argv[1], sys.argv[2]

    keyed_a = load_keyed(path_a, exclude_loopback=False)
    keyed_b = load_keyed(path_b, exclude_loopback=True)

    keys_a = set(keyed_a)
    keys_b = set(keyed_b)
    only_a = keys_a - keys_b
    only_b = keys_b - keys_a
    matched = keys_a & keys_b

    bytes_only_a = sum(v for k in only_a for v in keyed_a[k] if isinstance(v, (int, float)))
    bytes_only_b = sum(v for k in only_b for v in keyed_b[k] if isinstance(v, (int, float)))

    disagreeing = 0
    for k in matched:
        if sorted(keyed_a[k]) != sorted(keyed_b[k]):
            disagreeing += 1

    print(f"\nFILE A: {path_a} ({len(keys_a)} distinct keys)")
    print(f"FILE B: {path_b} ({len(keys_b)} distinct keys, post loopback exclusion)")
    print(f"\nmatched keys (in both):        {len(matched)}")
    print(f"  of which bytes disagree:     {disagreeing}")
    print(f"A-only keys (missing from B):  {len(only_a)}  ({bytes_only_a} bytes)")
    print(f"B-only keys (missing from A):  {len(only_b)}  ({bytes_only_b} bytes)")

    if only_b:
        print("\nSample B-only (missing from A) keys, up to 10:")
        for k in list(only_b)[:10]:
            print(f"  {k}  bytes={keyed_b[k]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
