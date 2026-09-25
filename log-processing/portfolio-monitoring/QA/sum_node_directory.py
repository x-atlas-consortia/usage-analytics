"""
Sum record count and total bytes_transferred across every genuine data
file (matched by a *.json glob, which naturally excludes sentinel marker
files like *.json.DONE.1.2.3 since those don't end in '.json') in one
node directory -- for comparing a pipeline's total coverage against the
other pipeline's.

This will take real time to run -- potentially many files, some over
200MB each, all loaded and parsed one at a time (not all in memory at
once, to keep this from being a memory problem on a node with many files).

Usage:
    python3 sum_node_directory.py DIRECTORY [--exclude-loopback] [--protocol gridftp|http]
"""

import json
import sys
from pathlib import Path


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("Usage: python3 sum_node_directory.py DIRECTORY [--exclude-loopback] [--protocol gridftp|http]")
        return 1
    directory = Path(args[0])
    exclude_loopback = "--exclude-loopback" in args
    protocol_filter = None
    if "--protocol" in args:
        protocol_filter = args[args.index("--protocol") + 1]
    if not directory.is_dir():
        print(f"Not a directory: {directory}")
        return 1

    total_records = 0
    total_bytes = 0
    files_processed = 0
    files_skipped = []
    non_numeric_bytes_examples = []
    loopback_excluded_count = 0
    protocol_excluded_count = 0

    for path in sorted(directory.glob("*.json")):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            files_skipped.append((path.name, str(e)))
            continue

        if not isinstance(data, list):
            files_skipped.append((path.name, f"top-level JSON is a {type(data).__name__}"))
            continue

        for record in data:
            if not isinstance(record, dict):
                continue
            if protocol_filter is not None and record.get("protocol") != protocol_filter:
                protocol_excluded_count += 1
                continue
            if exclude_loopback and record.get("destination_ip") == "127.0.0.1":
                loopback_excluded_count += 1
                continue
            total_records += 1
            value = record.get("bytes_transferred")
            if isinstance(value, (int, float)):
                total_bytes += value
            elif value is not None:
                if len(non_numeric_bytes_examples) < 10:
                    non_numeric_bytes_examples.append((path.name, repr(value)))
        files_processed += 1

    print(f"{directory}")
    if protocol_filter is not None:
        print(f"  protocol filter: {protocol_filter} ({protocol_excluded_count} other-protocol records excluded)")
    if exclude_loopback:
        print(f"  loopback (127.0.0.1) records excluded: {loopback_excluded_count}")
    print(f"  files processed: {files_processed}")
    print(f"  total records: {total_records}")
    print(f"  total bytes_transferred (numeric values only): {total_bytes}")
    if non_numeric_bytes_examples:
        print(f"  WARNING: found non-numeric, non-null bytes_transferred values"
              f" (excluded from the sum above), examples:")
        for name, value in non_numeric_bytes_examples:
            print(f"    {name}: {value}")
    if files_skipped:
        print(f"  files skipped ({len(files_skipped)}):")
        for name, reason in files_skipped:
            print(f"    {name}: {reason}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
