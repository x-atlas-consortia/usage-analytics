"""
Distribution stats for bytes_transferred in a single JSON source file --
count, sum, mean, median, max, and the top N largest individual values
(with dataset_uuid/relative_file_path so they can be cross-referenced
against the other pipeline's file if needed).

Meant to be run once per file, to see whether a gap between two files is
spread evenly across records (pointing at something systematic, like
double-counting bytes across multiple log lines for one transfer) or
concentrated in a few outliers (pointing at a parsing bug on specific
records).

Usage:
    python3 bytes_distribution.py FILE [--top N]
"""

import json
import statistics
import sys


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("Usage: python3 bytes_distribution.py FILE [--top N]")
        return 1
    path = args[0]
    top_n = 10
    if "--top" in args:
        top_n = int(args[args.index("--top") + 1])

    with open(path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"Top-level JSON is a {type(data).__name__}, not a list -- stopping.")
        return 1

    values = []
    for record in data:
        if isinstance(record, dict) and isinstance(record.get("bytes_transferred"), (int, float)):
            values.append((
                record["bytes_transferred"],
                record.get("dataset_uuid"),
                record.get("relative_file_path"),
            ))

    if not values:
        print("No records with a numeric bytes_transferred found.")
        return 1

    just_bytes = [v[0] for v in values]
    print(f"{path}")
    print(f"  records with numeric bytes_transferred: {len(values)}")
    print(f"  sum: {sum(just_bytes)}")
    print(f"  mean: {statistics.mean(just_bytes):.1f}")
    print(f"  median: {statistics.median(just_bytes)}")
    print(f"  max: {max(just_bytes)}")
    print(f"  top {top_n} by bytes_transferred:")
    for bytes_val, uuid, rel_path in sorted(values, key=lambda v: v[0], reverse=True)[:top_n]:
        print(f"    {bytes_val:>15}  {uuid}  {rel_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
