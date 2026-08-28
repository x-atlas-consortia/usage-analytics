#!/usr/bin/env python3
"""
Audit the actual content of pipeline JSON output files, as opposed to
audit_stage_completeness.py's structural/marker checks.

Usage:
  python3 audit_json_content.py --command=COUNT --element=user_info.user_tld

Recognized --command values: COUNT, ROLLUP, VALUES, FILENAME
Recognized --element values: user_info.user_tld

For a given element, there are up to three dedicated methods -- one per command --
named <element_with_dots_as_underscores>_<command, lowercase>. The dispatch table
below maps (element, command) pairs to those functions explicitly, rather than
constructing names dynamically, so a typo in a function name fails loudly at
definition time instead of silently missing at dispatch time.
"""
import argparse
import json
import sys
from pathlib import Path

JSON_FILE_NIGHTLY_DIR = "/hive/hubmap/pitt-analytics/globus-downloads-to-JSON"

RECOGNIZED_COMMANDS = ["COUNT", "ROLLUP", "VALUES", "FILENAME"]
RECOGNIZED_ELEMENTS = ["user_info.user_tld"]


def all_json_files():
    """Every *.json data file under JSON_FILE_NIGHTLY_DIR, at any depth. Markers
    (*.json.DONE.1, etc.) don't match this glob -- they don't end in exactly '.json'."""
    return sorted(Path(JSON_FILE_NIGHTLY_DIR).rglob("*.json"))


# --- user_info.user_tld: COUNT -------------------------------------------------------
#
# "Missing, empty, or null" for user_tld specifically means: the user_info object
# itself is missing or isn't an object, the user_tld key is missing from it, or its
# value is JSON null or an empty string. UNDETERMINED (the placeholder this pipeline
# actually uses for user_tld when it can't yet be determined) gets its own accumulator,
# separate from both present and absent -- it's a real value, just not a resolved one.
def user_info_user_tld_count():
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_file_counts = {d.name: 0 for d in root.iterdir() if d.is_dir()}

    file_count = 0
    total_objects = 0
    present_count = 0
    undetermined_count = 0
    absent_count = 0

    for json_file in all_json_files():
        file_count += 1
        node_file_counts[json_file.parent.name] = node_file_counts.get(json_file.parent.name, 0) + 1

        with open(json_file, 'r') as f:
            records = json.load(f)

        if not isinstance(records, list):
            print(f"Warning: {json_file} does not contain a top-level JSON array; skipping its contents.")
            continue

        total_objects += len(records)

        for record in records:
            user_info = record.get('user_info') if isinstance(record, dict) else None
            tld = user_info.get('user_tld') if isinstance(user_info, dict) else None
            category = _classify_user_tld(tld)
            if category == 'absent':
                absent_count += 1
            elif category == 'UNDETERMINED':
                undetermined_count += 1
            else:
                present_count += 1

    breakdown = ", ".join(f"{node}-{count}" for node, count in sorted(node_file_counts.items()))
    print(f"Files processed: {file_count} ({breakdown})")
    print(f"Total JSON objects: {total_objects}")
    print(f"user_info.user_tld present: {present_count}")
    print(f"user_info.user_tld UNDETERMINED: {undetermined_count}")
    print(f"user_info.user_tld missing, empty, or null: {absent_count}")


# --- user_info.user_tld: FILENAME ----------------------------------------------------
#
# Same present/UNDETERMINED/absent classification as COUNT, but instead of counting,
# reports the first file encountered under each node directory that contains at least
# one record in that category. A single file can satisfy more than one category at
# once (checked per-file, not stopped after the first category found), and scanning a
# node's files stops early once all three categories have been found for that node.
def _classify_user_tld(tld):
    if tld is None or tld == '':
        return 'absent'
    elif tld == 'UNDETERMINED':
        return 'UNDETERMINED'
    else:
        return 'present'


def user_info_user_tld_filename():
    categories = ['present', 'UNDETERMINED', 'absent']
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_dirs = sorted(d.name for d in root.iterdir() if d.is_dir())

    for node in node_dirs:
        found = {}  # category -> filename
        node_files = sorted((root / node).glob("*.json"))

        for json_file in node_files:
            remaining = [c for c in categories if c not in found]
            if not remaining:
                break  # all three already found for this node -- no need to check more files

            with open(json_file, 'r') as f:
                records = json.load(f)
            if not isinstance(records, list):
                continue

            categories_in_this_file = set()
            for record in records:
                user_info = record.get('user_info') if isinstance(record, dict) else None
                tld = user_info.get('user_tld') if isinstance(user_info, dict) else None
                categories_in_this_file.add(_classify_user_tld(tld))
                if len(categories_in_this_file) == len(categories):
                    break  # this file already covers all three, no need to scan more of its records

            for c in remaining:
                if c in categories_in_this_file:
                    found[c] = str(json_file)

        print(f"Node: {node} ({len(node_files)} files found)")
        for c in categories:
            print(f"  {c}: {found.get(c, '(none found)')}")


# --- user_info.user_tld: shared value-counting for ROLLUP and VALUES ----------------
#
# All four absent reasons (missing user_info, missing key, null, empty string) collapse
# into one special label here, same as COUNT's single 'absent' bucket -- this is what
# keeps ROLLUP/VALUES's totals matching COUNT's, per Karl's explicit requirement, rather
# than splitting null vs "" vs missing-key into three separate rollup rows.
ABSENT_LABEL = "(missing/empty/null)"


def _tld_value_counts():
    value_counts = {}
    for json_file in all_json_files():
        with open(json_file, 'r') as f:
            records = json.load(f)
        if not isinstance(records, list):
            continue
        for record in records:
            user_info = record.get('user_info') if isinstance(record, dict) else None
            tld = user_info.get('user_tld') if isinstance(user_info, dict) else None
            category = _classify_user_tld(tld)
            key = ABSENT_LABEL if category == 'absent' else tld  # UNDETERMINED is its own literal value already
            value_counts[key] = value_counts.get(key, 0) + 1
    return value_counts


# --- user_info.user_tld: ROLLUP and VALUES -------------------------------------------
#
# ROLLUP: each distinct value with its count, descending by count (ties broken by
# value, case-insensitive, for deterministic output). Tab-separated, so it pipes
# cleanly into sort/awk/cut if needed.
def user_info_user_tld_rollup():
    value_counts = _tld_value_counts()
    for value, count in sorted(value_counts.items(), key=lambda item: (-item[1], item[0].lower())):
        print(f"{value}\t{count}")


# VALUES: just the distinct values, descending alphabetically (case-insensitive), no counts.
def user_info_user_tld_values():
    value_counts = _tld_value_counts()
    for value in sorted(value_counts.keys(), key=str.lower, reverse=True):
        print(value)


# --- Dispatch table -------------------------------------------------------------------
DISPATCH = {
    ("user_info.user_tld", "COUNT"): user_info_user_tld_count
    , ("user_info.user_tld", "ROLLUP"): user_info_user_tld_rollup
    , ("user_info.user_tld", "VALUES"): user_info_user_tld_values
    , ("user_info.user_tld", "FILENAME"): user_info_user_tld_filename
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--command", required=True, help="One of: " + ", ".join(RECOGNIZED_COMMANDS))
    parser.add_argument("--element", required=False, default=None, help="One of: " + ", ".join(RECOGNIZED_ELEMENTS))
    args = parser.parse_args()

    if args.command not in RECOGNIZED_COMMANDS:
        print(f"Unrecognized command: {args.command!r}")
        print("Recognized commands: " + ", ".join(RECOGNIZED_COMMANDS))
        sys.exit(2)

    if args.element is None:
        print("Missing required --element argument.")
        print("Recognized elements: " + ", ".join(RECOGNIZED_ELEMENTS))
        sys.exit(2)

    if args.element not in RECOGNIZED_ELEMENTS:
        print(f"Unrecognized element: {args.element!r}")
        print("Recognized elements: " + ", ".join(RECOGNIZED_ELEMENTS))
        sys.exit(2)

    DISPATCH[(args.element, args.command)]()
