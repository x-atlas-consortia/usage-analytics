#!/usr/bin/env python3
"""
Audit the actual content of pipeline JSON output files, as opposed to
audit_stage_completeness.py's structural/marker checks.

Usage:
  python3 audit_json_content.py --command=COUNT --element=user_info.user_tld
  python3 audit_json_content.py --command=FILES --element=user_info.user

Recognized --command values: COUNT, ROLLUP, VALUES, FILENAME, FILES
Recognized --element values: user_info.user_tld, user_info.user, globus_task_id,
                              geolocation_info

For a given element, there are up to five dedicated methods -- one per command --
named <element_with_dots_as_underscores>_<command, lowercase>. The dispatch table
below maps (element, command) pairs to those functions explicitly, rather than
constructing names dynamically, so a typo in a function name fails loudly at
definition time instead of silently missing at dispatch time.

FILES answers a different question than COUNT: COUNT tallies *records* in each
category; FILES tallies how many distinct *files* contain at least one record in
that category. A single file with 50,000 PENDING records and one with a single
PENDING record both count as "1" toward PENDING under FILES -- useful for "how
widespread is this" rather than "how much of this is there."

Placeholder-value vocabulary actually used by the pipeline (there is no literal
'TBD' anywhere in output JSON -- that word only appears in comments/doc-strings
as shorthand for the general concept):
  user_info.user       -- PENDING (gridftp only, expected to resolve on a later
                           run once the usage CSV catches up -- not a current
                           gap), UNRESOLVED (gridftp only, CSV already covers this
                           date and still no match -- a real settled gap),
                           UNTRACKED (http only, structurally never resolvable)
  user_info.user_domain -- UNDETERMINED (mechanically follows from the three
                           user-field placeholders above, not independent),
                           ERROR (3+ '@' signs -- a genuinely malformed value)
  globus_task_id        -- NOT_FOUND (raw TASKID was 'none' -- normal for the
                           internal HTTP-to-GridFTP loopback case)
  geolocation_info.*    -- UNKNOWN (IP not covered by the geo DB), INVALID
                           (missing/malformed IP), MULTIPLE (2+ overlapping DB
                           ranges matched -- ambiguous)
"""
import argparse
import json
import sys
from pathlib import Path

JSON_FILE_NIGHTLY_DIR = "/hive/hubmap/pitt-analytics/globus-downloads-to-JSON"

RECOGNIZED_COMMANDS = ["COUNT", "ROLLUP", "VALUES", "FILENAME", "FILES"]
RECOGNIZED_ELEMENTS = ["user_info.user_tld", "user_info.user", "globus_task_id", "geolocation_info"]


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


# --- Generic FILES support (per-file category counts) -------------------------------
#
# FILES answers "how many distinct files contain at least one record in category X",
# as opposed to COUNT's "how many records total are in category X". Shared across every
# element via a single generic() function -- unlike the per-element COUNT/ROLLUP/VALUES/
# FILENAME functions above, the FILES logic genuinely doesn't vary by element once given
# a classify function and a value-getter, so duplicating it four times would just be four
# copies of the same loop. The dispatch table still binds one explicitly-named function
# per (element, "FILES") pair, same as everything else -- only the *body* is shared.
def _files_generic(classify_fn, get_value_fn):
    root = Path(JSON_FILE_NIGHTLY_DIR)
    per_category_file_counts = {}

    for json_file in all_json_files():
        with open(json_file, 'r') as f:
            records = json.load(f)
        if not isinstance(records, list):
            continue

        categories_in_this_file = set()
        for record in records:
            value = get_value_fn(record) if isinstance(record, dict) else None
            categories_in_this_file.add(classify_fn(value))

        for category in categories_in_this_file:
            per_category_file_counts[category] = per_category_file_counts.get(category, 0) + 1

    for category, count in sorted(per_category_file_counts.items(), key=lambda item: (-item[1], item[0].lower())):
        print(f"{category}\t{count}")


def _get_user_tld(record):
    user_info = record.get('user_info') if isinstance(record, dict) else None
    return user_info.get('user_tld') if isinstance(user_info, dict) else None


def user_info_user_tld_files():
    _files_generic(_classify_user_tld, _get_user_tld)


# --- user_info.user: PENDING / UNRESOLVED / UNTRACKED / present / absent ------------
#
# Unlike user_tld, these three placeholders are NOT interchangeable "something went
# wrong" markers -- see the module docstring. Keeping them as distinct categories here
# (rather than collapsing to one 'placeholder' bucket) is the whole point: PENDING on
# its own, right after a backfill, is not evidence of a problem.
def _classify_user(user):
    if user is None or user == '':
        return 'absent'
    elif user in ('PENDING', 'UNRESOLVED', 'UNTRACKED'):
        return user
    else:
        return 'present'


def _get_user(record):
    user_info = record.get('user_info') if isinstance(record, dict) else None
    return user_info.get('user') if isinstance(user_info, dict) else None


def _user_value_counts():
    value_counts = {}
    for json_file in all_json_files():
        with open(json_file, 'r') as f:
            records = json.load(f)
        if not isinstance(records, list):
            continue
        for record in records:
            user = _get_user(record)
            category = _classify_user(user)
            key = ABSENT_LABEL if category == 'absent' else (user if category == 'present' else category)
            value_counts[key] = value_counts.get(key, 0) + 1
    return value_counts


def user_info_user_count():
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_file_counts = {d.name: 0 for d in root.iterdir() if d.is_dir()}

    file_count = 0
    total_objects = 0
    counts = {'present': 0, 'PENDING': 0, 'UNRESOLVED': 0, 'UNTRACKED': 0, 'absent': 0}

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
            counts[_classify_user(_get_user(record))] += 1

    breakdown = ", ".join(f"{node}-{count}" for node, count in sorted(node_file_counts.items()))
    print(f"Files processed: {file_count} ({breakdown})")
    print(f"Total JSON objects: {total_objects}")
    print(f"user_info.user present (resolved identity): {counts['present']}")
    print(f"user_info.user PENDING (gridftp, may resolve on a future run): {counts['PENDING']}")
    print(f"user_info.user UNRESOLVED (gridftp, settled gap): {counts['UNRESOLVED']}")
    print(f"user_info.user UNTRACKED (http, structurally never resolvable): {counts['UNTRACKED']}")
    print(f"user_info.user missing, empty, or null: {counts['absent']}")


def user_info_user_rollup():
    value_counts = _user_value_counts()
    for value, count in sorted(value_counts.items(), key=lambda item: (-item[1], item[0].lower())):
        print(f"{value}\t{count}")


def user_info_user_values():
    value_counts = _user_value_counts()
    for value in sorted(value_counts.keys(), key=str.lower, reverse=True):
        print(value)


def user_info_user_filename():
    categories = ['present', 'PENDING', 'UNRESOLVED', 'UNTRACKED', 'absent']
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_dirs = sorted(d.name for d in root.iterdir() if d.is_dir())

    for node in node_dirs:
        found = {}
        node_files = sorted((root / node).glob("*.json"))

        for json_file in node_files:
            remaining = [c for c in categories if c not in found]
            if not remaining:
                break

            with open(json_file, 'r') as f:
                records = json.load(f)
            if not isinstance(records, list):
                continue

            categories_in_this_file = {_classify_user(_get_user(r)) for r in records if isinstance(r, dict)}
            for c in remaining:
                if c in categories_in_this_file:
                    found[c] = str(json_file)

        print(f"Node: {node} ({len(node_files)} files found)")
        for c in categories:
            print(f"  {c}: {found.get(c, '(none found)')}")


def user_info_user_files():
    _files_generic(_classify_user, _get_user)


# --- globus_task_id: NOT_FOUND / present / absent ------------------------------------
#
# NOT_FOUND means the raw log TASKID was 'none' -- the normal, expected value for the
# internal HTTP-to-GridFTP loopback case (see gridftp_log_extract.py's loopback_skip_
# counter). A high NOT_FOUND count is not inherently a problem; it's worth comparing
# against the loopback-skip figure already logged by Phase 1 before treating it as one.
def _classify_task_id(task_id):
    if task_id is None or task_id == '':
        return 'absent'
    elif task_id == 'NOT_FOUND':
        return 'NOT_FOUND'
    else:
        return 'present'


def _get_task_id(record):
    return record.get('globus_task_id') if isinstance(record, dict) else None


def globus_task_id_count():
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_file_counts = {d.name: 0 for d in root.iterdir() if d.is_dir()}

    file_count = 0
    total_objects = 0
    counts = {'present': 0, 'NOT_FOUND': 0, 'absent': 0}

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
            counts[_classify_task_id(_get_task_id(record))] += 1

    breakdown = ", ".join(f"{node}-{count}" for node, count in sorted(node_file_counts.items()))
    print(f"Files processed: {file_count} ({breakdown})")
    print(f"Total JSON objects: {total_objects}")
    print(f"globus_task_id present: {counts['present']}")
    print(f"globus_task_id NOT_FOUND (normal for loopback entries): {counts['NOT_FOUND']}")
    print(f"globus_task_id missing, empty, or null: {counts['absent']}")


def _task_id_value_counts():
    value_counts = {}
    for json_file in all_json_files():
        with open(json_file, 'r') as f:
            records = json.load(f)
        if not isinstance(records, list):
            continue
        for record in records:
            task_id = _get_task_id(record)
            category = _classify_task_id(task_id)
            key = ABSENT_LABEL if category == 'absent' else (task_id if category == 'present' else category)
            value_counts[key] = value_counts.get(key, 0) + 1
    return value_counts


def globus_task_id_rollup():
    value_counts = _task_id_value_counts()
    for value, count in sorted(value_counts.items(), key=lambda item: (-item[1], item[0].lower())):
        print(f"{value}\t{count}")


def globus_task_id_values():
    value_counts = _task_id_value_counts()
    for value in sorted(value_counts.keys(), key=str.lower, reverse=True):
        print(value)


def globus_task_id_filename():
    categories = ['present', 'NOT_FOUND', 'absent']
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_dirs = sorted(d.name for d in root.iterdir() if d.is_dir())

    for node in node_dirs:
        found = {}
        node_files = sorted((root / node).glob("*.json"))

        for json_file in node_files:
            remaining = [c for c in categories if c not in found]
            if not remaining:
                break

            with open(json_file, 'r') as f:
                records = json.load(f)
            if not isinstance(records, list):
                continue

            categories_in_this_file = {_classify_task_id(_get_task_id(r)) for r in records if isinstance(r, dict)}
            for c in remaining:
                if c in categories_in_this_file:
                    found[c] = str(json_file)

        print(f"Node: {node} ({len(node_files)} files found)")
        for c in categories:
            print(f"  {c}: {found.get(c, '(none found)')}")


def globus_task_id_files():
    _files_generic(_classify_task_id, _get_task_id)


# --- geolocation_info: UNKNOWN / INVALID / MULTIPLE / present / absent --------------
#
# All five geolocation_info sub-fields are always set together as one unit (see
# geolocation_details_updater.py / ip2geo.py), so country_code alone is a faithful
# proxy for the whole block's status -- no need to check all five independently.
# 'absent' here means Phase 3 (geolocation_details_updater.py) hasn't reached this
# record yet, not that anything failed.
def _classify_geo(country_code):
    if country_code is None or country_code == '':
        return 'absent'
    elif country_code in ('UNKNOWN', 'INVALID', 'MULTIPLE'):
        return country_code
    else:
        return 'present'


def _get_geo_country_code(record):
    geo = record.get('geolocation_info') if isinstance(record, dict) else None
    return geo.get('country_code') if isinstance(geo, dict) else None


def geolocation_info_count():
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_file_counts = {d.name: 0 for d in root.iterdir() if d.is_dir()}

    file_count = 0
    total_objects = 0
    counts = {'present': 0, 'UNKNOWN': 0, 'INVALID': 0, 'MULTIPLE': 0, 'absent': 0}

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
            counts[_classify_geo(_get_geo_country_code(record))] += 1

    breakdown = ", ".join(f"{node}-{count}" for node, count in sorted(node_file_counts.items()))
    print(f"Files processed: {file_count} ({breakdown})")
    print(f"Total JSON objects: {total_objects}")
    print(f"geolocation_info present (resolved): {counts['present']}")
    print(f"geolocation_info UNKNOWN (IP not in geo DB): {counts['UNKNOWN']}")
    print(f"geolocation_info INVALID (missing/malformed IP): {counts['INVALID']}")
    print(f"geolocation_info MULTIPLE (ambiguous DB match): {counts['MULTIPLE']}")
    print(f"geolocation_info missing (Phase 3 hasn't reached this record yet): {counts['absent']}")


def _geo_value_counts():
    value_counts = {}
    for json_file in all_json_files():
        with open(json_file, 'r') as f:
            records = json.load(f)
        if not isinstance(records, list):
            continue
        for record in records:
            cc = _get_geo_country_code(record)
            category = _classify_geo(cc)
            key = ABSENT_LABEL if category == 'absent' else (cc if category == 'present' else category)
            value_counts[key] = value_counts.get(key, 0) + 1
    return value_counts


def geolocation_info_rollup():
    value_counts = _geo_value_counts()
    for value, count in sorted(value_counts.items(), key=lambda item: (-item[1], item[0].lower())):
        print(f"{value}\t{count}")


def geolocation_info_values():
    value_counts = _geo_value_counts()
    for value in sorted(value_counts.keys(), key=str.lower, reverse=True):
        print(value)


def geolocation_info_filename():
    categories = ['present', 'UNKNOWN', 'INVALID', 'MULTIPLE', 'absent']
    root = Path(JSON_FILE_NIGHTLY_DIR)
    node_dirs = sorted(d.name for d in root.iterdir() if d.is_dir())

    for node in node_dirs:
        found = {}
        node_files = sorted((root / node).glob("*.json"))

        for json_file in node_files:
            remaining = [c for c in categories if c not in found]
            if not remaining:
                break

            with open(json_file, 'r') as f:
                records = json.load(f)
            if not isinstance(records, list):
                continue

            categories_in_this_file = {_classify_geo(_get_geo_country_code(r)) for r in records if isinstance(r, dict)}
            for c in remaining:
                if c in categories_in_this_file:
                    found[c] = str(json_file)

        print(f"Node: {node} ({len(node_files)} files found)")
        for c in categories:
            print(f"  {c}: {found.get(c, '(none found)')}")


def geolocation_info_files():
    _files_generic(_classify_geo, _get_geo_country_code)


# --- Dispatch table -------------------------------------------------------------------
DISPATCH = {
    ("user_info.user_tld", "COUNT"): user_info_user_tld_count
    , ("user_info.user_tld", "ROLLUP"): user_info_user_tld_rollup
    , ("user_info.user_tld", "VALUES"): user_info_user_tld_values
    , ("user_info.user_tld", "FILENAME"): user_info_user_tld_filename
    , ("user_info.user_tld", "FILES"): user_info_user_tld_files

    , ("user_info.user", "COUNT"): user_info_user_count
    , ("user_info.user", "ROLLUP"): user_info_user_rollup
    , ("user_info.user", "VALUES"): user_info_user_values
    , ("user_info.user", "FILENAME"): user_info_user_filename
    , ("user_info.user", "FILES"): user_info_user_files

    , ("globus_task_id", "COUNT"): globus_task_id_count
    , ("globus_task_id", "ROLLUP"): globus_task_id_rollup
    , ("globus_task_id", "VALUES"): globus_task_id_values
    , ("globus_task_id", "FILENAME"): globus_task_id_filename
    , ("globus_task_id", "FILES"): globus_task_id_files

    , ("geolocation_info", "COUNT"): geolocation_info_count
    , ("geolocation_info", "ROLLUP"): geolocation_info_rollup
    , ("geolocation_info", "VALUES"): geolocation_info_values
    , ("geolocation_info", "FILENAME"): geolocation_info_filename
    , ("geolocation_info", "FILES"): geolocation_info_files
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
