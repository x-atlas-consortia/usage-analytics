#!/usr/bin/env python3
"""
Audit stage-N completeness for both log types, for use between pipeline phases.

For every log file, checks the actual highest stage reached (by finding the
highest valid .DONE.1, .DONE.1.2, .DONE.1.2.3, ... sibling) against the requested
stage N:
  - behind N (json exists, marker for N doesn't) -- listed individually
  - exactly N+1 -- listed individually; this is the specific "did something
    already start the next phase early" case
  - N+2 or later -- summary counts only (e.g. "3 files at stage 4, 1 at stage 5"),
    no per-file paths; not the primary concern of this check
  - whichever marker actually exists is checked for read-only state (no write
    bit set for anyone), regardless of which tier it falls into

Read-only itself. Makes no changes.

Usage:
  python3 audit_stage_completeness.py 1
  python3 audit_stage_completeness.py 2 --data-root /hive/hubmap/data/globus-logs
"""
import argparse
import stat
import sys
from pathlib import Path

DEFAULT_DATA_ROOT = "/hive/hubmap/data/globus-logs-smoketest-july2026"
DEFAULT_OUT_ROOT = "/hive/hubmap/pitt-analytics/globus-downloads-to-JSON"


def marker_suffix(stage: int) -> str:
    # stage=1 -> "DONE.1", stage=2 -> "DONE.1.2", stage=3 -> "DONE.1.2.3", etc.
    return "DONE." + ".".join(str(i) for i in range(1, stage + 1))


def find_current_stage(out_json: Path) -> int:
    """Return the highest stage number for which a valid marker exists, or 0 if none.
    Validates that the suffix is a genuine 1, 1.2, 1.2.3, ... sequence rather than
    trusting any arbitrary '.DONE.*' sibling."""
    highest = 0
    for candidate in out_json.parent.glob(out_json.name + ".DONE.*"):
        suffix = candidate.name.split(".DONE.", 1)[-1]
        parts = suffix.split(".")
        if parts == [str(i) for i in range(1, len(parts) + 1)]:
            highest = max(highest, len(parts))
    return highest


def is_read_only(path: Path) -> bool:
    mode = stat.S_IMODE(path.stat().st_mode)
    return (mode & 0o222) == 0  # no write bit set for owner, group, or other


def check(log_files, strip_gz: bool, label: str, out_root: Path, stage: int, summary_only: bool = False):
    total = 0
    missing_json = []
    behind = []  # (path, actual_stage) -- json exists but hasn't reached the requested stage
    at_expected_stage = 0
    at_next_stage = []  # paths sitting exactly one stage ahead of what's requested
    beyond_next_by_level = {}  # {actual_stage: count} for anything more than one stage ahead
    not_read_only = []  # (path, octal_mode)

    for log_file in log_files:
        total += 1
        node = log_file.parent.parent.name  # .../<node>/gridftp-log/<file> or .../<node>/httpd/<file>
        base = log_file.name[:-3] if strip_gz else log_file.name  # strip '.gz' for gridftp only

        out_json = out_root / node / f"{base}.json"

        if not out_json.is_file():
            missing_json.append(str(log_file))
            continue

        actual_stage = find_current_stage(out_json)

        if actual_stage < stage:
            behind.append((str(out_json), actual_stage))
        elif actual_stage == stage:
            at_expected_stage += 1
            current_marker = Path(f"{out_json}.{marker_suffix(stage)}")
            if not is_read_only(current_marker):
                mode = stat.S_IMODE(current_marker.stat().st_mode)
                not_read_only.append((str(current_marker), oct(mode)))
        elif actual_stage == stage + 1:
            at_next_stage.append(str(out_json))
        else:  # actual_stage > stage + 1
            beyond_next_by_level[actual_stage] = beyond_next_by_level.get(actual_stage, 0) + 1

    if not summary_only:
        print(f"=== {label}: {total} log files checked (expecting stage {stage}) ===")
        if missing_json:
            print(f"-- MISSING .json output ({len(missing_json)}):")
            for f in missing_json:
                print(f"   {f}")
        if behind:
            print(f"-- BEHIND expected stage {stage}, json exists ({len(behind)}):")
            for f, actual in behind:
                print(f"   {f}  (actually at stage {actual})")
        bang = "!" if at_expected_stage == 0 else ""
        print(f"-- AT EXPECTED stage {stage} ({at_expected_stage}){bang}")
        if at_next_stage:
            print(f"-- ALREADY AT stage {stage + 1} ({len(at_next_stage)}):")
            for f in at_next_stage:
                print(f"   {f}")
        if beyond_next_by_level:
            parts = ", ".join(f"{count} from stage {lvl}" for lvl, count in sorted(beyond_next_by_level.items()))
            total_beyond = sum(beyond_next_by_level.values())
            print(f"-- Warning: {total_beyond} files are at stage {stage + 2} or later ({parts})")
        if not_read_only:
            print(f"-- SENTINEL FILE FOR stage {stage} NOT READ-ONLY ({len(not_read_only)}):")
            for f, mode in not_read_only:
                print(f"   {f}  (mode {mode})")
        print()
    return total, len(missing_json), len(behind), at_expected_stage, len(at_next_stage), sum(beyond_next_by_level.values()), len(not_read_only)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stage", type=int, help="Stage number to confirm (1, 2, 3, ...)")
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT,
                         help=f"Log file source root (default: {DEFAULT_DATA_ROOT})")
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT,
                         help=f"Pipeline output root (default: {DEFAULT_OUT_ROOT})")
    parser.add_argument("--summary", action="store_true",
                         help="Print only the final two summary lines, suppressing all per-file detail")
    args = parser.parse_args()

    if args.stage < 1:
        print("Stage must be 1 or greater.")
        sys.exit(2)

    data_root = Path(args.data_root)
    out_root = Path(args.out_root)

    gridftp_files = sorted(data_root.glob("*/gridftp-log/gridftp.log-*.gz"))
    http_files = sorted(data_root.glob("*/httpd/globus_access_log-*"))

    g = check(gridftp_files, strip_gz=True, label="GridFTP", out_root=out_root, stage=args.stage, summary_only=args.summary)
    h = check(http_files, strip_gz=False, label="HTTP access log", out_root=out_root, stage=args.stage, summary_only=args.summary)

    if not args.summary:
        print("=== Summary ===")
    print(f"GridFTP:  {g[0]} log files, {g[1]} missing json. Sentinel files - {g[2]} behind, {g[3]} at expected stage ({g[6]} not read-only), {g[4]} at stage {args.stage + 1}, {g[5]} at stage {args.stage + 2}+")
    print(f"HTTP:     {h[0]} log files, {h[1]} missing json. Sentinel files - {h[2]} behind, {h[3]} at expected stage ({h[6]} not read-only), {h[4]} at stage {args.stage + 1}, {h[5]} at stage {args.stage + 2}+")
