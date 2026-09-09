"""
Pure sentinel-parsing and validation logic for the globus-downloads-to-JSON
DuckDB loader. Deliberately kept free of any DuckDB or I/O-heavy dependency
so it can be unit-tested on its own -- the trickiest parts of this loader
(duplicate detection, sequential-extension validation) live here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

PREFIXES = ("gridftp.log-", "globus_access_log-")

# Matches e.g. "gridftp.log-20260905.json.LOADED.1.2.3" or "...json.DONE.1"
# -- confirmed against geolocation_details_updater.py's own
# STAGE2_DONE_PATTERNS/STAGE2_LOADED_PATTERNS, which embed ".json" between
# the date and the state word. Generalized vs. the literal ".1.[\.2-9]+"
# shorthand discussed: any starting phase number, any digit width, any
# number of dot-separated segments -- the sequential-extension check below
# is what actually enforces the "starts at 1, no gaps" contract, not the
# regex.
_SENTINEL_RE_TEMPLATE = (
    r"^{prefix}(?P<date>\d{{8}})\.json\.(?P<state>LOADED|DONE)\.(?P<phases>\d+(?:\.\d+)*)$"
)


@dataclass(frozen=True)
class SentinelMatch:
    node: str
    prefix: str
    date: str  # YYYYMMDD
    state: str  # "LOADED" or "DONE"
    phases: tuple[int, ...]
    filename: str  # full sentinel filename, as stored in the ledger

    @property
    def data_filename(self) -> str:
        return f"{self.prefix}{self.date}.json"

    @property
    def logical_key(self) -> tuple[str, str, str]:
        return (self.node, self.prefix, self.date)


def compile_patterns() -> dict[str, re.Pattern]:
    return {
        prefix: re.compile(_SENTINEL_RE_TEMPLATE.format(prefix=re.escape(prefix)))
        for prefix in PREFIXES
    }


def scan_node_directory(node: str, directory: Path) -> list[SentinelMatch]:
    """Return every sentinel file found directly in `directory` for this node."""
    matches: list[SentinelMatch] = []
    patterns = compile_patterns()
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            continue
        for prefix, pattern in patterns.items():
            m = pattern.match(entry.name)
            if m:
                phases = tuple(int(p) for p in m.group("phases").split("."))
                matches.append(
                    SentinelMatch(
                        node=node,
                        prefix=prefix,
                        date=m.group("date"),
                        state=m.group("state"),
                        phases=phases,
                        filename=entry.name,
                    )
                )
                break
    return matches


def validate_no_duplicates(all_matches: list[SentinelMatch]) -> list[str]:
    """
    Within each node directory, each (prefix, date, state) must match at most
    one sentinel file. Returns error messages; an empty list means clean.
    Scoped per node, per the confirmed decision -- dtn03 and dtn02 both
    having a same-date sentinel is normal and not a duplicate.
    """
    errors: list[str] = []
    seen: dict[tuple[str, str, str, str], list[str]] = {}
    for sm in all_matches:
        key = (sm.node, sm.prefix, sm.date, sm.state)
        seen.setdefault(key, []).append(sm.filename)

    for (node, prefix, date, state), filenames in sorted(seen.items()):
        if len(filenames) > 1:
            errors.append(
                f"Duplicate {state} sentinel match in node={node} prefix={prefix} "
                f"date={date}: {filenames}"
            )
    return errors


def is_sequential_extension(prior: tuple[int, ...], current: tuple[int, ...]) -> bool:
    """
    True if `current` equals `prior` with one or more additional trailing
    phase numbers appended, and those additional numbers form an unbroken
    run continuing from prior's last number.

    (1, 2, 3) -> (1, 2, 3, 4)     True   (single new phase)
    (1, 2, 3) -> (1, 2, 3, 4, 5)  True   (loader missed a run; still fine)
    (1, 2, 3) -> (1, 2, 3, 5)     False  (gap -- skipped phase 4)
    (1, 2, 3) -> (1, 2, 4)        False  (diverges, not an extension)
    (1, 2, 3) -> (1, 2)           False  (shorter -- regression)
    (1, 2, 3) -> (1, 2, 3)        False  (unchanged -- caller should treat
                                          this as a no-op and never call
                                          this function for that case)
    """
    if len(current) <= len(prior) or current[: len(prior)] != prior:
        return False
    appended = current[len(prior) :]
    expected_start = prior[-1] + 1
    return list(appended) == list(range(expected_start, expected_start + len(appended)))
