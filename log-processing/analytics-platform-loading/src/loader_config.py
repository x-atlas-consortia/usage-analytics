"""
Config discovery and loading for the analytics-platform-loading DuckDB
loader. Kept free of the duckdb dependency (like sentinel_logic.py) so it
can be exercised without a DuckDB install.

Per Karl's call, there is no --process-dir CLI argument. Config file
discovery instead relies on a consistent working-directory convention:
whatever invokes this script (.sh wrapper or PyCharm run config) sets cwd
to this project's own src/ directory -- matching the two locations Karl
demonstrated directly from that same cwd:
    $ cat duckdb_loader.ini                      (own ini: bare filename)
    $ cat ../../src/logProcessingProject.ini      (shared ini: fixed relative path)
"This process's own directory" (needed to locate exec_info/) is no longer
passed in either -- it's DERIVED after config is loaded, from the shared
ini's PROJECT_HIVE_DIR plus this project's own PROC_NAME. FLAGGED: this is
my read of "it will come from the ini file," not something spelled out
step by step -- confirm before relying on it.

FLAGGED, NOT ASSUMED: whether log_extract_xfer_utils.py's __init__ has
been updated to expose DUCKDB_PATH / PIPELINE_OUTPUT_DIR /
NODE_LOG_DIR_LIST via get_config(). Karl has confirmed JSON_FILE_NIGHTLY_DIR
stays in the shared ini for other portfolio processes (this loader just
doesn't use it), and that get_config() will expose the three new keys
directly -- so load_shared_config() below now trusts get_config() for all
of it, no separate configparser pass. If that update turns out not to be
in place yet when this actually runs, get_config()[...] will raise a
plain KeyError naming the missing key, which is a clear enough signal to
come back here rather than something to guess around in advance.
"""

from __future__ import annotations

import ast
import configparser
from pathlib import Path

from log_extract_xfer_utils import LogExtractXferUtils

# cwd is assumed to be this project's own src/ directory in every context
# (Docker, vm001-deployed, PyCharm dev) -- see module docstring.
OWN_CONFIG_CANDIDATES = [
    Path("duckdb_loader.ini"),
    Path("../../analytics-platform-loading/src/duckdb_loader.ini"),  # cwd elsewhere, e.g. repo root
]

SHARED_CONFIG_CANDIDATES = [
    Path("../../src/logProcessingProject.ini"),  # cwd = this project's own src/ (the normal case)
    Path("logProcessingProject.ini"),             # cwd = shared src/ directly (uncommon for this loader)
]

_PROCESS_SPECIFIC_SECTION = "ProcessSpecificSettings"


def find_config_file(candidates: list[Path], description: str) -> Path:
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise FileNotFoundError(
        f"Unable to find {description} in any expected location: "
        f"{[str(c) for c in candidates]}"
    )


def load_own_config() -> dict:
    """
    Reads duckdb_loader.ini's [ProcessSpecificSettings] -- now identical in
    shape to every other process's own ini (e.g.
    geolocation_details_updater.ini), per Karl's call to align conventions.
    Slack keys are read now (even though nothing consumes them yet) so this
    stays in sync with the real file rather than silently ignoring part of it.
    """
    config_path = find_config_file(OWN_CONFIG_CANDIDATES, "duckdb_loader.ini")
    config = configparser.ConfigParser()
    config.read(config_path)
    s = _PROCESS_SPECIFIC_SECTION
    return {
        "PROC_NAME": config.get(s, "PROC_NAME"),
        "SLACK_NOTIFICATION_CHANNEL": config.get(s, "SLACK_NOTIFICATION_CHANNEL"),
        "SLACK_BAD_NEWS_EMOJI": config.get(s, "SLACK_BAD_NEWS_EMOJI"),
        "SLACK_GOOD_NEWS_EMOJI": config.get(s, "SLACK_GOOD_NEWS_EMOJI"),
        "SLACK_NEUTRAL_INFO_EMOJI": config.get(s, "SLACK_NEUTRAL_INFO_EMOJI"),
        "SLACK_NOTIFICATIONS": config.get(s, "SLACK_NOTIFICATIONS"),
        "SLACK_USER_ID_MENTIONS_ON_ERROR": ast.literal_eval(
            config.get(s, "SLACK_USER_ID_MENTIONS_ON_ERROR")
        ),
        "SLACK_USER_ID_MENTIONS_ON_SUCCESS": ast.literal_eval(
            config.get(s, "SLACK_USER_ID_MENTIONS_ON_SUCCESS")
        ),
    }


def load_shared_config() -> dict:
    """
    Reads the shared logProcessingProject.ini via LogExtractXferUtils,
    unmodified -- trusting get_config() to expose DUCKDB_PATH,
    PIPELINE_OUTPUT_DIR, and NODE_LOG_DIR_LIST per Karl's confirmation.
    NODE_LOG_DIR_LIST is normalized to an actual list here regardless of
    whether get_config() hands back the raw ini string or an
    already-evaluated list, since which of those __init__ does wasn't
    specified.
    """
    config_path = find_config_file(SHARED_CONFIG_CANDIDATES, "logProcessingProject.ini")
    shared_utils = LogExtractXferUtils(config_file_name=str(config_path))
    result = dict(shared_utils.get_config())
    if isinstance(result.get("NODE_LOG_DIR_LIST"), str):
        result["NODE_LOG_DIR_LIST"] = ast.literal_eval(result["NODE_LOG_DIR_LIST"])
    return result


def find_exec_info_dir(project_hive_dir: str, proc_name: str) -> Path:
    """
    "This process's own directory" is derived, not passed in: the shared
    ini's PROJECT_HIVE_DIR (portfolio root) plus this project's own
    PROC_NAME, per "it will come from the ini file."
    """
    candidates = [
        Path(f"{project_hive_dir}/{proc_name}/exec_info"),  # derived deployed path
        Path("../exec_info"),                                # cwd = this project's own src/
        Path("exec_info"),                                   # cwd = this project's own dir directly
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate.resolve()
    raise FileNotFoundError(
        f"Unable to find exec_info directory in any expected location: "
        f"{[str(c) for c in candidates]}"
    )
