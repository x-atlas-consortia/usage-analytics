#!/usr/bin/env python3
"""
Phase 1 of analytics-platform-loading: load the globus-downloads-to-JSON output, derived
from Globus Grid FTP and HTTP logs for the nodes dtn02, dtn03, and app001
into the file_downloads and hot_file_downloads table of the shared usage-analytics DuckDB file.

Phase 2, for API Usage, loads into the same DuckDB file.  Since DuckDB only
allows one writing process at a time, each loader takes or waits for an exclusive lock on
the <database>.lock file before opening the database (see acquire_db_write_lock).

Files are loaded in date order (source date, then node, then prefix), not in
the order the directory scan returns them.  DuckDB keeps min-max statistics
for every block of rows and skips the blocks a date filter cannot match, and
it can only skip many blocks when rows are stored in date order.  A build
from scratch therefore comes out in date order, and each night's new days are
appended in order after it.  A file that is reloaded (its sentinel advanced)
or that arrives late for an old date is still appended at the end, so the
table drifts out of order slowly, and an occasional full rebuild restores it.

The ETL of File Downloads logged file transfers to DuckDB is assumed
to be a nightly process, even if delivery of data means there are
many nights with little or no change (e.g. waiting for a new
"usage details" spreadsheet from Globus most of the month.)

The File Downloads data is split into two tables, which can be
characterized as partial/complete, volatile/settled, hot/frozen, etc.
The tables are hot_file_download and file_download, each with the
same structure.  There is no data movement within DuckDB. All data is
managed by this "load" step of the ETL pipeline.

Since the tables within DuckDB are performing adequately after the initial
build with 72M rows, use of Parquet is not currently implemented.

Sentinel files should exist along with each JSON file with file transfer
content.  Comments in the globus-downloads-to-JSON pipeline should
describe the various states of sentinel files.  This loader's interpretation
of sentinal files is as follows.

1. On each run, if there are any duplicate sentinel files, log each one
   and exit.
2. The content associated with LOADED sentinel files is used to create
   the hot_file_download table.  This should reflect logged file transfers
   for which more information may eventually be received.  The table is
   simply dropped and created from LOADED-associated content each time.
   This is a quick operation because only the current and maybe previous
   month have partial information.  All other file transfers have as much
   information as they will ever have.
3. The content associated with DONE sentinel files is used to create
   the file_download table. A ledger kept in file_download_ledger tracking
   the sentinel files seen during previous loads.
   3.1 If the current sentinel file matches the sentinel file of the last
       load, the content is skipped.
   3.2 If the sentinel file suffix reflects a well-formed pattern indicating
       additional globus-downloads-to-JSON pipeline "phases" have been run
       on the content, the content is reloaded.
   3.3 Other changes to the sentinel file log an error, and the content is
       skipped.

The content JSON files may contain information not loaded into the DuckDB
tables, notably the `provenance` field.
"""

from __future__ import annotations

import argparse
import ast
import configparser
import fcntl
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from log_extract_xfer_utils import LogExtractXferUtils
from sentinel_logic import (
    SentinelMatch,
    is_sequential_extension,
    scan_node_directory,
    validate_no_duplicates,
)

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('Loading globus-downloads-to-JSON output into DuckDB')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase1' -> ':one:'), so relocating a script to a different phase directory
# automatically updates its emoji with no code change. Falls back to ':diamonds:'
# if the parent directory doesn't match the 'phaseN' pattern.
_PHASE_NUMBER_WORDS = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven', 8: 'eight', 9: 'nine'}
_phase_dir_match = re.match(r'^phase(\d+)$', Path(__file__).resolve().parent.name)
if _phase_dir_match and int(_phase_dir_match.group(1)) in _PHASE_NUMBER_WORDS:
    SLACK_PHASE_EMOJI = f":{_PHASE_NUMBER_WORDS[int(_phase_dir_match.group(1))]}:"
else:
    SLACK_PHASE_EMOJI = ':diamonds:'

arg_parser = argparse.ArgumentParser(
    description='Load globus-downloads-to-JSON output into DuckDB.'
)
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                         help="This project's own directory (one level above src/), e.g."
                              " .../log-processing/analytics-platform-loading. Normally supplied"
                              " by the .sh wrapper's own PROJECT_DIR.")
arg_parser.add_argument(
    '--source-root', type=Path, default=None,
    help="Override PIPELINE_OUTPUT_DIR. Mainly for pointing at synthetic "
         "fixtures during testing.",
)
arg_parser.add_argument(
    '--duckdb-path', type=Path, default=None,
    help="Override DUCKDB_PATH from the shared config.",
)
args = arg_parser.parse_args()
arg_project_dir = args.project_dir
arg_portfolio_dir = os.path.dirname(arg_project_dir)

Config = configparser.ConfigParser()

process_ini_candidates = [
    Path('duckdb_loader.ini'),
    Path(f'{arg_project_dir}/src/duckdb_loader.ini'),
    Path('../duckdb_loader.ini'),
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find duckdb_loader.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)
try:
    PROJECT_NAME = Config.get('ProcessSpecificSettings', 'PROJECT_NAME')
    SLACK_NOTIFICATION_CHANNEL = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATION_CHANNEL')
    SLACK_BAD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_NEWS_EMOJI')
    SLACK_GOOD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_NEWS_EMOJI')
    SLACK_NEUTRAL_INFO_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_INFO_EMOJI')
    SLACK_NOTIFICATIONS = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATIONS')
    slack_user_id_mentions_on_error_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR'))
    slack_user_id_mentions_on_success_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS'))
except Exception as e:
    print(f"\a\nUnable to read configuration from '{config_file_name}'.\n")
    sys.exit(3)
print('Process-specific configuration loaded')

exec_info_dir_candidates = [
    Path('exec_info'),
    Path(f'{arg_project_dir}/exec_info'),
    Path('../../exec_info'),
]
exec_info_dir = None
for candidate in exec_info_dir_candidates:
    if candidate.is_dir():
        exec_info_dir = str(candidate.resolve())
        break
if not exec_info_dir:
    print(f'Unable to find exec_info directory in any expected location.')
    sys.exit(3)
log_file_name = f"{exec_info_dir}" \
                f"/duckdb_file_downloads_loader-" \
                f"{datetime.now().strftime('%Y-%m-%d_%H%M%S')}" \
                f".log"
logging.basicConfig(filename=log_file_name
                    ,level=logging.INFO
                    ,format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    ,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

print('Logger instantiated')

portfolio_utils = None
try:
    config_file_location = None
    candidates = [
        Path('logProcessingProject.ini'),
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'),
        Path('../../../src/logProcessingProject.ini'),
    ]
    for candidate in candidates:
        if candidate.is_file():
            config_file_location = candidate.resolve()
            break
    portfolio_utils = LogExtractXferUtils(config_file_name=config_file_location
                                          , disable_slack_notifications=(SLACK_NOTIFICATIONS == 'DISABLED'))
    portfolio_config = portfolio_utils.get_config()
    print('Shared log processing configuration loaded.')
    PIPELINE_OUTPUT_DIR = portfolio_config['PIPELINE_OUTPUT_DIR']
    DUCKDB_PATH = portfolio_config['DUCKDB_PATH']
    node_dir_list = ast.literal_eval(portfolio_config['NODE_LOG_DIR_LIST'])
    logger.info("LogExtractXferUtils instantiated.")
    print('LogExtractXferUtils instantiated.')
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

source_root = args.source_root if args.source_root is not None else Path(PIPELINE_OUTPUT_DIR)
duckdb_path = args.duckdb_path if args.duckdb_path is not None else Path(DUCKDB_PATH)

PROGRESS_LOG_INTERVAL = 100

# DuckDB allows only one writing process at a time, and a second writer fails at once
# with a lock error instead of waiting.  So, before opening the database, each loader
# takes an exclusive flock on "<database file>.lock" and waits (polling) for any other
# loader to finish.  The lock is released when this process exits, however it exits.
#
# N.B. This relies on the shared Lustre filesystem /hive honoring flock locks, which was
#      verified between dtn03 and vm001 on 2026-10-14 (a lock held on one host blocked
#      the other).  In practice the loaders are expected to run on a single host.
DB_LOCK_WAIT_SECONDS = 2 * 60 * 60
DB_LOCK_POLL_SECONDS = 5


def acquire_db_write_lock(db_path: Path):
    """Take the exclusive writer lock for db_path, waiting up to DB_LOCK_WAIT_SECONDS.

    Returns the open lock file, which must stay referenced until the process exits
    (closing it releases the lock), or None if the lock could not be acquired in time.
    """
    lock_path = Path(str(db_path) + ".lock")
    lock_file = open(lock_path, "a")
    deadline = time.monotonic() + DB_LOCK_WAIT_SECONDS
    announced = False
    while True:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if announced:
                logger.info("Acquired %s.", lock_path)
            return lock_file
        except BlockingIOError:
            if not announced:
                logger.info("Waiting for another process to release %s (up to %d minutes).",
                            lock_path, DB_LOCK_WAIT_SECONDS // 60)
                announced = True
            if time.monotonic() >= deadline:
                lock_file.close()
                return None
            time.sleep(DB_LOCK_POLL_SECONDS)


HOT_TABLE = "hot_file_download"
FROZEN_TABLE = "file_download"
LEDGER_TABLE = "file_download_ledger"

_TABLE_SCHEMA = """
    destination_ip VARCHAR,
    country_code VARCHAR,
    country_name VARCHAR,
    region_name VARCHAR,
    city_name VARCHAR,
    zip_code VARCHAR,
    user_name VARCHAR,
    user_domain VARCHAR,
    user_tld VARCHAR,
    entity_uuid VARCHAR,
    dataset_type VARCHAR,
    application_id VARCHAR,
    entity_type VARCHAR,
    relative_file_path VARCHAR,
    bytes_transferred BIGINT,
    download_date_time TIMESTAMP,
    download_date DATE,
    download_year INTEGER,
    download_month INTEGER,
    protocol VARCHAR,
    globus_task_id VARCHAR,
    source_node VARCHAR,
    source_prefix VARCHAR,
    source_date VARCHAR
"""

_JSON_COLUMNS_SPEC = (
    "{"
    "'destination_ip': 'VARCHAR', "
    "'geolocation_info': 'STRUCT(country_code VARCHAR, country_name VARCHAR, "
    "region_name VARCHAR, city_name VARCHAR, zip_code VARCHAR)', "
    "'user_info': 'STRUCT(\"user\" VARCHAR, user_domain VARCHAR, user_tld VARCHAR)', "
    "'entity_uuid': 'VARCHAR', "
    "'dataset_type': 'VARCHAR', "
    "'application_id': 'VARCHAR', "
    "'entity_type': 'VARCHAR', "
    "'relative_file_path': 'VARCHAR', "
    "'bytes_transferred': 'BIGINT', "
    "'download_date_time': 'VARCHAR', "
    "'protocol': 'VARCHAR', "
    "'globus_task_id': 'VARCHAR'"
    "}"
)

_INSERT_SELECT = f"""
    SELECT
        destination_ip,
        geolocation_info.country_code AS country_code,
        geolocation_info.country_name AS country_name,
        geolocation_info.region_name AS region_name,
        geolocation_info.city_name AS city_name,
        geolocation_info.zip_code AS zip_code,
        user_info."user" AS user_name,
        user_info.user_domain AS user_domain,
        user_info.user_tld AS user_tld,
        entity_uuid,
        dataset_type,
        application_id,
        entity_type,
        relative_file_path,
        bytes_transferred,
        CAST(download_date_time AS TIMESTAMP) AS download_date_time,
        CAST(download_date_time AS DATE) AS download_date,
        EXTRACT(YEAR FROM CAST(download_date_time AS TIMESTAMP)) AS download_year,
        EXTRACT(MONTH FROM CAST(download_date_time AS TIMESTAMP)) AS download_month,
        protocol,
        globus_task_id,
        ? AS source_node,
        ? AS source_prefix,
        ? AS source_date
    FROM read_json(?, columns={_JSON_COLUMNS_SPEC}, format='array', ignore_errors=true)
"""

_JSON_COLUMNS_SPEC_COUNT_ONLY = _JSON_COLUMNS_SPEC.replace(
    "'bytes_transferred': 'BIGINT'", "'bytes_transferred': 'VARCHAR'"
)


def verify_configuration_expectations():
    global node_dir_list

    exit_rather_than_return = False
    if not source_root.exists():
        msg=f"Halting program due to not finding source_root at '{source_root}' relative to '{os.getcwd()}'."
        logger.error(msg)
        exit_rather_than_return = True
    if not os.path.exists(exec_info_dir):
        msg=f"Halting program due to not finding exec_info_dir at '{exec_info_dir}' relative to '{os.getcwd()}'."
        logger.error(msg)
        exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_green_square::skull_and_crossbones: ' * 5}\n"
                    f":large_green_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE TABLE IF NOT EXISTS {HOT_TABLE} ({_TABLE_SCHEMA})")
    con.execute(f"CREATE TABLE IF NOT EXISTS {FROZEN_TABLE} ({_TABLE_SCHEMA})")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
            node VARCHAR,
            prefix VARCHAR,
            date VARCHAR,
            phases VARCHAR,
            sentinel_filename VARCHAR,
            PRIMARY KEY (node, prefix, date)
        )
        """
    )


def _load_json_into_table(
    con: duckdb.DuckDBPyConnection,
    sm: SentinelMatch,
    table: str,
    source_root: Path,
) -> bool:
    json_path = source_root / sm.node / sm.data_filename
    if not json_path.exists():
        logger.error(
            "Expected data file missing for sentinel %s: %s", sm.filename, json_path
        )
        return False
    try:
        con.execute(
            f"INSERT INTO {table} {_INSERT_SELECT}",
            [sm.node, sm.prefix, sm.date, str(json_path)],
        )
    except Exception:
        logger.exception(
            "Failed to load %s (sentinel %s) into %s -- skipping this file, continuing.",
            json_path, sm.filename, table,
        )
        return False

    try:
        inserted = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE source_node = ? "
            "AND source_prefix = ? AND source_date = ?",
            [sm.node, sm.prefix, sm.date],
        ).fetchone()[0]
        true_total = con.execute(
            "SELECT COUNT(*) FROM read_json(?, "
            f"columns={_JSON_COLUMNS_SPEC_COUNT_ONLY}, format='array')",
            [str(json_path)],
        ).fetchone()[0]
        skipped = true_total - inserted
        if skipped > 0:
            logger.warning(
                "%s: %d of %d records skipped by ignore_errors (malformed values)",
                json_path, skipped, true_total,
            )
    except Exception:
        logger.exception(
            "Unable to verify record count for %s -- the load itself "
            "succeeded, but this skip-count visibility check failed.",
            json_path,
        )
    return True


def rebuild_hot_tail(
    con: duckdb.DuckDBPyConnection,
    loaded_matches: list[SentinelMatch],
    source_root: Path,
) -> None:
    logger.info("Wiping hot tail (%d LOADED files to reload)", len(loaded_matches))
    con.execute(f"DELETE FROM {HOT_TABLE}")
    loaded_ok = 0
    t_progress = time.perf_counter()
    total = len(loaded_matches)
    for idx, sm in enumerate(loaded_matches, start=1):
        if _load_json_into_table(con, sm, HOT_TABLE, source_root):
            loaded_ok += 1
        if idx % PROGRESS_LOG_INTERVAL == 0 or idx == total:
            elapsed = time.perf_counter() - t_progress
            logger.info(
                "Hot tail progress: %d/%d files (%.1fs elapsed, %.1f files/sec)",
                idx, total, elapsed, idx / elapsed if elapsed > 0 else 0,
            )
    logger.info("Hot tail rebuilt: %d/%d files loaded", loaded_ok, total)


def _load_ledger(
    con: duckdb.DuckDBPyConnection,
) -> dict[tuple[str, str, str], tuple[tuple[int, ...], str]]:
    rows = con.execute(
        f"SELECT node, prefix, date, phases, sentinel_filename FROM {LEDGER_TABLE}"
    ).fetchall()
    ledger: dict[tuple[str, str, str], tuple[tuple[int, ...], str]] = {}
    for node, prefix, date, phases_str, filename in rows:
        phases = tuple(int(p) for p in phases_str.split("."))
        ledger[(node, prefix, date)] = (phases, filename)
    return ledger


def _upsert_ledger(con: duckdb.DuckDBPyConnection, sm: SentinelMatch) -> None:
    phases_str = ".".join(str(p) for p in sm.phases)
    con.execute(
        f"""
        INSERT INTO {LEDGER_TABLE} (node, prefix, date, phases, sentinel_filename)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (node, prefix, date) DO UPDATE SET
            phases = excluded.phases,
            sentinel_filename = excluded.sentinel_filename
        """,
        [sm.node, sm.prefix, sm.date, phases_str, sm.filename],
    )


def process_frozen_zone(
    con: duckdb.DuckDBPyConnection,
    done_matches: list[SentinelMatch],
    source_root: Path,
) -> None:
    ledger = _load_ledger(con)
    new_count = reload_count = noop_count = error_count = 0
    t_progress = time.perf_counter()
    total = len(done_matches)

    for idx, sm in enumerate(done_matches, start=1):
        prior = ledger.get(sm.logical_key)

        if prior is None:
            logger.info("New file_download file: %s", sm.filename)
            con.execute(
                f"DELETE FROM {FROZEN_TABLE} WHERE source_node = ? AND source_prefix = ? "
                "AND source_date = ?",
                [sm.node, sm.prefix, sm.date],
            )
            if _load_json_into_table(con, sm, FROZEN_TABLE, source_root):
                _upsert_ledger(con, sm)
                new_count += 1
            else:
                error_count += 1

        elif sm.phases == prior[0]:
            noop_count += 1

        elif is_sequential_extension(prior[0], sm.phases):
            prior_filename = prior[1]
            logger.info(
                "Reloading %s: sentinel extended from %s to %s",
                sm.logical_key, prior_filename, sm.filename,
            )
            con.execute(
                f"DELETE FROM {FROZEN_TABLE} WHERE source_node = ? AND source_prefix = ? "
                "AND source_date = ?",
                [sm.node, sm.prefix, sm.date],
            )
            if _load_json_into_table(con, sm, FROZEN_TABLE, source_root):
                _upsert_ledger(con, sm)
                reload_count += 1
            else:
                error_count += 1

        else:
            prior_filename = prior[1]
            logger.error(
                "Unexpected sentinel change for %s: recorded=%s current=%s "
                "(not an unbroken sequential extension)",
                sm.logical_key, prior_filename, sm.filename,
            )
            error_count += 1

        if idx % PROGRESS_LOG_INTERVAL == 0 or idx == total:
            elapsed = time.perf_counter() - t_progress
            logger.info(
                "Frozen zone progress: %d/%d files (%.1fs elapsed, %.1f files/sec) -- "
                "%d new, %d reloaded, %d unchanged, %d errors so far",
                idx, total, elapsed, idx / elapsed if elapsed > 0 else 0,
                new_count, reload_count, noop_count, error_count,
            )

    current_keys = {sm.logical_key for sm in done_matches}
    missing_count = 0
    for logical_key, (_phases, filename) in ledger.items():
        if logical_key not in current_keys:
            logger.error(
                "Ledger entry %s (sentinel=%s) has no matching DONE file on this run",
                logical_key, filename,
            )
            missing_count += 1

    total_errors = error_count + missing_count
    logger.info(
        "Frozen zone: %d new, %d reloaded, %d unchanged, %d errors"
        "(%d ledger entries with no current match, %d other)",
        new_count, reload_count, noop_count, total_errors, missing_count, error_count,
    )


if __name__ == '__main__':
    msg =   f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to load Duck DB at {duckdb_path}" \
            f"  using JSON files at {PIPELINE_OUTPUT_DIR}.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_green_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    verify_configuration_expectations()

    t_start = time.perf_counter()

    all_matches: list[SentinelMatch] = []
    for node in node_dir_list:
        node_dir = source_root / node
        if not node_dir.is_dir():
            logger.warning("Node directory not found, skipping: %s", node_dir)
            continue
        all_matches.extend(scan_node_directory(node, node_dir))

    logger.info(
        "Scanned %d sentinel matches across %d configured node directories",
        len(all_matches), len(node_dir_list),
    )

    t_validate = time.perf_counter()
    dup_errors = validate_no_duplicates(all_matches)
    if dup_errors:
        for dup_msg in dup_errors:
            logger.error(dup_msg)
        logger.error(
            "%d duplicate sentinel match(es) found; exiting before touching DuckDB.",
            len(dup_errors),
        )
        bad_news = (f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" {len(dup_errors)} duplicate sentinel match(es) found. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_green_square::skull_and_crossbones: ' * 5}\n"
                    f":large_green_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(1)
    logger.info("Duplicate-match validation clean (%.3fs)", time.perf_counter() - t_validate)

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    db_lock = acquire_db_write_lock(duckdb_path)
    if db_lock is None:
        logger.error("Gave up after %d minutes waiting for the writer lock on %s.",
                     DB_LOCK_WAIT_SECONDS // 60, duckdb_path)
        bad_news = (f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Gave up after {DB_LOCK_WAIT_SECONDS // 60} minutes waiting for another process to release"
                    f" the writer lock on {duckdb_path}. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_green_square::skull_and_crossbones: ' * 5}\n"
                    f":large_green_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(1)
    con = duckdb.connect(str(duckdb_path))
    _ensure_schema(con)

    # Load in date order, then node, then prefix (see the docstring).  The ledger is
    # keyed by (node, prefix, date), so the order never changes what gets loaded.
    def load_order(m: SentinelMatch):
        return (m.date, m.node, m.prefix)
    loaded_matches = sorted((m for m in all_matches if m.state == "LOADED"), key=load_order)
    done_matches = sorted((m for m in all_matches if m.state == "DONE"), key=load_order)

    t_hot = time.perf_counter()
    rebuild_hot_tail(con, loaded_matches, source_root)
    logger.info("Hot tail rebuild took %.3fs", time.perf_counter() - t_hot)

    t_frozen = time.perf_counter()
    process_frozen_zone(con, done_matches, source_root)
    logger.info("Frozen zone processing took %.3fs", time.perf_counter() - t_frozen)

    hot_count = con.execute(f"SELECT COUNT(*) FROM {HOT_TABLE}").fetchone()[0]
    frozen_count = con.execute(f"SELECT COUNT(*) FROM {FROZEN_TABLE}").fetchone()[0]
    con.close()

    process_utc_finish = datetime.now(tz_utc)
    db_size_mb = duckdb_path.stat().st_size / (1024 * 1024)
    good_news = (f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" {HOT_TABLE}={hot_count} rows, {FROZEN_TABLE}={frozen_count} rows,"
                 f" duckdb file size={db_size_mb:.1f} MB.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':green_heart: ' * 5}\n"
                 f":large_green_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0)
