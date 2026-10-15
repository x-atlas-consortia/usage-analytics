#!/usr/bin/env python3
"""
Phase 2 of analytics-platform-loading: load the api-usage-to-JSON output, derived
from AWS API Gateway execution log records for entity-api, search-api, uuid-api,
into the api_usage table of the shared usage-analytics DuckDB file.

After the move to PSC, API Usage will originate from log files created by the
HuBMAP Gateway and service APIs executing there.

Phase 1, for File Downloads, loads into the same DuckDB file.  Since DuckDB only
allows one writing process at a time, each loader takes or waits for an exclusive lock on
the <database>.lock file before opening the database (see acquire_db_write_lock).

This load is incremental.  Each source file is loaded once, since the
data it contains is finalized.
  - Files have been loaded are recorded in api_usage_loaded_files.
    If a name is in that table, the file is not loaded again.
  - Each file is loaded in a single transaction: its rows and its ledger
    entry are committed together, or neither is.  A file that fails
    leaves nothing behind, is retried on the next run, and does not
    affect the other files; the run reports it and exits non-zero.
  - A file modified within the last --quiet-minutes (default 10) is
    skipped this run and picked up by a later one, so a file that is still
    being written is never recorded as loaded.
  - A file replaced in place under the same name is NOT noticed.  An
    upstream re-extraction of earlier data has to be handled by FULL
    RELOAD, below.
  - A name in the ledger that is no longer in the source directory is
    logged as a warning and otherwise ignored.

FULL RELOAD
stop any loader, then, while holding the writer lock, DROP TABLE api_usage
and DROP TABLE api_usage_loaded_files.  The next run loads every matching file.
The loader refuses to run when api_usage already has rows but the ledger is
empty, because that would load every file a second time.

ROW ORDERING
Each file's rows are appended to api_usage in date order: request_date, then
request_date_time, then host and response_id (for a repeatable order), with
NULL dates and times last.

Order is what makes DuckDB's automatic min-max statistics effective: a filter
on a date can skip every block of rows whose range cannot contain it, and
the more ordered the data, the more it skips.

The files themselves hold one block per host, so without this each day would
be spread over several places.  Files arrive in roughly chronological order,
so appending each sorted file keeps the whole table nearly sorted, deferring
creation of indices of api_usage.

The sort needs a file's rows together, so each file is staged in full in a
temporary table (DuckDB compresses it and can spill it to disk) before it is
appended.  The order of appended rows relies on DuckDB's default
preserve_insertion_order setting.

Source JSON files a JSON Array of JSON Objects with these fields:
-- always present, always strings
    host, response_id, datetime, response_code, user_agent
-- always present as keys, sometimes an explicit JSON null value
    resource_path, resource_path_pattern, http_method, client_ip
-- sometimes absent key
    entity_uuid         
File names do NOT align with the dates of the records inside them (a file named
for one day can hold several days of records, and may overlap other files), so
nothing here depends on the date in a file name.

Missing data never halts the load.  A key that is absent, or present with a
JSON null, becomes NULL in the row.  A response_code that is present but cannot
be converted to an integer is logged with logger.error() (value, file, and
identifying fields of the record) and the record is loaded with a NULL
response_code; the load then continues with the next record.

`datetime` is a naive ISO string (YYYY-MM-DDTHH:MM:SS, whole seconds) that is
UTC.  The request_* columns hold UTC values.

A small fraction of records) have a truncated time portion such as
"2026-08-08T0".  Those rows are loaded with
request_date / request_year / request_month taken from the date prefix, and
request_date_time left NULL; post-load checks report how many there are.
Files in subdirectories (e.g. the bootstrap fragment files) and files not
matching the pattern are ignored.  Expect the need for this to go away with
the next revision of the API Usage source JSON files.

Each source file is streamed, line by line, into newline-delimited chunk files
(see CHUNK_TARGET_BYTES) in a scratch directory, and each chunk is loaded in
turn, so file size is not a limit.  This relies on the layout of these files
(one record per line); a file that departs from it fails with a line number.

Every JSON field is read as VARCHAR and converted in the SELECT with TRY_CAST,
so a surprising value becomes NULL (and is reported in the post-load checks)
rather than failing the whole load.
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
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from log_extract_xfer_utils import LogExtractXferUtils

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('Loading api-usage-to-JSON output into DuckDB')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase2' -> ':two:'), so relocating a script to a different phase directory
# automatically updates its emoji with no code change. Falls back to ':diamonds:'
# if the parent directory doesn't match the 'phaseN' pattern.
_PHASE_NUMBER_WORDS = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven', 8: 'eight', 9: 'nine'}
_phase_dir_match = re.match(r'^phase(\d+)$', Path(__file__).resolve().parent.name)
if _phase_dir_match and int(_phase_dir_match.group(1)) in _PHASE_NUMBER_WORDS:
    SLACK_PHASE_EMOJI = f":{_PHASE_NUMBER_WORDS[int(_phase_dir_match.group(1))]}:"
else:
    SLACK_PHASE_EMOJI = ':diamonds:'

arg_parser = argparse.ArgumentParser(
    description='Load api-usage-to-JSON output into the api_usage DuckDB table.'
)
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                        help="This project's own directory (one level above src/), e.g."
                             " .../log-processing/analytics-platform-loading. Normally supplied"
                             " by the .sh wrapper's own PROJECT_DIR.")
arg_parser.add_argument(
    '--source-root', type=Path, default=None,
    help="Override the API Usage JSON directory "
         "(PITT_ANALYTICS_DIR/LOAD_PROJECT_2_NAME). Mainly for pointing at "
         "synthetic fixtures during testing.",
)
arg_parser.add_argument(
    '--duckdb-path', type=Path, default=None,
    help="Override DUCKDB_PATH from the shared config.",
)
arg_parser.add_argument(
    '--scratch-dir', type=Path, default=None,
    help="Directory for the temporary newline-delimited chunk files each source "
         "file is streamed through (see CHUNK_TARGET_BYTES). Defaults to the "
         "system temporary directory (TMPDIR or /tmp). Needs room for roughly "
         "one chunk at a time, not for a whole source file.",
)
arg_parser.add_argument(
    '--quiet-minutes', type=int, default=10,
    help="Skip, until a later run, any source file modified within this many "
         "minutes, in case it is still being written. 0 disables the check. "
         "Default 10.",
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
    LOAD_PROJECT_2_NAME = Config.get('ProcessSpecificSettings', 'LOAD_PROJECT_2_NAME')
    SLACK_NOTIFICATION_CHANNEL = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATION_CHANNEL')
    SLACK_BAD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_NEWS_EMOJI')
    SLACK_GOOD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_NEWS_EMOJI')
    SLACK_NEUTRAL_INFO_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_INFO_EMOJI')
    SLACK_NOTIFICATIONS = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATIONS')
    slack_user_id_mentions_on_error_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR'))
    slack_user_id_mentions_on_success_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS'))
except Exception as e:
    print(f"\a\nUnable to read configuration from '{config_file_name}'. e={str(e)}\n")
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
                f"/duckdb_api_usage_loader-" \
                f"{datetime.now().strftime('%Y-%m-%d_%H%M%S')}" \
                f".log"
logging.basicConfig(filename=log_file_name
                    , level=logging.INFO
                    , format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    , datefmt='%Y-%m-%d %H:%M:%S')
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
    PITT_ANALYTICS_DIR = portfolio_config['PITT_ANALYTICS_DIR']
    DUCKDB_PATH = portfolio_config['DUCKDB_PATH']
    logger.info("LogExtractXferUtils instantiated.")
    print('LogExtractXferUtils instantiated.')
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

source_root = args.source_root if args.source_root is not None \
    else Path(PITT_ANALYTICS_DIR) / LOAD_PROJECT_2_NAME
duckdb_path = args.duckdb_path if args.duckdb_path is not None else Path(DUCKDB_PATH)

API_USAGE_FILE_PATTERN = re.compile(r"execution_logs-[0-9]{8}\.json")

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


TABLE = "api_usage"
LEDGER_TABLE = "api_usage_loaded_files"

# Column layout.  Kept explicit rather than inferred from the first file loaded,
# so schema can't silently drift file-to-file.  The request_* date columns
# mirror the download_* columns of file_download to make date-based joins and
# comparisons between the two tables parallel.
_TABLE_SCHEMA = """
    host VARCHAR,
    response_id VARCHAR,
    request_date_time TIMESTAMP,
    request_date DATE,
    request_year INTEGER,
    request_month INTEGER,
    response_code INTEGER,
    http_method VARCHAR,
    resource_path VARCHAR,
    resource_path_pattern VARCHAR,
    client_ip VARCHAR,
    user_agent VARCHAR,
    entity_uuid VARCHAR
"""

# Everything is read as VARCHAR; conversion happens in _BATCH_SELECT with
# TRY_CAST.  A field absent from a file's records reads as NULL.
_JSON_COLUMNS_SPEC = (
    "{"
    "'host': 'VARCHAR', "
    "'response_id': 'VARCHAR', "
    "'datetime': 'VARCHAR', "
    "'response_code': 'VARCHAR', "
    "'http_method': 'VARCHAR', "
    "'resource_path': 'VARCHAR', "
    "'resource_path_pattern': 'VARCHAR', "
    "'client_ip': 'VARCHAR', "
    "'user_agent': 'VARCHAR', "
    "'entity_uuid': 'VARCHAR'"
    "}"
)

# A datetime string is trusted as a full timestamp only if it is complete:
# date, time with seconds, optional fractional seconds, optional Z/offset.
# Anything else (e.g. the truncated "2026-08-08T0" seen in real data) gets a
# NULL request_date_time, but its first 10 characters are still used for
# request_date / request_year / request_month when they form a valid date.
# Doing the regex check here, instead of relying on TRY_CAST alone, means a
# malformed string can never be silently turned into a plausible timestamp.
_FULL_TS_REGEX = (r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}"
                  r"(\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?$")

# Each source file is streamed, line by line, into newline-delimited JSON chunk
# files of about CHUNK_TARGET_BYTES bytes, and each chunk is loaded in turn.  The temporary
# chunk files and DuckDB's JSON read buffers are therefore bounded by the chunk
# size, not by the source file size, so very large files (>1 GB) can be read.
# (The rows themselves are then collected per file for sorting; see
# _load_json_file.)
#
# Trying to read each source file directly with read_json(format='array') failed
# with "Malformed JSON ... unexpected character" under DuckDB's default 16 MB
# maximum_object_size, yet loaded with any setting from 17 MB up.  The mechanism
# was not established (a 602 MB file loaded fine at the default), so the loader
# does not depend on that setting.  The same file converted to newline-delimited
# JSON loaded at default settings; the conversion took about 2 seconds.
CHUNK_TARGET_BYTES = 256 * 1024 * 1024

# Columns of the final table, in order.  Used for explicit-column INSERTs so
# the load never depends on positional alignment.
_TABLE_COLUMNS = ("host, response_id, request_date_time, request_date, request_year,"
                  " request_month, response_code, http_method, resource_path,"
                  " resource_path_pattern, client_ip, user_agent, entity_uuid")

# Builds one file's records as a small temporary "batch" table.  Besides the
# final columns it carries two raw columns (datetime_raw, response_code_raw)
# used only to log records whose response_code cannot be converted to an
# integer; they never reach the api_usage table.
#
# No ignore_errors on purpose: with every column read as VARCHAR there are no
# value-type errors to forgive, so anything read_json rejects is structurally
# broken JSON, and that should fail loudly (the file is reported as failed
# and none of its rows are kept) rather than silently dropping records.  Each chunk's
# row count is also checked against the number of records written to it.
_BATCH_SELECT = f"""
    SELECT
        host,
        response_id,
        ts AS request_date_time,
        d AS request_date,
        EXTRACT(YEAR FROM d) AS request_year,
        EXTRACT(MONTH FROM d) AS request_month,
        TRY_CAST(response_code_raw AS INTEGER) AS response_code,
        http_method,
        resource_path,
        resource_path_pattern,
        client_ip,
        user_agent,
        entity_uuid,
        datetime_raw,
        response_code_raw
    FROM (
        SELECT
            host,
            response_id,
            "datetime" AS datetime_raw,
            response_code AS response_code_raw,
            http_method,
            resource_path,
            resource_path_pattern,
            client_ip,
            user_agent,
            entity_uuid,
            CASE WHEN regexp_matches("datetime", '{_FULL_TS_REGEX}')
                 THEN TRY_CAST("datetime" AS TIMESTAMP) END AS ts,
            TRY_CAST(LEFT("datetime", 10) AS DATE) AS d
        FROM read_json(?, columns={_JSON_COLUMNS_SPEC}, format='newline_delimited')
    )
"""

BATCH_TABLE = "api_usage_file_batch"
FILE_ROWS_TABLE = "api_usage_file_rows"

# Order in which each file's rows are appended to api_usage (see the docstring).
# request_date comes first so that rows with a truncated time still sit with their day.
APPEND_ORDER_BY = ("request_date NULLS LAST, request_date_time NULLS LAST,"
                   " host NULLS LAST, response_id NULLS LAST")


def _notify(msg: str, mentions_dict: dict | None = None) -> None:
    """Post to Slack, but never let a Slack problem kill (or mask) the load."""
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=msg
                                           , mentions_dict=mentions_dict)
    except Exception:
        logger.exception('Unable to post Slack notification.')


def _header() -> str:
    return (f":large_green_circle: {portfolio_utils.get_slack_host_context()} :large_green_circle:"
            f" {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_green_circle:\n")


def _fail(reason: str, exit_code: int) -> None:
    """Log and announce a halt, then exit."""
    bad_news = (f"{_header()}"
                f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                f" {reason}\n"
                f" See the logs.\n"
                f" Process logged to {log_file_name}\n"
                f"{':large_green_square::skull_and_crossbones: ' * 5}\n"
                f":large_green_circle:")
    logger.error(bad_news)
    _notify(bad_news, slack_user_id_mentions_on_error_dict)
    sys.exit(exit_code)


def verify_configuration_expectations() -> None:
    if not source_root.is_dir():
        logger.error("Halting program due to not finding source_root directory at '%s'"
                     " relative to '%s'.", source_root, os.getcwd())
        _fail(f"Halted trying to verify configuration expectations:"
              f" API Usage JSON directory not found at {source_root}.", 2)
    if args.scratch_dir is not None and not args.scratch_dir.is_dir():
        logger.error("--scratch-dir '%s' is not an existing directory.", args.scratch_dir)
        _fail(f"Halted: --scratch-dir {args.scratch_dir} is not an existing directory.", 2)


def find_source_files(root: Path) -> list[Path]:
    """Matching files directly in root (non-recursive), oldest date first."""
    return sorted(
        p for p in root.iterdir()
        if p.is_file() and API_USAGE_FILE_PATTERN.fullmatch(p.name)
    )


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} ({_TABLE_SCHEMA})")
    con.execute(f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (filename VARCHAR PRIMARY KEY)")


def _load_ledger(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {row[0] for row in con.execute(f"SELECT filename FROM {LEDGER_TABLE}").fetchall()}


def _recently_modified(path: Path, quiet_seconds: int) -> bool:
    """True if path was modified within the last quiet_seconds (0 disables the check)."""
    if quiet_seconds <= 0:
        return False
    return (time.time() - path.stat().st_mtime) < quiet_seconds


def _log_bad_response_codes(con: duckdb.DuckDBPyConnection, json_path: Path) -> int:
    """Log, one error per record, every record in the current batch whose
    response_code was present but could not be converted to an integer.

    A record whose response_code is simply absent or null in the JSON is NOT
    a conversion failure (it loads with NULL, like any other missing field).
    The records are still loaded, with a NULL response_code; this only makes
    them visible.  Returns the number of such records.
    """
    rows = con.execute(
        f"SELECT response_code_raw, host, response_id, datetime_raw, http_method,"
        f" resource_path, client_ip FROM {BATCH_TABLE}"
        " WHERE response_code_raw IS NOT NULL AND response_code IS NULL"
    ).fetchall()
    for (rc_raw, host, response_id, datetime_raw, http_method,
         resource_path, client_ip) in rows:
        logger.error(
            "Non-integer response_code %r in %s: host=%r response_id=%r datetime=%r"
            " http_method=%r resource_path=%r client_ip=%r -- loaded with NULL"
            " response_code, continuing.",
            rc_raw, json_path.name, host, response_id, datetime_raw,
            http_method, resource_path, client_ip,
        )
    return len(rows)


def _stream_chunks(
    json_path: Path,
    scratch_dir: Path,
    chunk_bytes: int,
):
    """Stream one source file into newline-delimited chunk files.

    Yields (chunk_path, record_count) for each chunk, and deletes the chunk file
    once the caller moves on.  The source layout is expected to be one JSON object
    per line, inside an array: '[' begins the first line, every later record line
    begins with ',', and ']' ends the file.  The records are not parsed here (that
    is DuckDB's job); the layout is checked cheaply instead, and anything that
    departs from it raises ValueError with a line number, because quietly
    mis-splitting a record would be far worse than failing the file.  A missing
    closing ']' (a truncated file) also raises ValueError, before the final chunk
    is yielded.
    """
    out = None
    out_path = None
    out_records = 0
    out_bytes = 0
    chunk_no = 0
    saw_close = False

    try:
        with open(json_path, "rb") as fin:
            for lineno, raw in enumerate(fin, start=1):
                body = raw.rstrip(b"\r\n")
                if lineno == 1:
                    if not body.startswith(b"["):
                        raise ValueError("line 1 does not start with '['")
                    body = body[1:]
                elif body.startswith(b","):
                    body = body[1:]
                elif body.strip() not in (b"", b"]"):
                    raise ValueError(f"line {lineno} does not start with ','")

                stripped = body.strip()
                if stripped == b"]":
                    saw_close = True
                    continue
                if not stripped:
                    continue
                if saw_close:
                    raise ValueError(f"line {lineno}: content after the closing ']'")
                if not (stripped.startswith(b"{") and stripped.endswith(b"}")):
                    raise ValueError(f"line {lineno}: not a single-line JSON object")

                if out is None:
                    chunk_no += 1
                    out_path = scratch_dir / f"{json_path.stem}.chunk{chunk_no:05d}.ndjson"
                    out = open(out_path, "wb")
                    out_records = out_bytes = 0
                out.write(stripped + b"\n")
                out_records += 1
                out_bytes += len(stripped) + 1

                if out_bytes >= chunk_bytes:
                    out.close()
                    out = None
                    try:
                        yield out_path, out_records
                    finally:
                        out_path.unlink(missing_ok=True)

        if not saw_close:
            raise ValueError("file does not end with ']' (truncated?)")

        if out is not None:
            out.close()
            out = None
            try:
                yield out_path, out_records
            finally:
                out_path.unlink(missing_ok=True)
    finally:
        if out is not None:
            out.close()


def _load_json_file(
    con: duckdb.DuckDBPyConnection,
    json_path: Path,
    scratch_dir: Path | None = None,
) -> tuple[int, int] | None:
    """Load one source file into api_usage, in date order, in a single transaction.

    Chunks of the file are converted into a per-file temporary table; once the
    whole file is there, its rows are appended to api_usage sorted by
    APPEND_ORDER_BY, and the ledger entry is added.  Everything commits together.
    If anything fails, the transaction is rolled back: no rows and no ledger entry
    are kept, so the file is simply retried by the next run.

    Returns (rows_inserted, bad_response_code_count), or None if the file could
    not be loaded.  Each chunk goes through a temporary batch table, so the raw
    values are available for error logging.
    """
    inserted_total = 0
    bad_response_codes_total = 0
    try:
        with tempfile.TemporaryDirectory(dir=scratch_dir) as tmp:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(f"CREATE OR REPLACE TEMP TABLE {FILE_ROWS_TABLE} ({_TABLE_SCHEMA})")
                for chunk_path, expected in _stream_chunks(
                        json_path, Path(tmp), CHUNK_TARGET_BYTES):
                    con.execute(f"CREATE OR REPLACE TEMP TABLE {BATCH_TABLE} AS"
                                f" {_BATCH_SELECT}", [str(chunk_path)])
                    read_back = _count(con, BATCH_TABLE)
                    if read_back != expected:
                        raise RuntimeError(
                            f"{chunk_path.name}: wrote {expected} records but DuckDB read"
                            f" {read_back}")
                    bad_response_codes_total += _log_bad_response_codes(con, json_path)
                    con.execute(f"INSERT INTO {FILE_ROWS_TABLE} ({_TABLE_COLUMNS})"
                                f" SELECT {_TABLE_COLUMNS} FROM {BATCH_TABLE}")
                    inserted_total += read_back
                con.execute(f"INSERT INTO {TABLE} ({_TABLE_COLUMNS})"
                            f" SELECT {_TABLE_COLUMNS} FROM {FILE_ROWS_TABLE}"
                            f" ORDER BY {APPEND_ORDER_BY}")
                con.execute(f"INSERT INTO {LEDGER_TABLE} (filename) VALUES (?)", [json_path.name])
                con.execute("COMMIT")
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    logger.exception("ROLLBACK failed for %s.", json_path)
                raise
    except Exception:
        logger.exception("Failed to load %s into %s; nothing from it was kept.", json_path, TABLE)
        return None
    finally:
        for temp_table in (BATCH_TABLE, FILE_ROWS_TABLE):
            try:
                con.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                logger.exception("Unable to drop temporary table %s.", temp_table)
    return inserted_total, bad_response_codes_total


def load_new_files(
    con: duckdb.DuckDBPyConnection,
    files: list[Path],
    scratch_dir: Path | None = None,
) -> tuple[int, list[str], list[str], int]:
    """Load each file, one transaction apiece; a failure does not stop the others.

    Returns (rows_loaded, failed_file_names, empty_file_names,
    bad_response_code_count).
    """
    failed: list[str] = []
    empty: list[str] = []
    rows_loaded = 0
    bad_response_codes = 0
    total = len(files)

    for idx, json_path in enumerate(files, start=1):
        t_file = time.perf_counter()
        result = _load_json_file(con, json_path, scratch_dir)
        if result is None:
            failed.append(json_path.name)
            continue
        inserted, bad_rc = result
        rows_loaded += inserted
        bad_response_codes += bad_rc
        if inserted == 0:
            logger.warning("No records in %s (empty array?); recorded as loaded.", json_path.name)
            empty.append(json_path.name)
        logger.info("Loaded %s: %d rows in %.1fs (%d/%d new files; %d rows so far, %d failed)",
                    json_path.name, inserted, time.perf_counter() - t_file, idx, total,
                    rows_loaded, len(failed))
    return rows_loaded, failed, empty, bad_response_codes


def post_load_checks(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Log summary statistics; return a list of warning strings (empty if all clean)."""
    warnings: list[str] = []

    total, min_dt, max_dt = con.execute(
        f"SELECT COUNT(*), MIN(request_date_time), MAX(request_date_time) FROM {TABLE}"
    ).fetchone()
    logger.info("%s: %d rows, request_date_time range %s .. %s", TABLE, total, min_dt, max_dt)

    # datetime is always present in the source, so every row should have both a
    # request_date and a request_date_time.  A row with a date but no time had a
    # malformed (e.g. truncated) time portion; a row with no date had nothing usable.
    date_only = con.execute(
        f"SELECT COUNT(*) FROM {TABLE}"
        " WHERE request_date_time IS NULL AND request_date IS NOT NULL").fetchone()[0]
    if date_only:
        msg = (f"{date_only} rows have a request_date but NULL request_date_time"
               " (malformed time portion in the datetime string)")
        logger.warning(msg)
        warnings.append(msg)

    no_date = con.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE request_date IS NULL").fetchone()[0]
    if no_date:
        msg = f"{no_date} rows have NULL request_date (datetime string unusable)"
        logger.warning(msg)
        warnings.append(msg)

    # The Elasticsearch _id was host + '_' + response_id + '_' + datetime as epoch seconds,
    # so Elasticsearch silently collapsed duplicates of that key.  DuckDB keeps them;
    # report them here, don't enforce.
    dup_keys = con.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {TABLE}"
        " GROUP BY host, response_id, request_date_time HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if dup_keys:
        msg = f"{dup_keys} (host, response_id, request_date_time) keys occur more than once"
        logger.warning(msg)
        warnings.append(msg)
    else:
        logger.info("No duplicate (host, response_id, request_date_time) keys.")

    return warnings


if __name__ == '__main__':
    launch_msg = (f"{_header()}"
                  f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to load new API Usage files into {TABLE} in Duck DB at {duckdb_path}"
                  f" using JSON files at {source_root}.\n"
                  f" Process logging to {log_file_name}\n"
                  f":large_green_circle:")
    logger.info(launch_msg)
    _notify(launch_msg)

    verify_configuration_expectations()

    all_files = find_source_files(source_root)
    if not all_files:
        logger.error("No files matching %s found in %s.",
                     API_USAGE_FILE_PATTERN.pattern, source_root)
        _fail(f"No files matching {API_USAGE_FILE_PATTERN.pattern} found in {source_root};"
              f" {TABLE} was not touched.", 1)
    logger.info("Found %d source files in %s (%s .. %s)",
                len(all_files), source_root, all_files[0].name, all_files[-1].name)

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    db_lock = acquire_db_write_lock(duckdb_path)
    if db_lock is None:
        logger.error("Gave up after %d minutes waiting for the writer lock on %s.",
                     DB_LOCK_WAIT_SECONDS // 60, duckdb_path)
        _fail(f"Gave up after {DB_LOCK_WAIT_SECONDS // 60} minutes waiting for another process"
              f" to release the writer lock on {duckdb_path}; {TABLE} was not touched.", 1)

    con = duckdb.connect(str(duckdb_path))
    try:
        _ensure_schema(con)
        already_loaded = _load_ledger(con)
        existing_rows = _count(con, TABLE)
        if not already_loaded and existing_rows > 0:
            logger.error("%s has %d rows but %s is empty; loading every file again would duplicate"
                         " them. See 'Starting over' in this script's docstring.",
                         TABLE, existing_rows, LEDGER_TABLE)
            _fail(f"{TABLE} already has {existing_rows} rows but {LEDGER_TABLE} is empty, so every"
                  f" file would be loaded a second time. Drop {TABLE} first (see 'Starting over' in"
                  f" the script's docstring). Nothing was loaded.", 1)

        present_names = {p.name for p in all_files}
        gone = sorted(already_loaded - present_names)
        if gone:
            logger.warning("%d files in %s are no longer in %s (ignored): %s",
                           len(gone), LEDGER_TABLE, source_root, ", ".join(gone[:10]))

        quiet_seconds = args.quiet_minutes * 60
        to_load: list[Path] = []
        deferred: list[str] = []
        for path in all_files:
            if path.name in already_loaded:
                continue
            if _recently_modified(path, quiet_seconds):
                deferred.append(path.name)
            else:
                to_load.append(path)
        if deferred:
            logger.info("%d files modified within the last %d minutes were deferred to a later run: %s",
                        len(deferred), args.quiet_minutes, ", ".join(deferred))
        logger.info("%d files already loaded, %d new to load, %d deferred.",
                    len(already_loaded & present_names), len(to_load), len(deferred))

        t_load = time.perf_counter()
        rows_added, failed_files, empty_files, bad_response_codes = load_new_files(
            con, to_load, args.scratch_dir)
        logger.info("Loading took %.3fs", time.perf_counter() - t_load)

        files_loaded = len(to_load) - len(failed_files)
        check_warnings: list[str] = []
        if files_loaded > 0:
            check_warnings = post_load_checks(con)
        if bad_response_codes:
            msg = (f"{bad_response_codes} records had a non-integer response_code"
                   " (loaded with NULL; each logged as an error)")
            logger.warning(msg)
            check_warnings.append(msg)
        final_count = _count(con, TABLE)
    finally:
        con.close()

    process_utc_finish = datetime.now(tz_utc)
    db_size_mb = duckdb_path.stat().st_size / (1024 * 1024)
    summary = (f" {TABLE}={final_count} rows (+{rows_added} from {files_loaded} new files);"
               f" {len(already_loaded & present_names)} files were already loaded,"
               f" {len(deferred)} deferred as recently modified, {len(failed_files)} failed."
               f" duckdb file size={db_size_mb:.1f} MB.\n")
    warning_text = ""
    if check_warnings:
        warning_text = " Warnings (whole table): " + "; ".join(check_warnings) + ".\n"

    if failed_files:
        logger.error("%d files failed to load: %s", len(failed_files), ", ".join(failed_files[:20]))
        bad_news = (f"{_header()}"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} with"
                    f" {len(failed_files)} failed files (first: {failed_files[0]}); nothing from a failed file"
                    f" was kept, and it will be retried next run.\n"
                    f"{summary}"
                    f"{warning_text}"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_green_square::skull_and_crossbones: ' * 5}\n"
                    f":large_green_circle:")
        logger.error(bad_news)
        _notify(bad_news, slack_user_id_mentions_on_error_dict)
        sys.exit(1)

    good_news = (f"{_header()}"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f"{summary}"
                 f"{warning_text}"
                 f" Process logged to {log_file_name}\n"
                 f"{':green_heart: ' * 5}\n"
                 f":large_green_circle:")
    logger.info(good_news)
    _notify(good_news, slack_user_id_mentions_on_success_dict)

    sys.exit(0)
