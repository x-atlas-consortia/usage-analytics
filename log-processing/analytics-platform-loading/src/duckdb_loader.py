#!/usr/bin/env python3
"""
DuckDB loader for the globus-downloads-to-JSON pipeline's File Downloads
output (GridFTP + HTTP transfer logs on dtn03 / dtn02 / app001).

Per run:
  1. Duplicate-match validation, FIRST, across all node directories. If any
     (node, prefix, date, state) matches more than one sentinel file, log
     every offending case and exit before touching DuckDB at all.
  2. Hot tail: wiped completely, then rebuilt from scratch from whatever
     currently matches the LOADED sentinel pattern. No diffing, no history.
  3. Frozen zone: append-only, driven by a DuckDB-internal ledger table
     keyed by (node, prefix, date) -> sentinel filename loaded. A DONE
     sentinel whose name is unchanged from the ledger is a no-op; one
     extended by an unbroken run of the next sequential phase number(s)
     triggers a reload (log.info); anything else -- missing, shorter, or
     a non-sequential change -- triggers a log.error and is left alone.

Sentinel content is otherwise treated as opaque: this loader does not know
or care what "LOADED" or "DONE" mean beyond the two regex states above, so
renaming/restructuring the rest of the sentinel vocabulary upstream should
not require changes here.

NOTE ON STORAGE (v1 simplification, flagged for discussion): "hot tail" and
"frozen zone" are the concepts (recent + volatile vs. settled + append-only);
the actual tables are hot_file_download (LOADED-backed) and file_download
(DONE-backed), both plain DuckDB tables inside one .duckdb file, loaded
directly via read_json_auto -- not the monthly-consolidated Parquet layout
discussed earlier. Whether Parquet export happens at all is now an open
question rather than a settled follow-on step: the whole hybrid design may
simplify to DuckDB-only once evaluated and reviewed. Exporting a table to
partitioned Parquet later, if it does happen, is a cheap columnar copy of
already-typed data (not a re-parse of the source JSON), so it doesn't cost
much either way.

NOTE ON CONFIG: this loader lives in analytics-platform-loading, a sibling
project to log-processing/src (where the shared logProcessingProject.ini
and log_extract_xfer_utils.py live). Per Karl's call, there is no
--process-dir argument -- config discovery relies on a consistent cwd
convention, and PIPELINE_OUTPUT_DIR / NODE_LOG_DIR_LIST / DUCKDB_PATH now
live in the SHARED ini rather than this project's own. This project's own
duckdb_loader.ini is now shaped exactly like every other process's own ini
([ProcessSpecificSettings]: PROC_NAME + Slack settings), per Karl's call to
align conventions. See loader_config.py for the actual discovery logic and
the two things flagged there rather than assumed (JSON_FILE_NIGHTLY_DIR's
removal from the shared ini, and whether LogExtractXferUtils.get_config()
already exposes the three new shared keys).

NOTE ON PROVENANCE: the `provenance` field from the source JSON is not
currently loaded into either table -- none of the sponsor's initial query
batch needs it, and Phase 2/3 re-stamp their `process_utc_dt` on every run
regardless of whether the record changed, so it wouldn't be reliable for
freshness logic anyway. Add it later if a query needs it.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

from loader_config import find_exec_info_dir, load_own_config, load_shared_config
from sentinel_logic import (
    SentinelMatch,
    is_sequential_extension,
    scan_node_directory,
    validate_no_duplicates,
)

LOGGER = logging.getLogger("globus_downloads_loader")

# How often (in files processed) to log a progress line during the
# potentially long frozen-zone / hot-tail loops -- so a run has a visible
# timing trail in the exec_info log even if nobody is watching the console.
PROGRESS_LOG_INTERVAL = 100

# Table names. "hot tail" / "frozen zone" are still how we talk about these
# two conceptually (recent + volatile vs. settled + append-only), but the
# actual DuckDB tables are named per Karl's call:
HOT_TABLE = "hot_file_download"      # LOADED-backed
FROZEN_TABLE = "file_download"       # DONE-backed
# Not explicitly named by Karl -- extending his naming to the ledger too
# for consistency; flag if you'd rather keep frozen_zone_ledger or want a
# different name entirely.
LEDGER_TABLE = "file_download_ledger"

# Shared column layout for hot_file_download and file_download. Kept
# explicit rather than inferred from the first file loaded, so schema
# can't silently drift file-to-file.
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
    dataset_uuid VARCHAR,
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

# Explicit schema for read_json, rather than read_json_auto's per-file
# inference. Discovered live: reading one globus_access_log- (HTTP) file
# with read_json_auto only inferred 5 columns (dataset_uuid,
# bytes_transferred, geolocation_info, relative_file_path,
# download_date_time), missing destination_ip / user_info / protocol /
# globus_task_id entirely -- then threw a binder error the moment a
# gridftp.log- (GridFTP) file referenced one of those. Per Karl, protocol
# should be universally present, so this was likely read_json_auto's
# sampling missing it in that file rather than a genuine structural gap --
# but declaring the schema explicitly is the right fix regardless of which
# it turns out to be: every field's absence resolves to NULL instead of a
# bind error, for whatever reason it's absent.
_JSON_COLUMNS_SPEC = (
    "{"
    "'destination_ip': 'VARCHAR', "
    "'geolocation_info': 'STRUCT(country_code VARCHAR, country_name VARCHAR, "
    "region_name VARCHAR, city_name VARCHAR, zip_code VARCHAR)', "
    "'user_info': 'STRUCT(\"user\" VARCHAR, user_domain VARCHAR, user_tld VARCHAR)', "
    "'dataset_uuid': 'VARCHAR', "
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
        dataset_uuid,
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

# Same shape as _JSON_COLUMNS_SPEC, but bytes_transferred as VARCHAR --
# nothing can fail to cast to VARCHAR, so COUNT(*) against this gives the
# file's TRUE total record count regardless of malformed values, letting
# us see exactly how many records ignore_errors=true silently dropped
# rather than leaving that invisible.
_JSON_COLUMNS_SPEC_COUNT_ONLY = _JSON_COLUMNS_SPEC.replace(
    "'bytes_transferred': 'BIGINT'", "'bytes_transferred': 'VARCHAR'"
)


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
    max_rows_per_file: int | None = None,
) -> bool:
    json_path = source_root / sm.node / sm.data_filename
    if not json_path.exists():
        LOGGER.error(
            "Expected data file missing for sentinel %s: %s", sm.filename, json_path
        )
        return False
    # LIMIT is interpolated directly (DuckDB doesn't accept it as a bound
    # ? parameter) -- safe here since it's an int from argparse, never raw
    # user/file text.
    limit_clause = f"LIMIT {int(max_rows_per_file)}" if max_rows_per_file else ""
    try:
        con.execute(
            f"INSERT INTO {table} {_INSERT_SELECT} {limit_clause}",
            [sm.node, sm.prefix, sm.date, str(json_path)],
        )
    except Exception:
        # Deliberately non-fatal: one file with an unexpected shape
        # shouldn't take down a run processing thousands of others.
        # Full traceback goes to both the console and the exec_info log.
        LOGGER.exception(
            "Failed to load %s (sentinel %s) into %s -- skipping this file, continuing.",
            json_path, sm.filename, table,
        )
        return False

    if max_rows_per_file is None:
        # Visibility into ignore_errors=true's silent drops: compare the
        # row count actually inserted for this file against its TRUE total
        # record count (bytes_transferred read as VARCHAR, so nothing can
        # fail to cast) -- rather than leaving the skip count invisible.
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
                LOGGER.warning(
                    "%s: %d of %d records skipped by ignore_errors (malformed values)",
                    json_path, skipped, true_total,
                )
        except Exception:
            LOGGER.exception(
                "Unable to verify record count for %s -- the load itself "
                "succeeded, but this skip-count visibility check failed.",
                json_path,
            )
    return True


def rebuild_hot_tail(
    con: duckdb.DuckDBPyConnection,
    loaded_matches: list[SentinelMatch],
    source_root: Path,
    max_rows_per_file: int | None = None,
) -> None:
    LOGGER.info("Wiping hot tail (%d LOADED files to reload)", len(loaded_matches))
    con.execute(f"DELETE FROM {HOT_TABLE}")
    loaded_ok = 0
    t_progress = time.perf_counter()
    total = len(loaded_matches)
    for idx, sm in enumerate(loaded_matches, start=1):
        if _load_json_into_table(con, sm, HOT_TABLE, source_root, max_rows_per_file):
            loaded_ok += 1
        if idx % PROGRESS_LOG_INTERVAL == 0 or idx == total:
            elapsed = time.perf_counter() - t_progress
            LOGGER.info(
                "Hot tail progress: %d/%d files (%.1fs elapsed, %.1f files/sec)",
                idx, total, elapsed, idx / elapsed if elapsed > 0 else 0,
            )
    LOGGER.info("Hot tail rebuilt: %d/%d files loaded", loaded_ok, total)


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
    max_rows_per_file: int | None = None,
) -> None:
    ledger = _load_ledger(con)
    new_count = reload_count = noop_count = error_count = 0
    t_progress = time.perf_counter()
    total = len(done_matches)

    for idx, sm in enumerate(done_matches, start=1):
        prior = ledger.get(sm.logical_key)

        if prior is None:
            LOGGER.info("New file_download file: %s", sm.filename)
            con.execute(
                f"DELETE FROM {FROZEN_TABLE} WHERE source_node = ? AND source_prefix = ? "
                "AND source_date = ?",
                [sm.node, sm.prefix, sm.date],
            )
            if _load_json_into_table(con, sm, FROZEN_TABLE, source_root, max_rows_per_file):
                _upsert_ledger(con, sm)
                new_count += 1
            else:
                error_count += 1

        elif sm.phases == prior[0]:
            noop_count += 1

        elif is_sequential_extension(prior[0], sm.phases):
            prior_filename = prior[1]
            LOGGER.info(
                "Reloading %s: sentinel extended from %s to %s",
                sm.logical_key, prior_filename, sm.filename,
            )
            con.execute(
                f"DELETE FROM {FROZEN_TABLE} WHERE source_node = ? AND source_prefix = ? "
                "AND source_date = ?",
                [sm.node, sm.prefix, sm.date],
            )
            if _load_json_into_table(con, sm, FROZEN_TABLE, source_root, max_rows_per_file):
                _upsert_ledger(con, sm)
                reload_count += 1
            else:
                error_count += 1

        else:
            prior_filename = prior[1]
            LOGGER.error(
                "Unexpected sentinel change for %s: recorded=%s current=%s "
                "(not an unbroken sequential extension)",
                sm.logical_key, prior_filename, sm.filename,
            )
            error_count += 1

        # Unconditional -- NOT skipped by any branch above (no continue
        # statements in this loop), since on this very first run every file
        # hits the "new" branch and a skippable progress check would never
        # fire at all.
        if idx % PROGRESS_LOG_INTERVAL == 0 or idx == total:
            elapsed = time.perf_counter() - t_progress
            LOGGER.info(
                "Frozen zone progress: %d/%d files (%.1fs elapsed, %.1f files/sec) -- "
                "%d new, %d reloaded, %d unchanged, %d errors so far",
                idx, total, elapsed, idx / elapsed if elapsed > 0 else 0,
                new_count, reload_count, noop_count, error_count,
            )

    current_keys = {sm.logical_key for sm in done_matches}
    missing_count = 0
    for logical_key, (_phases, filename) in ledger.items():
        if logical_key not in current_keys:
            LOGGER.error(
                "Ledger entry %s (sentinel=%s) has no matching DONE file on this run",
                logical_key, filename,
            )
            missing_count += 1

    total_errors = error_count + missing_count
    LOGGER.info(
        "Frozen zone: %d new, %d reloaded, %d unchanged, %d errors"
        "(%d ledger entries with no current match, %d other)",
        new_count, reload_count, noop_count, total_errors, missing_count, error_count,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load globus-downloads-to-JSON output into DuckDB"
    )
    parser.add_argument(
        "--source-root", type=Path, default=None,
        help="Override PIPELINE_OUTPUT_DIR. Mainly for pointing at synthetic "
             "fixtures during testing.",
    )
    parser.add_argument(
        "--duckdb-path", type=Path, default=None,
        help="Override DUCKDB_PATH from the shared config.",
    )
    parser.add_argument(
        "--max-rows-per-file", type=int, default=None,
        help="TESTING ONLY: load at most this many records from each source "
             "JSON file, silently dropping the rest. Defaults to unlimited. "
             "Never pass this for a real production run.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    try:
        own_config = load_own_config()
    except FileNotFoundError as e:
        print(f"\a\n{e}\n")
        return 3

    try:
        shared_config = load_shared_config()
    except FileNotFoundError as e:
        print(f"\a\n{e}\n")
        return 3

    source_root = args.source_root if args.source_root is not None else Path(
        shared_config["PIPELINE_OUTPUT_DIR"]
    )
    duckdb_path = args.duckdb_path if args.duckdb_path is not None else Path(
        shared_config["DUCKDB_PATH"]
    )
    node_dir_list = shared_config["NODE_LOG_DIR_LIST"]

    try:
        exec_info_dir = find_exec_info_dir(
            shared_config["PROJECT_HIVE_DIR"], own_config["PROC_NAME"]
        )
    except FileNotFoundError as e:
        print(f"\a\n{e}\n")
        return 3

    log_file_name = (
        f"{exec_info_dir}/duckdb_loader-{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
    )
    # Logs to both the exec_info file (matching portfolio convention) and
    # the console (NOT in geolocation_details_updater.py, which is file-only
    # plus separate print() calls) -- added so timing/size numbers are
    # visible during an interactive run without tailing the log file. Say
    # if you'd rather match the file-only + print() style exactly instead.
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(log_file_name)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logging.basicConfig(level=args.log_level, handlers=[file_handler, stream_handler])
    LOGGER.info("Logging to %s", log_file_name)

    t_start = time.perf_counter()

    if args.max_rows_per_file is not None:
        LOGGER.warning(
            "--max-rows-per-file=%d is set -- this is a TESTING run that "
            "silently truncates every file. Do not treat resulting row "
            "counts as real.",
            args.max_rows_per_file,
        )

    all_matches: list[SentinelMatch] = []
    for node in node_dir_list:
        node_dir = source_root / node
        if not node_dir.is_dir():
            LOGGER.warning("Node directory not found, skipping: %s", node_dir)
            continue
        all_matches.extend(scan_node_directory(node, node_dir))

    LOGGER.info(
        "Scanned %d sentinel matches across %d configured node directories",
        len(all_matches), len(node_dir_list),
    )

    # Step 1, moved to the front per Karl's call: fail fast, before touching
    # DuckDB or the hot tail, if any duplicate matches are found.
    t_validate = time.perf_counter()
    dup_errors = validate_no_duplicates(all_matches)
    if dup_errors:
        for msg in dup_errors:
            LOGGER.error(msg)
        LOGGER.error(
            "%d duplicate sentinel match(es) found; exiting before touching DuckDB.",
            len(dup_errors),
        )
        return 1
    LOGGER.info("Duplicate-match validation clean (%.3fs)", time.perf_counter() - t_validate)

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    _ensure_schema(con)

    loaded_matches = [m for m in all_matches if m.state == "LOADED"]
    done_matches = [m for m in all_matches if m.state == "DONE"]

    t_hot = time.perf_counter()
    rebuild_hot_tail(con, loaded_matches, source_root, args.max_rows_per_file)
    LOGGER.info("Hot tail rebuild took %.3fs", time.perf_counter() - t_hot)

    t_frozen = time.perf_counter()
    process_frozen_zone(con, done_matches, source_root, args.max_rows_per_file)
    LOGGER.info("Frozen zone processing took %.3fs", time.perf_counter() - t_frozen)

    hot_count = con.execute(f"SELECT COUNT(*) FROM {HOT_TABLE}").fetchone()[0]
    frozen_count = con.execute(f"SELECT COUNT(*) FROM {FROZEN_TABLE}").fetchone()[0]
    con.close()

    db_size_mb = duckdb_path.stat().st_size / (1024 * 1024)
    LOGGER.info(
        "Done in %.3fs total. %s=%d rows, %s=%d rows, duckdb file size=%.1f MB",
        time.perf_counter() - t_start, HOT_TABLE, hot_count, FROZEN_TABLE, frozen_count,
        db_size_mb,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
