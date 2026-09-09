#!/bin/bash

####################################################################################################
# Load globus-downloads-to-JSON's output (file_download / hot_file_download) into DuckDB.
#
# Intended crontab entry (adjust day-of-week and time as needed):
#   /hive/users/hive/scripts/PittCronJobs/analytics/usage-analytics/log-processing/analytics-platform-loading/src/duckdb_loader.sh >> .../exec_info/bash_output.log 2>&1
#
# This script runs code from the GitHub repo
# https://github.com/x-atlas-consortia/usage-analytics
#
# Logs shell output to PROCESS_DIR/exec_info/bash_output.log (via crontab redirect).
# Logs Python output to PROCESS_DIR/exec_info/duckdb_loader_python_output.log -- this
# overlaps somewhat with duckdb_loader.py's own internal logging (which writes a second,
# separately-timestamped file in the same directory, and also streams to stdout/stderr,
# which is what lands here), but this redirect is what catches anything printed before
# the Python script's own logging is configured (i.e. its own early config-loading
# failures) or an unexpected uncaught exception, same as elsewhere in the portfolio.
#
# This script intentionally does minimal pre-flight checking. The Python script reads its
# own configuration and reports any missing INI keys or directories via a non-zero exit
# code and its own log messages. (NOTE: unlike geolocation_details_updater.py and similar,
# duckdb_loader.py does not yet have Slack notification wired in -- that's a known,
# deliberately deferred gap, not an oversight in this wrapper.)
#
# duckdb_loader.ini (the real one, not duckdb_loader.ini.example) must never be committed
# to this repo, matching the existing convention for every other process's own real ini.
#
# KBKBKB @TODO: revisit once Slack notifications are added to duckdb_loader.py, to match
# the halt/notify pattern the rest of the portfolio uses.
####################################################################################################

function enter_script() {
    echo Begin execution $0 at `date` by `whoami`
}

function exit_script() {
    echo End execution $0 at `date` by `whoami`
    exit $1
}

enter_script

# Derive all directory locations from the location of this script, so the script
# works without modification on any server where the repo is checked out.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_DIR="$(dirname "$SCRIPT_DIR")"        # .../analytics-platform-loading
PORTFOLIO_DIR="$(dirname "$PROCESS_DIR")"     # .../log-processing
SRC_DIR="$SCRIPT_DIR"                         # .../src (same as SCRIPT_DIR)
PORTFOLIO_SRC_DIR="$PORTFOLIO_DIR/src"
EXEC_INFO_DIR="$PROCESS_DIR/exec_info"

echo SCRIPT_DIR=$SCRIPT_DIR
echo PROCESS_DIR=$PROCESS_DIR
echo PORTFOLIO_DIR=$PORTFOLIO_DIR
echo EXEC_INFO_DIR=$EXEC_INFO_DIR

# @TODO: On a fresh deployment, populate the reqs/ directory before running:
#        pip install -r requirements.txt -t ../reqs/
#        (requirements.txt currently lists just "duckdb")

# Establish PYTHONPATH: process src/, portfolio src/, and the process reqs/ directory.
for a_path in "$SRC_DIR" "$PORTFOLIO_SRC_DIR" "$PROCESS_DIR/reqs"; do
    case ":$PYTHONPATH:" in
        *":$a_path:"*) ;;
        *) PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$a_path" ;;
    esac
done
export PYTHONPATH
echo Exported PYTHONPATH=$PYTHONPATH

# Make sure exec_info exists, since that's where Python's own output log goes; if Python
# fails for any other configuration reason, we want that failure captured on disk.
mkdir -p "$EXEC_INFO_DIR"

# cd into SRC_DIR so the Python script's own relative-path candidate search (a bare
# .ini filename, ../../src/logProcessingProject.ini, a bare 'exec_info', etc. -- see
# loader_config.py) resolves correctly. Without this, CWD is whatever cron started the
# script in (typically $HOME), not this script's own directory, and none of those
# candidates would find anything. Exit code 3 on failure matches duckdb_loader.py's own
# convention for "couldn't even get started due to a config/environment problem."
cd "$SRC_DIR" || exit_script 3

echo executing "python3 duckdb_loader.py with PYTHONPATH=$PYTHONPATH"
# NOTE: no --process-dir here, unlike geolocation_details_updater.py and similar --
# duckdb_loader.py deliberately does not take that argument; its own directory is
# derived from the shared ini's PROJECT_HIVE_DIR plus its own PROC_NAME instead.
python3 "$SRC_DIR/duckdb_loader.py" > "$EXEC_INFO_DIR/duckdb_loader_python_output.log" 2>&1

exit_script $?
