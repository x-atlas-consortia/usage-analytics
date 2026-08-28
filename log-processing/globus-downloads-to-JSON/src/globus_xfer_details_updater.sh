#!/bin/bash

####################################################################################################
# Resolve TBD fields (e.g. 'user') in LOGGED.json files using the Globus transfer-details CSV,
# and write the resolved records to a corresponding XFER.json file.
#
# Intended crontab entry (adjust day-of-week and time as needed):
#   /hive/users/hive/scripts/PittCronJobs/analytics/usage-analytics/log-processing/globus-downloads-to-JSON/src/globus_xfer_details_updater.sh >> .../exec_info/bash_output.log 2>&1
#
# This script runs code from the GitHub repo
# https://github.com/x-atlas-consortia/usage-analytics
#
# Logs shell output to PROCESS_DIR/exec_info/bash_output.log (via crontab redirect).
# Logs Python output to PROCESS_DIR/exec_info/globus_xfer_details_updater_python_output.log.
#
# This script intentionally does minimal pre-flight checking. The Python script reads its
# own configuration and reports any missing INI keys or directories via Slack and a
# non-zero exit code. Re-validating every INI-listed path here just duplicates that logic.
#
# Whether a LOGGED.json file is deleted after a successful XFER.json write is controlled by
# a hard-coded flag (DELETE_LOGGED_JSON_ON_SUCCESS) inside globus_xfer_details_updater.py,
# not by anything in this shell script or its INI file.
#
# KBKBKB @TODO: reword "To reload" below once this process has been in place a while --
# reloading now needs to account for the LOGGED.json / XFER.json split.
# To reload: delete the local XFER.json files and, if any LOGGED.json files were already
# deleted by DELETE_LOGGED_JSON_ON_SUCCESS, regenerate them by re-running the extraction
# scripts before re-running this one.
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
PROCESS_DIR="$(dirname "$SCRIPT_DIR")"        # .../globus-downloads-to-JSON
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
# .ini filename, a bare 'exec_info') resolves correctly. Without this, CWD is whatever
# cron started the script in (typically $HOME), not this script's own directory, and
# none of the Python script's candidates would find anything.
cd "$SRC_DIR" || exit_script 3

echo executing "python3 globus_xfer_details_updater.py with PYTHONPATH=$PYTHONPATH"
python3 "$SRC_DIR/globus_xfer_details_updater.py" --process-dir "$PROCESS_DIR" > "$EXEC_INFO_DIR/globus_xfer_details_updater_python_output.log" 2>&1

exit_script $?
