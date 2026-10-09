#!/bin/bash

#####################################################################################
# Wrapper for globus_access_log_extract.py extraction and transformation of logged
# Globus HTTP file download events in JSON files.
#
# Uses the shared venv, reqs (replacing site-packages), and src of the log-processing
# portfolio.
#
# This script intentionally does minimal pre-flight checking. The Python script reads
# its own configuration and reports any missing INI keys or directories via a
# non-zero exit code and its own log messages.
#####################################################################################

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
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"  # .../globus-downloads-to-JSON (two levels up, not one)
PORTFOLIO_DIR="$(dirname "$PROJECT_DIR")"            # .../log-processing
SRC_DIR="$SCRIPT_DIR"                                # .../src/phase1 (same as SCRIPT_DIR)
PORTFOLIO_SRC_DIR="$PORTFOLIO_DIR/src"
REQS_DIR="$PORTFOLIO_DIR/reqs"                       # shared, portfolio-level
EXEC_INFO_DIR="$PROJECT_DIR/exec_info"

echo SCRIPT_DIR=$SCRIPT_DIR
echo PROJECT_DIR=$PROJECT_DIR
echo PORTFOLIO_DIR=$PORTFOLIO_DIR
echo EXEC_INFO_DIR=$EXEC_INFO_DIR

# venv/ provides only the interpreter (its own site-packages stays empty by design),
# reqs/ is where actual dependencies live, added to PYTHONPATH below regardless of which Python runs this.
VENV_ACTIVATE="${PORTFOLIO_DIR}/venv/bin/activate"
if [ -f "${VENV_ACTIVATE}" ]; then
    source "${VENV_ACTIVATE}"
    PYTHON_CMD="python"
    echo "Activated virtual environment: ${PORTFOLIO_DIR}/venv"
else
    PYTHON_CMD="python3"
    echo "No venv/ virtual environment found -- using system Python: $(which python3)"
fi

# Portfolio-shared code (log_extract_xfer_utils.py etc.) isn't something pip could ever
# install, so it still needs to be on PYTHONPATH explicitly, alongside the shared reqs/.
export PYTHONPATH="$SRC_DIR:$PORTFOLIO_SRC_DIR:$REQS_DIR"
echo Exported PYTHONPATH=$PYTHONPATH

# Make sure exec_info exists, since that's where Python's own output log goes; if Python
# fails for any other configuration reason, we want that failure captured on disk.
mkdir -p "$EXEC_INFO_DIR"

# cd into SRC_DIR so the Python script's own relative-path candidate searches
# resolve correctly. Exit code 3 to indicate could not start due to config/environment
# problems.
cd "$SRC_DIR" || exit_script 3

echo executing "$PYTHON_CMD globus_access_log_extract.py with PYTHONPATH=$PYTHONPATH"
"$PYTHON_CMD" "$SRC_DIR/globus_access_log_extract.py" --project-dir "$PROJECT_DIR" > "$EXEC_INFO_DIR/globus_access_python_output.log" 2>&1

exit_script $?
