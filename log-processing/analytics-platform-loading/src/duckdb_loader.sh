#!/bin/bash

#####################################################################################
# Wrapper for duckdb_loader.py to load globus-downloads-to-JSON's output files into
# the DuckDB file_download and hot_file_download tables.
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
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"        # .../analytics-platform-loading
PORTFOLIO_DIR="$(dirname "$PROJECT_DIR")"     # .../log-processing
SRC_DIR="$SCRIPT_DIR"                         # .../src (same as SCRIPT_DIR)
PORTFOLIO_SRC_DIR="$PORTFOLIO_DIR/src"
REQS_DIR="$PORTFOLIO_DIR/reqs"                # shared, portfolio-level -- not this project's own
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

# Establish PYTHONPATH: process src/, portfolio src/, and the shared portfolio reqs/ directory.
for a_path in "$SRC_DIR" "$PORTFOLIO_SRC_DIR" "$REQS_DIR"; do
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

# cd into SRC_DIR so the Python script's own relative-path candidate searches
# resolve correctly. Exit code 3 to indicate could not start due to config/environment
# problems.
cd "$SRC_DIR" || exit_script 3

echo executing "$PYTHON_CMD duckdb_loader.py with PYTHONPATH=$PYTHONPATH"
"$PYTHON_CMD" "$SRC_DIR/duckdb_loader.py" --project-dir "$PROJECT_DIR" > "$EXEC_INFO_DIR/duckdb_loader_python_output.log" 2>&1

exit_script $?
