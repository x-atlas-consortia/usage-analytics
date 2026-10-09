#!/bin/bash

#####################################################################################
# Wrapper for augment_with_entity_info.py -- Phase 2 (the augment-with-entity-info
# phase) of the globus-downloads-to-JSON pipeline: resolves PENDING entity_type,
# application_id, and dataset_type fields using Neo4j directly, via the
# EntityInfoProvider class in entity_info_provider.py
#
# Uses the shared venv, reqs (replacing site-packages), and src of the log-processing
# portfolio.
#
# This script intentionally does minimal pre-flight checking. The Python script reads
# its own configuration and reports any missing INI keys or directories via a
# non-zero exit code and its own log messages.
#####################################################################################

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}

function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}

# Derive all directory locations from the location of this script, so the script
# works without modification on any server where the repo is checked out.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # .../globus-downloads-to-JSON/src/phase2
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"        # .../globus-downloads-to-JSON (two levels up, not one)
SRC_DIR="${SCRIPT_DIR}"                                    # .../src/phase2 (same as SCRIPT_DIR)
PORTFOLIO_DIR="$(dirname "$PROJECT_DIR")"                  # .../log-processing
PORTFOLIO_SRC_DIR="${PORTFOLIO_DIR}/src"                   # shared log_extract_xfer_utils.py lives here
REQS_DIR="${PORTFOLIO_DIR}/reqs"                           # shared, portfolio-level
EXEC_INFO_DIR="${PROJECT_DIR}/exec_info"

enter_script

echo "SCRIPT_DIR: ${SCRIPT_DIR}"
echo "SRC_DIR: ${SRC_DIR}"
echo "PROJECT_DIR: ${PROJECT_DIR}"
echo "PORTFOLIO_DIR: ${PORTFOLIO_DIR}"
echo "EXEC_INFO_DIR: ${EXEC_INFO_DIR}"

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
mkdir -p "${EXEC_INFO_DIR}"

# cd into SRC_DIR so the Python script's own relative-path candidate search (a bare
# .ini filename, a bare 'exec_info') resolves correctly. Without this, CWD is whatever
# cron started the script in (typically $HOME), not this script's own directory, and
# none of the Python script's candidates would find anything.
cd "${SRC_DIR}" || exit_script 3

echo "executing $PYTHON_CMD augment_with_entity_info.py with PYTHONPATH=${PYTHONPATH}"
"$PYTHON_CMD" augment_with_entity_info.py --project-dir "${PROJECT_DIR}" > "${EXEC_INFO_DIR}/augment_with_entity_info_python_output.log" 2>&1
exit_script $?
