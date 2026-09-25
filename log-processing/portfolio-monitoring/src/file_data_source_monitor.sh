#!/bin/bash

#####################################################################################
# Wrapper for file_data_source_monitor.py to monitor files which are external
# data sources for the projects in the log-processing portfolio.
#
# Uses the shared venv, reqs (replacing site-packages), and src of the log-processing
# portfolio.
#####################################################################################

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}
function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}

enter_script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"        # .../log-processing/portfolio-monitoring
PORTFOLIO_DIR="$(dirname "$PROJECT_DIR")"     # .../log-processing
echo SCRIPT_DIR=$SCRIPT_DIR
echo PROJECT_DIR=$PROJECT_DIR
echo PORTFOLIO_DIR=$PORTFOLIO_DIR
SRC_DIR="$SCRIPT_DIR"
PORTFOLIO_SRC_DIR="$PORTFOLIO_DIR/src"
REQS_DIR="$PORTFOLIO_DIR/reqs"

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

# Extend the PYTHONPATH inherited to include the shared portfolio reqs and
# src as well as this projects src.
NEW_PYTHONPATH="$PYTHONPATH"
for dir in "$REQS_DIR" "$SRC_DIR" "$PORTFOLIO_SRC_DIR"; do
    case ":$NEW_PYTHONPATH:" in
        *":$dir:"*) ;;
        *) NEW_PYTHONPATH="$NEW_PYTHONPATH:$dir" ;;
    esac
done
export PYTHONPATH="$NEW_PYTHONPATH"
echo "Exported PYTHONPATH=$PYTHONPATH"
echo "executing $PYTHON_CMD file_data_source_monitor.py with PYTHONPATH=$PYTHONPATH"
$PYTHON_CMD "$SCRIPT_DIR/file_data_source_monitor.py" --project-dir "$PROJECT_DIR"
rc=$?
exit_script $rc
