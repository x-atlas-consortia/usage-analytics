#!/bin/bash

####################################################################################################
# Wrapper for file_data_source_monitor.py, following the same conventions as the phase scripts in
# the sibling globus-downloads-to-JSON project: resolves its own directory via BASH_SOURCE so it
# works regardless of invocation cwd, builds its own PYTHONPATH (reqs/ + this project's src/ +
# the shared portfolio src/), and passes --process-dir explicitly rather than having the Python
# script guess its own location.
####################################################################################################

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}
function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}
enter_script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_DIR="$(dirname "$SCRIPT_DIR")"        # .../log-processing/portfolio-monitoring
PORTFOLIO_DIR="$(dirname "$PROCESS_DIR")"     # .../log-processing
echo SCRIPT_DIR=$SCRIPT_DIR
echo PROCESS_DIR=$PROCESS_DIR
echo PORTFOLIO_DIR=$PORTFOLIO_DIR
SRC_DIR="$SCRIPT_DIR"
PORTFOLIO_SRC_DIR="$PORTFOLIO_DIR/src"
REQS_DIR="$PROCESS_DIR/reqs"

# venv/ provides only the interpreter (its own site-packages stays empty by design),
# reqs/ is where actual dependencies live, added to PYTHONPATH below regardless of which Python runs this.
VENV_ACTIVATE="${PROCESS_DIR}/venv/bin/activate"
if [ -f "${VENV_ACTIVATE}" ]; then
    source "${VENV_ACTIVATE}"
    PYTHON_CMD="python"
    echo "Activated virtual environment: ${PROCESS_DIR}/venv"
else
    PYTHON_CMD="python3"
    echo "No venv/ virtual environment found -- using system Python: $(which python3)"
fi

# Build PYTHONPATH from whatever's already inherited (e.g. reqs/ from an
# orchestrator, if this is ever called from one.)  Add this project's own
# reqs/src and the shared portfolio src/ if not already present.
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
$PYTHON_CMD "$SCRIPT_DIR/file_data_source_monitor.py" --process-dir "$PROCESS_DIR"
rc=$?
exit_script $rc
