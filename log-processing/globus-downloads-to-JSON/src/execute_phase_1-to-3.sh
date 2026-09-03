#!/bin/bash

####################################################################################################
# Orchestration script for the globus-downloads-to-JSON pipeline.
#
# Lives in src/, alongside the phase scripts it runs:
#   Phase 1: globus_access_log_extract.sh
#   Phase 1: gridftp_log_extract.sh
#   Phase 2: globus_xfer_details_updater.sh
#   Phase 3: geolocation_details_updater.sh
#
# This script is intended to be invoked by cron with the --now flag. It is written for that
# use case, not for interactive/exploratory use -- argument checking is deliberately minimal.
#
# Execution environments supported:
#   VENV_ENV   — venv/ exists one level up from src/ (i.e. at the project root) and
#                provides an isolated Python interpreter (`python3 -m venv venv`).
#                Its own site-packages is intentionally left empty -- dependencies
#                are NOT installed into it. Instead they come from reqs/ (see below),
#                added to PYTHONPATH, so there's a single place packages live rather
#                than two separate installs to keep in sync.
#   IMAGE_ENV  — no venv/ present; assumes a Docker image where the system
#                Python already has all dependencies installed (or, later, the
#                image also reuses reqs/ via this same PYTHONPATH pattern).
#
# reqs/, also at the project root, holds all installed dependencies -- populated via
# `pip install --target=reqs -r requirements.txt` (run with the venv activated, so the
# venv's own pip/python version is what's used). This script adds reqs/ to PYTHONPATH
# whenever it exists, in both VENV_ENV and IMAGE_ENV, so whichever Python is running
# finds packages there.
#
# The script executes from its own directory (src/), so the phase scripts alongside it
# are resolved relative to that location.
#
# Exports HOST_HOSTNAME (the short hostname, e.g. "dtn03") for phase scripts' Python
# processes to read via util/host_context.py, so Slack messages can identify which
# host ran the job. This also future-proofs a Docker pivot: a future container
# launcher just needs to forward this same variable into `docker run -e HOST_HOSTNAME=...`
# and the physical/VM host stays identifiable even from inside a container.
####################################################################################################

function print_help() {
    cat <<EOF
Usage: $0 --now

Runs the globus-downloads-to-JSON pipeline (Phases 1-3) in sequence:
    Phase 1: globus_access_log_extract.sh
    Phase 1: gridftp_log_extract.sh
    Phase 2: globus_xfer_details_updater.sh
    Phase 3: geolocation_details_updater.sh

This is a cron-facing script, not a general-purpose CLI. --now is required to actually
run the pipeline; that is a deliberate guard against accidental invocation, not the start
of a larger option set.

  --now      Run all phases now.
  --help     Show this help and exit.
  (no args)  Show this help and exit.
EOF
}

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}

function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}

function run_phase() {
    local description="$1"
    shift
    echo "=== $description ==="
    "$@"
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "!!! '$description' exited with code $rc -- halting rather than continuing to the next phase."
        exit_script $rc
    fi
    echo
}

# --- Argument handling ------------------------------------------------------
# Deliberately minimal: this script is meant to be called by cron with --now,
# not explored interactively, so it doesn't try to be robust against misuse.
if [ $# -eq 0 ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    print_help
    exit 0
fi

if [ "$1" != "--now" ]; then
    echo "Unrecognized argument: $1" >&2
    print_help
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/exec_info"
mkdir -p "${LOG_DIR}"

# Append everything from here on (stdout and stderr) to a log in exec_info/,
# while still echoing to the console for cron's benefit / interactive runs.
LOG_FILE="${LOG_DIR}/execute_phase_1-to-3.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

enter_script

# Exported so each phase's Python process (via util/host_context.py) can label
# Slack messages with the real host, not just whatever hostname it sees locally.
# On bare metal this matches the kernel hostname anyway; the export mainly exists
# so a future Docker-based launcher can forward it into `docker run -e HOST_HOSTNAME=...`
# and Slack messages keep identifying the physical/VM host after that pivot.
export HOST_HOSTNAME="$(hostname -s)"
echo "Host: ${HOST_HOSTNAME}"

# reqs/ is the single location where dependencies actually live (filled via
# `pip install --target=reqs -r requirements.txt`). Add it to PYTHONPATH whenever
# it exists -- venv/'s own site-packages is intentionally empty, and this same
# addition covers a future IMAGE_ENV that reuses reqs/ the same way.
REQS_DIR="${PROJECT_ROOT}/reqs"
if [ -d "${REQS_DIR}" ]; then
    export PYTHONPATH="${REQS_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
    echo "PYTHONPATH includes: ${REQS_DIR}"
fi

# Determine execution environment and set PYTHON_CMD accordingly
VENV_ACTIVATE="${PROJECT_ROOT}/venv/bin/activate"
if [ -f "${VENV_ACTIVATE}" ]; then
    EXEC_ENV=VENV_ENV
    source "${VENV_ACTIVATE}"
    PYTHON_CMD="python"
    echo "Activated virtual environment: ${PROJECT_ROOT}/venv"
else
    EXEC_ENV=IMAGE_ENV
    PYTHON_CMD="python3"
    echo "No venv/ virtual environment found — using system Python: $(which python3)"
fi

echo "Execution environment: ${EXEC_ENV}"
echo "Python: $(which ${PYTHON_CMD}) -- $(${PYTHON_CMD} --version)"
echo "Logging to ${LOG_FILE}"

# Execute all phase scripts from the directory where this script resides (src/)
cd "${SCRIPT_DIR}"

run_phase "Phase 1: globus_access_log_extract.sh" "${SCRIPT_DIR}/globus_access_log_extract.sh"
run_phase "Phase 1: gridftp_log_extract.sh" "${SCRIPT_DIR}/gridftp_log_extract.sh"
run_phase "Phase 2: globus_xfer_details_updater.sh" "${SCRIPT_DIR}/globus_xfer_details_updater.sh"
run_phase "Phase 3: geolocation_details_updater.sh" "${SCRIPT_DIR}/geolocation_details_updater.sh"

echo "All phases complete."
exit_script 0
