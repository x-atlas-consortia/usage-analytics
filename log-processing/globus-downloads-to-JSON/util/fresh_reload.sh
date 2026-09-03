#!/bin/bash

####################################################################################################
# Full pipeline run: cleanup, then Phase 1 -> Phase 2 -> Phase 3, checking with
# audit_stage_completeness.py --summary after each phase.
#
# Lives in claude/, a sibling to src/ that is not tracked in GitHub -- every reference to
# the actual pipeline scripts below is an explicit path into src/, not a same-directory
# relative call. Nothing here needs its own PYTHONPATH setup: the four phase scripts are
# each self-contained .sh wrappers that handle that internally, and audit_stage_completeness.py
# is pure stdlib with no project-specific imports at all.
#
# Halts immediately if any phase script exits non-zero, rather than continuing on to the
# next phase against broken or incomplete output from the one before it.
#
# Every path this script needs (both tracking files, the real log source directory, and the
# real JSON output root) is read out of the actual .ini files rather than hardcoded or left
# to audit_stage_completeness.py's own defaults -- those defaults are still July's smoke-test
# paths, not the full corpus this now runs against.
####################################################################################################

function show_help() {
    cat << 'EOF'
Usage: fresh_reload.sh --now

Full pipeline run: cleanup, then Phase 1 -> Phase 2 -> Phase 3, checking with
audit_stage_completeness.py --summary after each phase.

This is destructive: it deletes existing JSON output and resets the GridFTP
tracking file before re-running everything from scratch. --now is required as
an explicit confirmation -- running this with no arguments, or with --help,
only shows this message and does nothing else.
EOF
}

if [ "$1" != "--now" ]; then
    show_help
    exit 0
fi

function enter_script() {
    echo Begin execution $0 at `date` by `whoami`
}

function exit_script() {
    echo End execution $0 at `date` by `whoami`
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

enter_script

# Self-locating: CLAUDE_DIR is wherever this file actually is, so this still works
# regardless of the CWD it's invoked from. SRC_DIR is its sibling, not itself.
# PORTFOLIO_DIR is one level further up, where the shared logProcessingProject.ini lives.
CLAUDE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_DIR="$(dirname "$CLAUDE_DIR")"        # .../globus-downloads-to-JSON
SRC_DIR="$PROCESS_DIR/src"
PORTFOLIO_DIR="$(dirname "$PROCESS_DIR")"     # .../log-processing

echo CLAUDE_DIR=$CLAUDE_DIR
echo PROCESS_DIR=$PROCESS_DIR
echo SRC_DIR=$SRC_DIR
echo PORTFOLIO_DIR=$PORTFOLIO_DIR

# Every path below is read out of the real .ini files rather than hardcoded or assumed --
# same reasoning as the original GRIDFTP_TRACKING_FILE read: an assumed path silently goes
# stale the moment the real .ini disagrees with it, and fails quietly rather than loudly.
GRIDFTP_TRACKING_FILE=$(grep -E "^TRACKING_FILE=" "$SRC_DIR/gridftp_log_extract.ini" | tail -1 | cut -d'=' -f2-)
HTTP_TRACKING_FILE=$(grep -E "^TRACKING_FILE=" "$SRC_DIR/globus_access_log_extract.ini" | tail -1 | cut -d'=' -f2-)
LOG_DATA_ROOT=$(grep -E "^LOG_FILE_NIGHTLY_DIR=" "$SRC_DIR/gridftp_log_extract.ini" | tail -1 | cut -d'=' -f2-)
PROC_NAME=$(grep -E "^PROC_NAME=" "$SRC_DIR/gridftp_log_extract.ini" | tail -1 | cut -d'=' -f2-)
JSON_NIGHTLY_DIR=$(grep -E "^JSON_FILE_NIGHTLY_DIR=" "$PORTFOLIO_DIR/src/logProcessingProject.ini" | tail -1 | cut -d'=' -f2-)

if [ -z "$GRIDFTP_TRACKING_FILE" ] || [ -z "$HTTP_TRACKING_FILE" ] || [ -z "$LOG_DATA_ROOT" ] \
   || [ -z "$PROC_NAME" ] || [ -z "$JSON_NIGHTLY_DIR" ]; then
    echo "!!! Could not find one of TRACKING_FILE (x2), LOG_FILE_NIGHTLY_DIR, PROC_NAME, or"
    echo "    JSON_FILE_NIGHTLY_DIR in the expected .ini files -- halting rather than guessing."
    exit_script 3
fi

JSON_OUT_ROOT="$JSON_NIGHTLY_DIR/$PROC_NAME"

echo "=== Cleanup ==="
# Both marker states need clearing -- .DONE.* AND .LOADED.* (as of the 8/31 PENDING/LOADED
# change, a provisional file's marker is .LOADED.1.2 or .LOADED.1.2.3, not .DONE.*). Missing
# the LOADED glob here would delete the .json data file while leaving an orphaned LOADED
# marker pointing at nothing.
rm -f "$JSON_OUT_ROOT"/{dtn03,dtn02,app001}/*.json
rm -f "$JSON_OUT_ROOT"/{dtn03,dtn02,app001}/*.json.DONE.1*
rm -f "$JSON_OUT_ROOT"/{dtn03,dtn02,app001}/*.json.LOADED.1*

# Reset BOTH tracking files -- gridftp_log_extract.py and globus_access_log_extract.py each
# keep their own, and missing either one leaves that extractor thinking everything is
# already processed despite the .json output just having been deleted above.
echo "Resetting tracking file: $GRIDFTP_TRACKING_FILE"
echo '{}' > "$GRIDFTP_TRACKING_FILE"
echo "Resetting tracking file: $HTTP_TRACKING_FILE"
echo '{}' > "$HTTP_TRACKING_FILE"
echo "Cleanup done."
echo

run_phase "Phase 1: globus_access_log_extract.sh" "$SRC_DIR/globus_access_log_extract.sh"
run_phase "Phase 1: gridftp_log_extract.sh" "$SRC_DIR/gridftp_log_extract.sh"
run_phase "Checking stage 1" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 1 --summary \
    --data-root "$LOG_DATA_ROOT" --out-root "$JSON_OUT_ROOT"

run_phase "Phase 2: globus_xfer_details_updater.sh" "$SRC_DIR/globus_xfer_details_updater.sh"
run_phase "Checking stage 2" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 2 --summary \
    --data-root "$LOG_DATA_ROOT" --out-root "$JSON_OUT_ROOT"

run_phase "Phase 3: geolocation_details_updater.sh" "$SRC_DIR/geolocation_details_updater.sh"
run_phase "Checking stage 3" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 3 --summary \
    --data-root "$LOG_DATA_ROOT" --out-root "$JSON_OUT_ROOT"

echo "All phases complete."
exit_script 0
