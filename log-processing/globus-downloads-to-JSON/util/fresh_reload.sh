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
# Currently scoped to July's smoke-test data (audit_stage_completeness.py's own defaults).
# When this moves to the full corpus, update that script's defaults, or add
# --data-root/--out-root arguments here and thread them through, whichever fits at the time.
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
CLAUDE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_DIR="$(dirname "$CLAUDE_DIR")"        # .../globus-downloads-to-JSON
SRC_DIR="$PROCESS_DIR/src"

echo CLAUDE_DIR=$CLAUDE_DIR
echo PROCESS_DIR=$PROCESS_DIR
echo SRC_DIR=$SRC_DIR

echo "=== Cleanup ==="
rm -f /hive/hubmap/pitt-analytics/globus-downloads-to-JSON/{dtn03,dtn02,app001}/*.json
rm -f /hive/hubmap/pitt-analytics/globus-downloads-to-JSON/{dtn03,dtn02,app001}/*.json.DONE.1*

# Read the real TRACKING_FILE path out of the actual ini rather than assuming it lives at
# $SRC_DIR/gridftp_log_extract_tracking.json -- that assumption was wrong (the real ini can
# point anywhere, e.g. an absolute PSC path even while testing locally) and silently left
# stale tracking-file entries in place, which is exactly what caused gridftp_log_extract.py
# to skip every file as "already processed" despite the actual .json output being deleted.
GRIDFTP_TRACKING_FILE=$(grep -E "^TRACKING_FILE=" "$SRC_DIR/gridftp_log_extract.ini" | tail -1 | cut -d'=' -f2-)
if [ -z "$GRIDFTP_TRACKING_FILE" ]; then
    echo "!!! Could not find TRACKING_FILE in $SRC_DIR/gridftp_log_extract.ini -- halting rather than guessing."
    exit_script 3
fi
echo "Resetting tracking file: $GRIDFTP_TRACKING_FILE"
echo '{}' > "$GRIDFTP_TRACKING_FILE"
echo "Cleanup done."
echo

run_phase "Phase 1: globus_access_log_extract.sh" "$SRC_DIR/globus_access_log_extract.sh"
run_phase "Phase 1: gridftp_log_extract.sh" "$SRC_DIR/gridftp_log_extract.sh"
run_phase "Checking stage 1" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 1 --summary

run_phase "Phase 2: globus_xfer_details_updater.sh" "$SRC_DIR/globus_xfer_details_updater.sh"
run_phase "Checking stage 2" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 2 --summary

run_phase "Phase 3: geolocation_details_updater.sh" "$SRC_DIR/geolocation_details_updater.sh"
run_phase "Checking stage 3" python3 "$CLAUDE_DIR/audit_stage_completeness.py" 3 --summary

echo "All phases complete."
exit_script 0
