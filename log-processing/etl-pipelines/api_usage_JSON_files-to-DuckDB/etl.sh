#!/bin/bash

#####################################################################################
# ETL pipeline to load API Usage analytic data into DuckDB (the api_usage table), using
# the analytics-platform-loading project's Phase 2 loader. Assumes the JSON has already
# been written by its own extract/transform process (not run here).
#
# This stands alone until an api_usage-to-JSON_files-to-DuckDB pipeline, which also
# extracts and transforms, is available to replace it.
#
# N.B. The loader waits for the DuckDB writer lock (see acquire_db_write_lock in
#      duckdb_api_usage_loader.py), so this can be started while another loader, such as
#      the File Downloads load, still has the database.
#
# Directory locations are derived from the location of this script, which is expected
# to be two levels below the log-processing portfolio directory, at
# log-processing/etl-pipelines/<this pipeline>/etl.sh.
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
LOG_PROCESSING_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOADER_SCRIPT="${LOG_PROCESSING_DIR}/analytics-platform-loading/src/phase2/duckdb_api_usage_loader.sh"

# Exported so the Python process can label Slack messages with the real host.
export HOST_HOSTNAME="$(hostname -s)"
echo "Host: ${HOST_HOSTNAME}"
echo "LOG_PROCESSING_DIR=${LOG_PROCESSING_DIR}"

"${LOADER_SCRIPT}"
rc=$?
if [ $rc -ne 0 ]; then
    echo "Load script exited $rc." >&2
fi
exit_script $rc
