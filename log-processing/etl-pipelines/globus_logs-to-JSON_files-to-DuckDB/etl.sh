#!/bin/bash

#####################################################################################
# ETL pipeline to gather File Downloads analytic data, using one portfolio project to
# parse log files to JSON and another portfolio project to load that JSON to DuckDB.
#####################################################################################

LOG_PROCESSING_DIR="/hive/users/hive/scripts/PittCronJobs/analytics/usage-analytics/log-processing"
EXTRACT_TRANSFORM_SCRIPT="${LOG_PROCESSING_DIR}/globus-downloads-to-JSON/src/execute_phase_1-to-4.sh"
LOADER_SCRIPT="${LOG_PROCESSING_DIR}/analytics-platform-loading/src/duckdb_loader.sh"

"${EXTRACT_TRANSFORM_SCRIPT}" --now
rc=$?
if [ $rc -ne 0 ]; then
    echo "Extract and transform script exited $rc -- not proceeding to the load script."
    exit $rc
fi

"${LOADER_SCRIPT}"
exit $?
