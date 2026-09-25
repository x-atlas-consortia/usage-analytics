#!/bin/bash
cd /hive/hubmap/pitt-analytics/globus-downloads-to-JSON

total=0
file_count=0
for f in dtn03/*.json dtn02/*.json app001/*.json; do
    [ -f "$f" ] || continue
    n=$(jq 'length' "$f")
    total=$((total + n))
    file_count=$((file_count + 1))
done
echo "Files counted: $file_count"
echo "Total records: $total"
