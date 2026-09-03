#!/bin/bash
####################################################################################################
# Reports every JSON output file whose sentinel is currently open (.LOADED.1.2.3, meaning at
# least one record is still PENDING and this file will be retried on a future run), plus any
# file where the sentinel disagrees with what the file's own content implies:
#
#   - A '.DONE.1.2.3' file that still has a PENDING record inside it -- dangerous: DONE is
#     supposed to mean fully settled, and this would mean it never gets retried.
#   - A '.LOADED.1.2.3' file with no PENDING record left -- a stale marker that should have
#     advanced to DONE but didn't (e.g. a partial failure left it behind).
#   - No recognized sentinel at all -- still mid-pipeline, or a prior run failed on it.
#
# Ordinary, correctly-settled DONE files (no PENDING, sentinel already says DONE) are the
# large majority of the corpus and are silently skipped -- this reports what's still open or
# wrong, not what's already fine.
#
# UNRESOLVED is reported alongside PENDING for context, since it's often what QA conversations
# actually care about -- but it plays no part in DONE-vs-LOADED itself; see punchlist.md.
####################################################################################################
cd /hive/hubmap/pitt-analytics/globus-downloads-to-JSON

for f in dtn03/*.json dtn02/*.json app001/*.json; do
    [ -f "$f" ] || continue

    read -r pending unresolved < <(jq -r '"\([.[] | select(.user_info.user == "PENDING")] | length) \([.[] | select(.user_info.user == "UNRESOLVED")] | length)"' "$f")

    if [ -f "${f}.DONE.1.2.3" ]; then
        sentinel="DONE"
    elif [ -f "${f}.LOADED.1.2.3" ]; then
        sentinel="LOADED"
    else
        sentinel="MISSING"
    fi

    expected="DONE"
    [ "$pending" -gt 0 ] && expected="LOADED"

    if [ "$sentinel" = "DONE" ] && [ "$expected" = "DONE" ]; then
        continue
    fi

    flag=""
    [ "$sentinel" != "$expected" ] && flag=" <<< MISMATCH"

    printf '%s\tpending=%s\tunresolved=%s\tsentinel=%s\texpected=%s%s\n' "$f" "$pending" "$unresolved" "$sentinel" "$expected" "$flag"
done
