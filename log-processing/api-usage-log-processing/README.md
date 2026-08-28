# Gateway API Usage Log Processor (PSC)

PSC successor to the CloudWatch-based `cloudwatch_logs_to_s3.py`. Instead of
pulling from CloudWatch via boto3, it reads the HuBMAP Gateway's rotated log
files on disk, extracts one usage record per matching line, and stages a JSON
array per processed file to S3. Output schema is unchanged from the AWS version
except `response_id` has been dropped (see below).

## Assumptions

These reflect decisions still being finalized; confirm before production.

- **Source files.** Rotated, date-stamped, gzipped gateway logs named
  `uwsgi-hubmap-auth.log-YYYYMMDD.gz` in `/home/centos/hubmap/gateway/hubmap-auth/log/`.
  Both the name pattern and directory are configurable (`GATEWAY_LOG_PATTERN`,
  `GATEWAY_LOG_DIR`). If rotation is *not* switched to date-stamped + `compress`,
  this pattern must change. Only rotated logs are processed, never live.
- **Incrementality by mtime.** A single high-water-mark file
  (`last_processed_mtime.json`) records the newest file modification time
  processed, analogous to the AWS version's `last_retrieval_time.json`. For the time being this is just stored and retrieved locally, however it may end up on s3 as well like the previous implementation.  Each run
  processes only files with a newer mtime and advances the mark. Absent the
  file, everything available is processed. mtime (not the filename date) is used
  so that a size-triggered second rotation on the same day is still handled
  correctly.
- **Cadence.** Intended to run daily via cron. Days with no newly rotated file
  are a no-op. (The AWS version ran monthly; daily was adopted to avoid
  reprocessing large backlogs.)
- **Line format.** Common Log Format plus two logfmt fields (`pattern=`,
  `authority=`), optionally preceded by a Python logging prefix. Lines are
  matched by search, so non-usage lines (debug output, etc.) are skipped. Works
  whether the gateway logs usage lines into a mixed log or a future dedicated
  usage-only log.
- **Output.** One JSON array per processed file, named by the file's mtime
  (`api_usage-YYYYMMDDTHHMMSS.json`, UTC) to avoid collisions when a day
  rotates more than once. Staged to S3 via the same `AWS_S3_*` configuration as
  the AWS version.

## Fields

Seven fields, matching the AWS output minus `response_id`:
`datetime`, `response_code`, `resource_path`, `resource_path_pattern`,
`http_method`, `client_ip`, `host`.

- `response_id` dropped: it existed only to stitch multi-line CloudWatch entries
  into one request. The gateway logs one line per request, so it is unnecessary.
- `response_code` is the gateway's **authorization** outcome (200/401), not the
  service's final status. The nginx `auth_request` mechanism only yields
  200/401/403 and cannot report the true downstream code; this was accepted as
  sufficient for API Usage for the time being. This may change.
- `host` comes from the log line's `authority=` field.
- `resource_path_pattern` comes from the gateway's `pattern=` field (the route
  template, e.g. `/entities/<*>`), which only the gateway can produce.

## Configuration

Environment variables:

- `GATEWAY_LOG_DIR`, `GATEWAY_LOG_PATTERN`: source directory and filename regex.
- `USAGE_OUTPUT_DIR`: local output directory.
- `USAGE_LAST_MTIME_PATH`: high-water-mark file location.
- `AWS_S3_BUCKET_NAME`, `AWS_S3_FOLDER_NAME`, `AWS_S3_DELIM`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION_NAME`: S3 staging.
  If `AWS_S3_BUCKET_NAME` is unset, upload is skipped and boto3 is not required.

## Things still to be decided

- Final rotated filename pattern (date format, compression) once the log
  rotation policy is set.
- Final output destination. Staged to S3 for now to match the existing
  pipeline.
- Whether a dedicated usage-only log is introduced (would simplify, though the
  parser already tolerates a mixed log).
- Repo home: placed in devops alongside the AWS version for now; may move to a
  dedicated analytics repo.
