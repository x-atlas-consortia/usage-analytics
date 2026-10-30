# GitHub Analytics

These scripts collect GitHub repository traffic analytics, store snapshots in S3, and index those snapshots in Elasticsearch or OpenSearch.

## Setup

Install the dependency from this directory:

```sh
python3 -m pip install -r requirements.txt
```

The scripts use the AWS credential provider chain by default, such as credentials configured with the AWS CLI or an instance role. Alternatively, pass `--access_key` and `--secret_key` to either command. The upload command also needs a GitHub token that can access traffic analytics for each repository.

## 1. Upload analytics to S3

Run `upload_analytics.py` with one or more repositories in `owner/repo` form:

```sh
export GITHUB_TOKEN="your-github-token"

python3 upload_analytics.py \
  --repos owner1/repo1 owner2/repo2 \
  --token "$GITHUB_TOKEN" \
  --bucket your-s3-bucket
```

The script requests clone, view, and popular-referrer analytics from the GitHub API. It excludes today's potentially incomplete clone and view data. Each run uploads one JSON object per repository, named:

```text
<owner>-<repo>_<start-date>_<end-date>.json
```

The date range is the 14 days ending yesterday (UTC). Re-running for the same date range writes to the same S3 key, replacing that snapshot.

Options:

- `-r`, `--repos`: required; one or more repository names, such as `owner/repo`.
- `-t`, `--token`: required; GitHub API token.
- `-b`, `--bucket`: required; destination S3 bucket.
- `-a`, `--access_key`: optional; AWS access key ID.
- `-s`, `--secret_key`: optional; AWS secret access key.

## 2. Index snapshots

Run `index_analytics.py` with the S3 bucket and the base URL of the Elasticsearch/OpenSearch endpoint:

```sh
python3 index_analytics.py \
  --bucket your-s3-bucket \
  --elasticsearch https://your-search-endpoint
```

The endpoint URL should not end in a slash. The script scans every object in the bucket, downloads each object as JSON, creates an index if needed, and writes the document. For a key such as `owner-repo_2026-10-16_2026-10-29.json`, it uses index `analytics_owner_repo` and document ID `2026-10-16_2026-10-29`.

Indexing the same snapshot again updates the document with the same ID. The endpoint must accept the script's unauthenticated HTTP requests; configure network access accordingly.

Options:

- `-e`, `--elasticsearch`: required; Elasticsearch/OpenSearch base URL.
- `-b`, `--bucket`: required; source S3 bucket.
- `-a`, `--access_key`: optional; AWS access key ID.
- `-s`, `--secret_key`: optional; AWS secret access key.

## Notes

- The indexer expects every object in the bucket to follow the snapshot filename format above. Use a dedicated bucket or prefix contents accordingly; the script currently scans the entire bucket.
- Index names replace hyphens in the owner/repository portion with underscores. Repositories whose combined owner/repository names normalize to the same value will target the same index.
- The scripts print HTTP errors and continue processing other repositories or objects where possible. Upload failures are logged.
