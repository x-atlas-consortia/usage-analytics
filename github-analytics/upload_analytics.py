"""Script to upload GitHub repository analytics to an AWS S3 bucket"""

import json
import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.request import HTTPError, Request, urlopen

import boto3

# parse command line arguments
parser = ArgumentParser(description="Upload GitHub repository analytics to an AWS S3 bucket")
parser.add_argument(
    "-r",
    "--repos",
    nargs="*",
    type=str,
    required=True,
    dest="repos",
    help="List of GitHub repositories (ex: 'owner1/repo1 owner2/repo2')",
)
parser.add_argument(
    "-t",
    "--token",
    type=str,
    required=True,
    dest="token",
    help="GitHub API token",
)
parser.add_argument(
    "-b",
    "--bucket",
    type=str,
    required=True,
    dest="bucket",
    help="AWS S3 bucket name",
)
parser.add_argument(
    "-a",
    "--access_key",
    type=str,
    default=None,
    dest="access_key",
    help="AWS Access Key ID. If not provided, credentials read from ~/.aws/credentials",
)
parser.add_argument(
    "-s",
    "--secret_key",
    type=str,
    default=None,
    dest="secret_key",
    help=("AWS Secret Access Key. If not provided, credentials read from " "~/.aws/credentials"),
)
args = parser.parse_args()

# s3 setup
s3 = boto3.resource("s3", aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key)
bucket = s3.Bucket(args.bucket)

headers = {
    "Authorization": f"Bearer {args.token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
end_date = today - timedelta(days=1)
start_date = end_date - timedelta(days=13)
end_date_str = end_date.strftime("%Y-%m-%d")
start_date_str = start_date.strftime("%Y-%m-%d")

for repo in args.repos:
    try:
        # get clone data from GitHub API
        url = f"https://api.github.com/repos/{repo}/traffic/clones?per=day"
        req = Request(url, method="GET", headers=headers)
        with urlopen(req) as res:
            b_data = res.read()
        clone_data = json.loads(b_data)

        # don't include today's date since it may be incomplete
        clone_data["clones"] = [
            d
            for d in clone_data["clones"]
            if datetime.strptime(d["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            < today
        ]

        # get views data from GitHub API
        url = f"https://api.github.com/repos/{repo}/traffic/views?per=day"
        req = Request(url, method="GET", headers=headers)
        with urlopen(req) as res:
            b_data = res.read()
        view_data = json.loads(b_data)

        # don't include today's date since it may be incomplete
        view_data["views"] = [
            d
            for d in view_data["views"]
            if datetime.strptime(d["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            < today
        ]

        # get referrer data from GitHub API
        url = f"https://api.github.com/repos/{repo}/traffic/popular/referrers?per=day"
        req = Request(url, method="GET", headers=headers)
        with urlopen(req) as res:
            b_data = res.read()
        referrer_data = json.loads(b_data)

        # combine clone and view data
        data = {"clones": clone_data, "views": view_data, "referrers": referrer_data}
        filename = f"{repo.replace('/', '-')}_{start_date_str}_{end_date_str}.json"
        b_data = json.dumps(data, separators=(",", ":")).encode("utf-8")

        # upload data to S3
        byteio = BytesIO(b_data)
        bucket.upload_fileobj(byteio, filename)
        logging.info(f"Uploaded analytics for {repo} to {filename}")
    except HTTPError as e:
        logging.error(f"Failed to get analytics for {repo}: {e.code} {e.reason}")
    except Exception as e:
        logging.error(f"Failed to upload analytics for {repo}: {e}")
