"""Script to index GitHub analytics from AWS S3 bucket to Elasticsearch/OpenSearch."""

import json
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile
from urllib.request import HTTPError, Request, urlopen

import boto3

# parse command line arguments
parser = ArgumentParser(
    description="Index GitHub analytics from AWS S3 bucket to Elasticsearch/OpenSearch"
)
parser.add_argument(
    "-e",
    "--elasticsearch",
    type=str,
    required=True,
    dest="es_base",
    help="Elasticsearch/OpenSearch base URL",
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
    help="AWS Secret Access Key. If not provided, credentials read from " "~/.aws/credentials",
)
args = parser.parse_args()

# s3 setup
s3 = boto3.resource("s3", aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key)
bucket = s3.Bucket(args.bucket)


def does_index_exist(idx_name: str) -> bool:
    """Check if an index exists in Elasticsearch/OpenSearch.

    Parameters
    ----------
    idx_name : str
        Name of the index.

    Returns
    -------
    bool
        True if index exists, False otherwise.
    """
    url = f"{args.es_base}/{idx_name}"
    req = Request(url, method="GET")
    try:
        with urlopen(req) as res:
            return res.status == 200
    except HTTPError as e:
        print(e)
        return False


def create_index(idx_name: str) -> bool:
    """Create an index in Elasticsearch/OpenSearch.

    Parameters
    ----------
    idx_name : str
        Name of the index.

    Returns
    -------
    bool
        True if index is created, False otherwise.
    """
    url = f"{args.es_base}/{idx_name}"
    req = Request(url, method="PUT")
    try:
        with urlopen(req) as res:
            print(res.read())
            return res.status == 200
    except HTTPError as e:
        print(e)
        return False


def index_data(idx_name: str, doc_id: str, data: dict) -> bool:
    """Create or update document in Elasticsearch/OpenSearch.

    Parameters
    ----------
    idx_name : str
        Name of the index.
    doc_id : str
        ID of the document.
    data : dict
        Data to be indexed.

    Returns
    -------
    bool
        True if document is indexed, False otherwise.
    """
    url = f"{args.es_base}/{idx_name}/_doc/{doc_id}"
    req = Request(url, method="PUT", data=json.dumps(data).encode("utf-8"))
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req) as res:
            print(res.read())
            return res.status == 200 or res.status == 201
    except HTTPError as e:
        print(e)
        return False


for file in bucket.objects.all():
    filename = file.key
    repo_name, start_date, end_date = filename.removesuffix(".json").split("_")

    idx_name = f"analytics_{repo_name.replace('-', '_')}"
    doc_id = f"{start_date}_{end_date}"
    if not does_index_exist(idx_name):
        res = create_index(idx_name)
        if not res:
            print(f"Failed to create index {repo_name}")
            continue

    with NamedTemporaryFile(mode="w+b") as tmp:
        bucket.download_fileobj(filename, tmp)
        tmp.seek(0)

        json_data = json.load(tmp)
        res = index_data(idx_name, doc_id, json_data)
        if not res:
            print(f"Failed to index {repo_name} for {doc_id}")
