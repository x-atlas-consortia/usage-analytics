from datetime import datetime
from dotenv import load_dotenv
import glob
import gzip
import json
import os
import re
load_dotenv()
LOG_DIR = os.getenv("GATEWAY_LOG_DIR", "/hive/hubmap/data/gateway-logs/vm001-dev")
LOG_FILENAME_PATTERN = os.getenv("GATEWAY_LOG_PATTERN", r"uwsgi-hubmap-auth\.log-(\d{8})\.gz")
OUTPUT_DIR = os.getenv("USAGE_OUTPUT_DIR", "./output")
LAST_DATE_PATH = os.getenv("USAGE_LAST_DATE_PATH", "./last_processed_date.json")
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_S3_FOLDER_NAME = os.getenv("AWS_S3_FOLDER_NAME", "")
AWS_S3_DELIM = os.getenv("AWS_S3_DELIM", "/")
CLF_TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
filename_pattern = re.compile(LOG_FILENAME_PATTERN)
usage_line_pattern = re.compile(
    r'(?P<client_ip>\S+) '
    r'(?P<caller>\S+) '
    r'(?P<user>\S+) '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\S+) '
    r'(?P<bytes>\S+) '
    r'pattern=(?P<pattern>\S+) '
    r'authority=(?P<authority>\S+)'
)
def load_last_date():
    try:
        with open(LAST_DATE_PATH) as f:
            return int(json.load(f)["last_processed_date"])
    except (FileNotFoundError, KeyError, ValueError):
        return 0
def save_last_date(date_int):
    with open(LAST_DATE_PATH, "w") as f:
        json.dump({"last_processed_date": date_int}, f)
def discover_rotated_logs():
    found = []
    for path in glob.glob(os.path.join(LOG_DIR, "*")):
        match = filename_pattern.search(os.path.basename(path))
        if match:
            found.append((int(match.group(1)), path))
    found.sort(key=lambda pair: pair[0])
    return found


def open_log_file(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, "rt", errors="replace")


def parse_line(line):
    match = usage_line_pattern.search(line)
    if match is None:
        return None
    fields = match.groupdict()
    request_parts = fields["request"].split(" ")
    if len(request_parts) != 3:
        return None
    method, path, _protocol = request_parts
    try:
        event_dt = datetime.strptime(fields["time"], CLF_TIME_FORMAT)
    except ValueError:
        return None
    return {
        "datetime": event_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "response_code": fields["status"],
        "resource_path": path,
        "resource_path_pattern": fields["pattern"],
        "http_method": method,
        "client_ip": fields["client_ip"],
        "host": fields["authority"],
    }


def process_file(path):
    records = []
    with open_log_file(path) as f:
        for line in f:
            record = parse_line(line)
            if record is not None:
                records.append(record)
    return records
def output_name(date_int):
    return f"api_usage-{date_int}.json"
def write_output(name, records):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, name)
    with open(output_path, "w") as f:
        json.dump(records, f)
    return output_path


def upload_to_s3(s3_client, local_path, name):
    object_name = f"{AWS_S3_FOLDER_NAME}{AWS_S3_DELIM}{name}"
    with open(local_path, "rb") as data:
        s3_client.upload_fileobj(data, AWS_S3_BUCKET_NAME, object_name)
    return object_name


def main():
    last_date = load_last_date()
    rotated_logs = discover_rotated_logs()

    s3_client = None
    if AWS_S3_BUCKET_NAME:
        import boto3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION_NAME"),
        )

    newly_processed = 0
    highest_date = last_date
    for date_int, path in rotated_logs:
        if date_int <= last_date:
            continue
        records = process_file(path)
        name = output_name(date_int)
        local_path = write_output(name, records)
        if s3_client is not None:
            object_name = upload_to_s3(s3_client, local_path, name)
            print(f"Uploaded {len(records)} records from {os.path.basename(path)} to s3://{AWS_S3_BUCKET_NAME}/{object_name}")
        else:
            print(f"Wrote {len(records)} records from {os.path.basename(path)} to {local_path} (S3 upload skipped: no bucket configured)")
        if date_int > highest_date:
            highest_date = date_int
        newly_processed += 1
    if highest_date > last_date:
        save_last_date(highest_date)
    if newly_processed == 0:
        print("No new rotated log files to process.")


if __name__ == "__main__":
    main()
