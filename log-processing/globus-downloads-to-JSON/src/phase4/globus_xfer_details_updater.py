"""
Extraction and transformation of Globus Usage Details for the
globus-downloads-to-JSON pipeline's File Downloads.  Output is to JSON
files created during an earlier phase of this pipeline.

N.B. This ETL of File Downloads logged file transfers is assumed
     to be a nightly process, even if delivery of data means there are
     many nights with little or no change (e.g. waiting for a new
     "usage details" spreadsheet from Globus most of the month.)

N.B. The ETL processes all logged file download events that it can,
     without being restrained by the delivery dates of other data, such
     as the Globus Usage Details spreadsheet.  Placeholder values
     are inserted during earlier phases of this pipeline, such as the
     ones this process tries to update.  These placeholder remain
     unchanged until the new enough information is received to
     update them or mark them UNRESOLVED.

Sentinel files should exist along with each JSON file with file transfer
content.  Comments in the analytics-platform-loading loading process
describe the loader's interpretation of sentinel files.  This
globus-downloads-to-JSON process's manipulation of sentinel files is
described in the comments of the gridftp_log_extract.py file.

The content JSON files this process creates may contain information not
loaded into the DuckDB tables, notably the `provenance` field.
"""
import os
import argparse
import sys
import configparser
import logging
import copy
import re
import csv
import json
import ast
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils
from log_extract_xfer_utils import parse_datetime_flexible

tz_pgh = ZoneInfo(key='America/New_York')
tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('Resolving PENDING fields using Globus transfer details')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase4' -> ':four:'), so relocating a script to a different phase directory
# automatically updates its emoji with no code change. Falls back to ':diamonds:'
# if the parent directory doesn't match the 'phaseN' pattern.
_PHASE_NUMBER_WORDS = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven', 8: 'eight', 9: 'nine'}
_phase_dir_match = re.match(r'^phase(\d+)$', Path(__file__).resolve().parent.name)
if _phase_dir_match and int(_phase_dir_match.group(1)) in _PHASE_NUMBER_WORDS:
    SLACK_PHASE_EMOJI = f":{_PHASE_NUMBER_WORDS[int(_phase_dir_match.group(1))]}:"
else:
    SLACK_PHASE_EMOJI = ':diamonds:'

# arg_project_dir is provided as an argument by the .sh wrapper calling this program from
# its BASH_SOURCE-derived PROJECT_DIR (e.g. globus-downloads-to-JSON).
arg_parser = argparse.ArgumentParser(description='Resolve PENDING fields using the Globus transfer-details CSV.')
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROJECT_DIR.")
args = arg_parser.parse_args()
arg_project_dir = args.project_dir
arg_portfolio_dir = os.path.dirname(arg_project_dir) # one level above arg_project_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

# NOTE: this script lives one directory deeper than before (src/phase4/, not src/
# directly), so its own vm001-default candidate needs the extra phase4/ segment, and
# the PyCharm-dev candidate needs an extra '../' level.
process_ini_candidates = [
    Path('globus_xfer_details_updater.ini'),                                              # Docker WORKDIR
    Path(f'{arg_project_dir}/src/phase4/globus_xfer_details_updater.ini'),                # vm001 default
    Path('../../../globus-downloads-to-JSON/src/phase4/globus_xfer_details_updater.ini'), # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find globus_xfer_details_updater.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)
try:
    # The PROJECT_NAME pulled from the INI file should match the script variable PROJECT_DIR
    # in the bash script executing this program.
    # PROJECT_NAME must match the PROJECT_NAME used by globus_access_log_extract.py,
    # gridftp_log_extract.py, and augment_with_entity_info.py -- that's the directory
    # under JSON_FILE_NIGHTLY_DIR where this process reads sentinel files for completion markers.
    PROJECT_NAME = Config.get('ProcessSpecificSettings', 'PROJECT_NAME')
    SLACK_NOTIFICATION_CHANNEL = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATION_CHANNEL')
    SLACK_BAD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_NEWS_EMOJI')
    SLACK_GOOD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_NEWS_EMOJI')
    SLACK_NEUTRAL_INFO_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_INFO_EMOJI')
    SLACK_NOTIFICATIONS = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATIONS')
    slack_user_id_mentions_on_error_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR'))
    slack_user_id_mentions_on_success_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS'))
except Exception as e:
    print(f"\a\nUnable to read configuration from '{config_file_name}'.\n")
    sys.exit(3)
print('Process-specific configuration loaded')

#
# Set up a logger in the configured directory for the current execution.
#
exec_info_dir_candidates = [
    Path('exec_info'),                          # Docker WORKDIR
    Path(f'{arg_project_dir}/exec_info'),       # vm001 default
    Path(f'../../../{PROJECT_NAME}/exec_info'), # PyCharm dev
]
exec_info_dir = None
for candidate in exec_info_dir_candidates:
    if candidate.is_dir():
        exec_info_dir = str(candidate.resolve())
        break
if not exec_info_dir:
    print(f'Unable to find exec_info directory in any expected location.')
    sys.exit(3)
log_file_name = f"{exec_info_dir}" \
                f"/globus_xfer_details_updater-" \
                f"{datetime.now().strftime('%Y-%m-%d_%H%M%s')}" \
                f".log"
logging.basicConfig(filename=log_file_name
                    ,level=logging.INFO
                    ,format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    ,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

print('Logger instantiated')

portfolio_utils = None
try:
    config_file_location = None
    candidates = [
        Path('logProcessingProject.ini'),                          # Docker WORKDIR
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'), # vm001 default
        Path('../../../src/logProcessingProject.ini'),             # PyCharm dev
    ]
    for candidate in candidates:
        if candidate.is_file():
            config_file_location = candidate.resolve()
            break
    portfolio_utils = LogExtractXferUtils(config_file_name=config_file_location
                                          , disable_slack_notifications=(SLACK_NOTIFICATIONS == 'DISABLED'))
    portfolio_config = portfolio_utils.get_config()
    print('Shared log processing configuration loaded.')
    TRANSFER_DETAIL_FILE = portfolio_config['TRANSFER_DETAIL_FILE']
    JSON_FILE_NIGHTLY_DIR = portfolio_config['JSON_FILE_NIGHTLY_DIR']
    node_dir_list = ast.literal_eval(portfolio_config['NODE_LOG_DIR_LIST'])
    logger.info("LogExtractXferUtils instantiated.")
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

#
# Set more global constants specific to parsing files and generating JSON.
#
# Set up a paths with PCRE regular expressions for sentinel files indicating the
# previous phase has been done, and that this phase and subsequent phases have not.
STAGE3_DONE_PATTERNS = [
    re.compile(r'^globus_access_log-\d{8}\.json\.DONE\.1\.2\.3$')
    ,re.compile(r'^gridftp\.log-\d{8}\.json\.DONE\.1\.2\.3$')
]

# Marker filenames indicating a file went through this process (stage 4) at least once
# but was left provisional: it still had a 'PENDING' record when this process last touched
# it, meaning the usage CSV hadn't yet caught up to that record's download_date_time. Since
# every phase runs every invocation, a file left provisional leaves a sentinel file
# '.LOADED.1.2.3.4' -- this indicates the file is not DONE yet.  Processes which encounter
# provisional sentinel files need to handle them until the to '.DONE.1.2.3.4'.
STAGE4_LOADED_RETRY_PATTERNS = [
    re.compile(r'^globus_access_log-\d{8}\.json\.LOADED\.1\.2\.3\.4$')
    ,re.compile(r'^gridftp\.log-\d{8}\.json\.LOADED\.1\.2\.3\.4$')
]

# The key this process uses for its own entry in each JSON object's provenance dict.
UPDATER_PROVENANCE_KEY = Path(__file__).stem

# Verify any expectations about the configuration are valid. Print
# messages for each expectation not met and halt if there are any.
def verify_configuration_expectations():
    global node_dir_list

    exit_rather_than_return=False
    if not os.path.exists(JSON_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding JSON_FILE_NIGHTLY_DIR at "
              f"'{JSON_FILE_NIGHTLY_DIR}'")
        exit_rather_than_return = True
    if not os.path.isfile(TRANSFER_DETAIL_FILE):
        print(f"Halting program due to not finding TRANSFER_DETAIL_FILE at "
              f"'{TRANSFER_DETAIL_FILE}'")
        exit_rather_than_return = True
    if not os.path.exists(exec_info_dir):
        print(f"Halting program due to not finding exec_info_dir at "
              f"'{exec_info_dir}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_yellow_circle: {portfolio_utils.get_slack_host_context()} :large_yellow_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_yellow_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_yellow_square::skull_and_crossbones: ' * 5}\n"
                    f":large_yellow_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)

# https://www.globus.org/blog/self-service-usage-reports-now-available-globus-subscribers
# https://docs.globus.org/faq/subscriptions/#globus_usage_transfer_detail_columns
#
# Returns (globus_usage_xfer_dict, coverage_latest_utc):
#   - globus_usage_xfer_dict: rows keyed by taskid
#   - coverage_latest_utc: the most recent completion_time found in the CSV, i.e. how far
#     this usage report currently extends. None if no row had a parseable completion_time.

# completion_time is used rather than request_time because a request may start within
# a "coverage window", but remain uncompleted when the report CSV is generated. Since
# Globus's usage report only covers transfers through the end of the previous month,
# coverage_latest_utc will lag real time, and completion_time will be blank for
# transfers still in progress at CSV-export time.
def load_globus_usage_xfer_from_csv():
    print(f"parsing csv from {TRANSFER_DETAIL_FILE}")
    globus_usage_xfer_dict = {}
    coverage_latest_utc = None
    with open(TRANSFER_DETAIL_FILE) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        reader = csv.DictReader(csvfile, dialect=dialect)
        for row in reader:
            if 'taskid' in row:
                globus_usage_xfer_dict[row['taskid']] = row
            else:
                logger.error(f"Missing 'taskid' to use as globus_usage_xfer_dict key in row={row}")
            if row.get('completion_time'):
                try:
                    ct_local = parse_datetime_flexible(row['completion_time'])
                except ValueError as e:
                    logger.error(f"Time conversion failure for"
                                 f" key='completion_time',"
                                 f" value={row['completion_time']},"
                                 f" e={str(e)}")
                    continue
                ct_local = ct_local.replace(tzinfo=tz_pgh)
                ct_utc = ct_local.astimezone(tz_utc)
                if coverage_latest_utc is None or ct_utc > coverage_latest_utc:
                    coverage_latest_utc = ct_utc
    logger.info(f"Loaded {len(globus_usage_xfer_dict)} transfer detail records from {TRANSFER_DETAIL_FILE}, keyed by taskid.")
    if coverage_latest_utc:
        logger.info(f"Usage CSV coverage extends through {coverage_latest_utc.isoformat()}.")
    else:
        logger.error(f"Unable to determine usage CSV coverage window from {TRANSFER_DETAIL_FILE}"
                     f" (no row had a parseable completion_time); every unmatched record will be"
                     f" left as 'PENDING' this run, since coverage can't be confirmed.")
    return globus_usage_xfer_dict, coverage_latest_utc

# Find files ready for this process by matching STAGE3_DONE_PATTERNS and
# STAGE4_LOADED_RETRY_PATTERNS for sentinel files.
def get_processable_markers():
    global node_dir_list

    marker_files = []
    all_patterns = STAGE3_DONE_PATTERNS + STAGE4_LOADED_RETRY_PATTERNS
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        for f in Path(node_json_dir_fullpath).iterdir():
            if not f.is_file():
                continue
            if not any(pattern.fullmatch(f.name) for pattern in all_patterns):
                continue
            marker_files.append(str(f))
    return marker_files

# Identify the "user's domain" by using the email address reported in the Globus Usage
# Details data.
#
# We are not using true email parsing that complies with IETF RFC 5322 or 6530. We
# manually parse the provided identity for three cases:
# - For a string with 1 @ symbol, the part after that symbol is returned.
# - For a string with 2 @ symbols, the part between the @ symbols is returned.
#   e.g. 'name@institution.edu@accounts.google.com'
# - For a string with 0 @ symbols, the placeholder UNDETERMINED is returned.
#   e.g. the placeholdeers 'PENDING', 'UNRESOLVED', 'UNTRACKED'
# - Otherwise, the string ERROR is returned.
def compute_user_domain(user: str) -> str:
    at_count = user.count('@')
    if at_count == 0:
        return 'UNDETERMINED'
    elif at_count == 1:
        return user.split('@', 1)[1]
    elif at_count == 2:
        first = user.index('@')
        second = user.index('@', first + 1)
        return user[first + 1:second]
    else:
        return 'ERROR'

# Identify the "user's top-level domain" by using the domain extracted from
# their email.  This is taken to be the last two elements of the period-separated
# domain, ignoring subdomains. This not only the last element, a true IANA entry in
# the Root Zone Database.
def compute_user_tld(domain: str) -> str:
    dot_count = domain.count('.')
    if dot_count < 2:
        return domain
    parts = domain.split('.')
    return '.'.join(parts[-2:])

# Given one transfer record from a content JSON file, resolve whichever 'PENDING'-valued
# fields the transfer-details CSV can supply, and stamp this run into the 'provenance'
# field of the JSON.
# Three outcomes for 'user':
#   - a real identity, if the CSV has a matching taskid with a usable owner_identity_name
#   - 'UNRESOLVED', if the CSV's coverage window already extends past this transfer's date
#     but there's still no usable match.
#   - unchanged ('PENDING'), if the CSV doesn't yet cover this transfer's date -- a future
#     run, once Globus's usage report catches up, gets another chance to resolve it
def resolve_tbd_fields(transfer_record:dict, xfer_details_dict:dict, csv_coverage_latest_utc, run_provenance:dict)->tuple[dict,bool]:
    resolved_any = False
    g_tid = transfer_record.get('globus_task_id')

    # user_info is created by Phase 1 now (both extraction scripts), already correctly
    # positioned in the record. setdefault is just defensive -- Phase 1 should always
    # have created this already.
    user_info = transfer_record.setdefault('user_info', {})

    if user_info.get('user') == 'PENDING':
        owner = None
        if g_tid and g_tid != 'NOT_FOUND' and g_tid in xfer_details_dict:
            owner = xfer_details_dict[g_tid].get('owner_identity_name')

        if owner:
            user_info['user'] = owner
            resolved_any = True
        else:
            # No usable match. Whether that's a settled 'UNRESOLVED' or still just 'PENDING'
            # depends on whether the CSV's coverage window has caught up to this transfer yet.
            transfer_dt = None
            try:
                transfer_dt = datetime.strptime(transfer_record['download_date_time']
                                                ,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=tz_utc)
            except Exception as e:
                logger.exception(f"Unable to parse download_date_time="
                                  f"{transfer_record.get('download_date_time')!r} for taskid={g_tid};"
                                  f" leaving 'user' as 'PENDING'.")

            if transfer_dt and csv_coverage_latest_utc and transfer_dt <= csv_coverage_latest_utc:
                user_info['user'] = 'UNRESOLVED'
                logger.debug(f"No usable transfer-detail match for taskid={g_tid}, but the usage CSV"
                             f" already covers {transfer_dt.isoformat()}; marking 'UNRESOLVED'.")
            else:
                logger.debug(f"No transfer-detail match yet for taskid={g_tid}; usage CSV covers"
                             f" through"
                             f" {csv_coverage_latest_utc.isoformat() if csv_coverage_latest_utc else 'unknown'},"
                             f" leaving 'user' as 'PENDING'.")

    # user_domain and user_tld are always computed here, for every record this pass
    # touches -- HTTP records included, even though they never go through the PENDING
    # branch above (their 'user' is either already a real value or 'UNTRACKED'). Since
    # Phase 1 already added user_info in the right position, these fields just
    # get added into it directly without restructuring of the outer record.
    user_info['user_domain'] = compute_user_domain(user_info.get('user'))
    user_info['user_tld'] = compute_user_tld(user_info['user_domain'])

    # Provenance is stamped unconditionally on every record this pass
    # touches -- this timestamp reflects the last time this process actually ran and
    # confirmed the current value is still the correct value, not just the last
    # time the value changed. "When did we last check this" is itself meaningful
    # information, separate from "did the check change anything."
    if 'provenance' in transfer_record:
        transfer_record['provenance'][UPDATER_PROVENANCE_KEY] = copy.deepcopy(run_provenance)

    return transfer_record, resolved_any

# True if any record in transfer_records still has user_info.user == 'PENDING', indicating
# the usage CSV hasn't yet caught up to that record's download_date_time.  Once the result
# is False, the content of this file can be called permanently settled. This should occur
# when CSV's own completion_time coverage is after every JSON Objects's download_date_time.
def has_pending_records(transfer_records:list)->bool:
    return any(r.get('user_info', {}).get('user') == 'PENDING' for r in transfer_records)

# Determine the content JSON file name for a given sentinel file name which this
# process works with, and also whether or not the the file remains in a provisional
# state to be resolved during a future run. Cases cover everything accepted by
# STAGE3_DONE_PATTERNS and STAGE4_LOADED_RETRY_PATTERNS.
#
# N.B. simple/hard-coded for now. Atomic temp-file replace for the data file,
# and the read-only permission handling to be evaluated in the future.
def process_marker_file(marker_filename:str, xfer_details_dict:dict, csv_coverage_latest_utc):
    if marker_filename.endswith('.DONE.1.2.3'):
        data_filename = re.sub(r'\.DONE\.1\.2\.3$', '', marker_filename)
    elif marker_filename.endswith('.LOADED.1.2.3.4'):
        data_filename = re.sub(r'\.LOADED\.1\.2\.3\.4$', '', marker_filename)
    else:
        logger.error(f"Marker '{marker_filename}' doesn't match either a stage-3-done or a"
                      f" stage-4-loaded-retry pattern; skipping.")
        return None

    if not os.path.isfile(data_filename):
        logger.error(f"Marker '{marker_filename}' exists but data file '{data_filename}' does not; skipping.")
        return None

    with open(data_filename, 'r') as f:
        transfer_records = json.load(f)

    run_provenance = {
        'process_script': os.path.basename(__file__)
        , 'process_utc_dt': datetime.now(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        , 'local_file': data_filename
    }

    resolved_count = 0
    for idx, transfer_record in enumerate(transfer_records):
        transfer_records[idx], resolved = resolve_tbd_fields(transfer_record=transfer_record
                                                             , xfer_details_dict=xfer_details_dict
                                                             , csv_coverage_latest_utc=csv_coverage_latest_utc
                                                             , run_provenance=run_provenance)
        if resolved:
            resolved_count += 1

    with open(data_filename, "w") as jf:
        jf.write(json.dumps(transfer_records))

    if has_pending_records(transfer_records):
        new_marker = f"{data_filename}.LOADED.1.2.3.4"
        still_loaded = True
    else:
        new_marker = f"{data_filename}.DONE.1.2.3.4"
        still_loaded = False

    if marker_filename != new_marker:
        os.rename(marker_filename, new_marker)

    logger.info(f"Wrote {len(transfer_records)} records ({resolved_count} newly resolved) to"
                f" '{data_filename}'; marker is now '{new_marker}'"
                f" ({'still LOADED, will retry a future run' if still_loaded else 'advanced to DONE'}).")
    return data_filename, still_loaded

if __name__ == '__main__':
    msg =   f":large_yellow_circle: {portfolio_utils.get_slack_host_context()} :large_yellow_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_yellow_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to resolve PENDING fields in" \
            f" JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}\n" \
            f" using Globus Usage Details.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_yellow_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Loaded exactly once for the whole run, pulling the entire Globus Usage
    # Details data into memory. Every subsequent lookup reuses this same
    # instance.
    xfer_details_dict, csv_coverage_latest_utc = load_globus_usage_xfer_from_csv()

    marker_files = get_processable_markers()
    logger.info(f"Found {len(marker_files)} files ready to process (stage-3-done or loaded-retry).")

    advanced_to_done_count = 0
    still_loaded_count = 0
    failed_count = 0
    for marker_filename in marker_files:
        try:
            result = process_marker_file(marker_filename=marker_filename
                                         ,xfer_details_dict=xfer_details_dict
                                         ,csv_coverage_latest_utc=csv_coverage_latest_utc)
            if not result:
                failed_count += 1
                continue
            data_filename, still_loaded = result
            if still_loaded:
                still_loaded_count += 1
            else:
                advanced_to_done_count += 1
        except Exception as e:
            logger.exception(f"Error processing '{marker_filename}'.")
            failed_count += 1

    if failed_count > 0:
        bad_news = (f":large_yellow_circle: {portfolio_utils.get_slack_host_context()} :large_yellow_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_yellow_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" {failed_count} of {failed_count + advanced_to_done_count + still_loaded_count} files could not be processed. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_yellow_square::skull_and_crossbones: ' * 5}\n"
                    f":large_yellow_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":large_yellow_circle: {portfolio_utils.get_slack_host_context()} :large_yellow_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_yellow_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" {advanced_to_done_count} files advanced to DONE, {still_loaded_count} still LOADED"
                 f" (will retry a future run), {failed_count} failed.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':yellow_heart: ' * 5}\n"
                 f":large_yellow_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0 if failed_count == 0 else 2)
