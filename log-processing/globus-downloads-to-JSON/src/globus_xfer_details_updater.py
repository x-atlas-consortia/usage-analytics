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

#
# arg_process_dir replaces the old hardcoded HIVE_DEPLOY_BASE. It's the .sh wrapper's own
# BASH_SOURCE-derived PROCESS_DIR (e.g. .../log-processing/globus-downloads-to-JSON), passed
# in explicitly rather than guessed at, so moving this whole tree to a new server or a new
# repository never requires touching this file's own content.
arg_parser = argparse.ArgumentParser(description='Resolve PENDING fields using the Globus transfer-details CSV.')
arg_parser.add_argument('--process-dir', required=True, dest='process_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROCESS_DIR.")
args = arg_parser.parse_args()
arg_process_dir = args.process_dir
arg_portfolio_dir = os.path.dirname(arg_process_dir)  # one level above arg_process_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

process_ini_candidates = [
    Path('globus_xfer_details_updater.ini'),                                    # Docker WORKDIR
    Path(f'{arg_process_dir}/src/globus_xfer_details_updater.ini'),             # vm001 default
    Path('../../globus-downloads-to-JSON/src/globus_xfer_details_updater.ini'), # PyCharm dev
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
    # PROC_NAME must match the PROC_NAME used by globus_access_log_extract.py and
    # gridftp_log_extract.py -- that's the directory under JSON_FILE_NIGHTLY_DIR this
    # process scans for their stage-1-complete markers. It is not a separate namespace.
    PROC_NAME = Config.get('ProcessSpecificSettings', 'PROC_NAME')
    NODE_LOG_DIR_LIST = Config.get('ProcessSpecificSettings', 'NODE_LOG_DIR_LIST')
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
    Path('exec_info'),                                                             # Docker WORKDIR
    Path(f'{arg_process_dir}/exec_info'),                 # vm001 default
    Path(f'../../{PROC_NAME}/exec_info'),                 # PyCharm dev
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
                    ,level=logging.DEBUG+1 # INFO
                    ,format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    ,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

print('Logger instantiated')

portfolio_utils = None
try:
    config_file_location = None
    candidates = [
        Path('logProcessingProject.ini'),                                    # Docker WORKDIR
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'),          # vm001 default
        Path('../../src/logProcessingProject.ini'),                          # PyCharm dev
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
    logger.info("LogExtractXferUtils instantiated.")
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

# Create a usable Python list global from the str in the INI file
node_dir_list = ast.literal_eval(NODE_LOG_DIR_LIST)

# Marker filenames indicating stage 1 (globus_access_log_extract.py / gridftp_log_extract.py)
# has completed for a given data file, and it's waiting on stage 2 (this process). A full match
# is required, not a substring check -- '.json.DONE.1' is itself a substring of '.json.DONE.1.2',
# so a file already advanced past stage 2 must not be re-matched here.
STAGE1_DONE_PATTERNS = [
    re.compile(r'^globus_access_log-\d{8}\.json\.DONE\.1$')
    ,re.compile(r'^gridftp\.log-\d{8}\.json\.DONE\.1$')
]

# The key this process uses for its own entry in each JSON object's provenance dict.
# N.B. This is deliberately NOT PROC_NAME -- PROC_NAME is shared with the two
#      extraction scripts to locate their output directory, and reusing it here
#      would silently overwrite their provenance entry instead of adding to it.
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
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_yellow_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_yellow_circle:\n"
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
#   - coverage_latest_utc: the most recent request_time found in the CSV, i.e. how far this
#     usage report currently extends. None if no row had a parseable request_time.
# N.B. Globus's usage report only covers transfers through the end of the previous month, so
#      coverage_latest_utc lags real time -- that's expected, not a data quality problem.
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
            if 'request_time' in row:
                try:
                    rt_local = parse_datetime_flexible(row['request_time'])
                except ValueError as e:
                    logger.error(f"Time conversion failure for"
                                 f" key='request_time',"
                                 f" value={row['request_time']},"
                                 f" e={str(e)}")
                    continue
                rt_local = rt_local.replace(tzinfo=tz_pgh)
                rt_utc = rt_local.astimezone(tz_utc)
                if coverage_latest_utc is None or rt_utc > coverage_latest_utc:
                    coverage_latest_utc = rt_utc
    logger.info(f"Loaded {len(globus_usage_xfer_dict)} transfer detail records from {TRANSFER_DETAIL_FILE}, keyed by taskid.")
    if coverage_latest_utc:
        logger.info(f"Usage CSV coverage extends through {coverage_latest_utc.isoformat()}.")
    else:
        logger.error(f"Unable to determine usage CSV coverage window from {TRANSFER_DETAIL_FILE}"
                     f" (no row had a parseable request_time); every unmatched record will be"
                     f" left as 'PENDING' this run, since coverage can't be confirmed.")
    return globus_usage_xfer_dict, coverage_latest_utc

# Find stage-1-complete markers under each configured node directory. A file already
# advanced past stage 2 has a marker ending '.DONE.1.2', not '.DONE.1', so it naturally
# stops matching here -- no separate "already done" check needed.
def get_stage1_done_markers():
    global node_dir_list

    marker_files = []
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
        for f in Path(node_json_dir_fullpath).iterdir():
            if not f.is_file():
                continue
            if not any(pattern.fullmatch(f.name) for pattern in STAGE1_DONE_PATTERNS):
                continue
            marker_files.append(str(f))
    return marker_files

# The 0-'@' case covers every current sentinel value ('PENDING', 'UNRESOLVED', 'UNTRACKED')
# and any future one, without needing to enumerate them -- none of those strings contain
# an '@'. It also covers any other unexpected non-email value the same way.
#
# Port of the LibreOffice Calc formula Karl supplied for user_domain, given a user/email
# string. Deliberately naive/positional (counts '@' occurrences), not a real email parser.
# N.B. multi-'@' federated-identity strings like 'name@institution.edu@accounts.google.com'
# are common, real values Globus sends -- not malformed edge cases -- and the 2-'@' branch
# below is specifically what correctly extracts 'institution.edu' from those.
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

# Port of the LibreOffice Calc formula Karl supplied for user_tld, given a user_domain
# string. Deliberately naive/positional (counts '.' occurrences, takes the last two
# labels) -- not a real public-suffix-list-aware TLD lookup, and not meant to be one.
def compute_user_tld(domain: str) -> str:
    dot_count = domain.count('.')
    if dot_count < 2:
        return domain
    parts = domain.split('.')
    return '.'.join(parts[-2:])

# Given one transfer record from a data JSON file, resolve whichever 'PENDING'-valued
# fields the transfer-details CSV can supply, and stamp this run into its provenance.
# N.B. 'user' is the only field either extraction script currently marks 'PENDING'; the
#      pattern here generalizes so a future PENDING field could be added the same way.
#
# Three outcomes for 'user':
#   - a real identity, if the CSV has a matching taskid with a usable owner_identity_name
#   - 'UNRESOLVED', if the CSV's coverage window already extends past this transfer's date
#     but there's still no usable match -- a settled "checked, not there" outcome
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
    # user_info already exists in the right position (Phase 1's job now), these just
    # get added into it directly -- no restructuring of the outer record needed.
    user_info['user_domain'] = compute_user_domain(user_info.get('user'))
    user_info['user_tld'] = compute_user_tld(user_info['user_domain'])

    if 'provenance' in transfer_record:
        transfer_record['provenance'][UPDATER_PROVENANCE_KEY] = copy.deepcopy(run_provenance)

    return transfer_record, resolved_any

# Read the data file for one stage-1-complete marker, resolve what the transfer-details CSV
# allows, overwrite the same data file in place, and advance the marker from '.DONE.1' to
# '.DONE.1.2'. Returns the data file path on success, or None on failure.
# N.B. simple/hard-coded for now, per plan -- atomic temp-file replace for the data file,
# and the read-only permission handling, come with next week's checklist pass.
def process_marker_file(marker_filename:str, xfer_details_dict:dict, csv_coverage_latest_utc)->str:
    data_filename = re.sub(r'\.DONE\.1$', '', marker_filename)
    if not os.path.isfile(data_filename):
        logger.error(f"Marker '{marker_filename}' exists but data file '{data_filename}' does not; skipping.")
        return None

    with open(data_filename, 'r') as f:
        transfer_records = json.load(f)

    run_provenance = {
        'process_script': os.path.basename(__file__)
        , 'process_utc_dt': datetime.now(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        , 'source_log_file': data_filename
        , 'destination_local_file': data_filename
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

    new_marker = f"{marker_filename}.2"
    os.rename(marker_filename, new_marker)
    logger.info(f"Wrote {len(transfer_records)} records ({resolved_count} newly resolved) to"
                f" '{data_filename}'; advanced marker to '{new_marker}'.")
    return data_filename

if __name__ == '__main__':
    msg =   f":large_yellow_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_yellow_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to resolve PENDING fields in" \
            f" JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}\n" \
            f" using Globus Usage Details.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_yellow_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    xfer_details_dict, csv_coverage_latest_utc = load_globus_usage_xfer_from_csv()

    marker_files = get_stage1_done_markers()
    logger.info(f"Found {len(marker_files)} stage-1-complete files to process.")

    processed_count = 0
    failed_count = 0
    for marker_filename in marker_files:
        try:
            data_filename = process_marker_file(marker_filename=marker_filename
                                                ,xfer_details_dict=xfer_details_dict
                                                ,csv_coverage_latest_utc=csv_coverage_latest_utc)
            if not data_filename:
                failed_count += 1
                continue
            processed_count += 1
        except Exception as e:
            logger.exception(f"Error processing '{marker_filename}'.")
            failed_count += 1

    if failed_count > 0:
        bad_news = (f":large_yellow_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_yellow_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" {failed_count} of {failed_count + processed_count} files could not be advanced to stage 2. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_yellow_square::skull_and_crossbones: ' * 5}\n"
                    f":large_yellow_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":large_yellow_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_yellow_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" Advanced {processed_count} files to stage 2, {failed_count} failed.\n"
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
