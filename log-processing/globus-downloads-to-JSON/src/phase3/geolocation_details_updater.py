"""
Extraction and transformation of geolocation information for the
globus-downloads-to-JSON pipeline's File Downloads.  Output is to JSON
files created during an earlier phase of this pipeline.

N.B. This ETL of File Downloads logged file transfers is assumed
     to be a nightly process, even if delivery of data means there are
     many nights with little or no change (e.g. waiting for a new
     "usage details" spreadsheet from Globus most of the month.)

N.B. The ETL processes all logged file download events that it can,
     without being restrained by the delivery dates of other data, such
     as the Globus Usage Details spreadsheet.  Therefore placeholder values
     are inserted where later phases of this pipeline, such as this one,
     may add information.

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
import json
import ast
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils
from ip2geo import IP2Geo

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('Adding geolocation info using IP2Geo')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase3' -> ':three:'), so relocating a script to a different phase directory
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
arg_parser = argparse.ArgumentParser(description='Add geolocation info to stage-2-complete JSON files using IP2Geo.')
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROJECT_DIR.")
args = arg_parser.parse_args()
arg_project_dir = args.project_dir
arg_portfolio_dir = os.path.dirname(arg_project_dir)  # one level above arg_project_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

# NOTE: this script lives one directory deeper than before (src/phase3/, not src/
# directly), so its own vm001-default candidate needs the extra phase3/ segment, and
# the PyCharm-dev candidate needs an extra '../' level.
process_ini_candidates = [
    Path('geolocation_details_updater.ini'),                                               # Docker WORKDIR
    Path(f'{arg_project_dir}/src/phase3/geolocation_details_updater.ini'),                 # vm001 default
    Path('../../../globus-downloads-to-JSON/src/phase3/geolocation_details_updater.ini'),  # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find geolocation_details_updater.ini in any expected location.\n")
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
                f"/geolocation_details_updater-" \
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
    JSON_FILE_NIGHTLY_DIR = portfolio_config['JSON_FILE_NIGHTLY_DIR']
    # Licensed data (IP2Location.com) that must never be committed to the repo.
    # Read from the shared logProcessingProject.ini, not this script's own process-specific ini.
    NONPUBLIC_GEO_DB = portfolio_config['NONPUBLIC_GEO_DB']
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
STAGE2_DONE_PATTERNS = [
    re.compile(r'^globus_access_log-\d{8}\.json\.DONE\.1\.2$')
    ,re.compile(r'^gridftp\.log-\d{8}\.json\.DONE\.1\.2$')
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
    if not os.path.isfile(NONPUBLIC_GEO_DB):
        print(f"Halting program due to not finding NONPUBLIC_GEO_DB at "
              f"'{NONPUBLIC_GEO_DB}'")
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
        bad_news = (f":large_blue_circle: {portfolio_utils.get_slack_host_context()} :large_blue_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_blue_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_blue_square::skull_and_crossbones: ' * 5}\n"
                    f":large_blue_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)

# Find files ready for this process by matching STAGE2_DONE_PATTERNS for sentinel files.
def get_processable_markers():
    global node_dir_list

    marker_files = []
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        for f in Path(node_json_dir_fullpath).iterdir():
            if not f.is_file():
                continue
            if not any(pattern.fullmatch(f.name) for pattern in STAGE2_DONE_PATTERNS):
                continue
            marker_files.append(str(f))
    return marker_files

# Given one transfer record, look up destination_ip's geolocation via the shared
# IP2Geo instance and add a geolocation_info object (all 5 fields IP2Geo returns),
# positioned where destination_ip sits in the record's key order. destination_ip
# itself is a Phase 1 field and is never modified here.
# N.B. IP2Geo's own failure sentinels (INVALID/UNKNOWN/MULTIPLE geo dicts) are stored
#      as-is, not translated to this pipeline's own UNDETERMINED convention -- they
#      distinguish different failure modes IP2Geo itself already knows apart.
def add_geolocation_info(transfer_record: dict, ip2geo: IP2Geo, run_provenance: dict) -> dict:
    destination_ip = transfer_record.get('destination_ip')
    geo = ip2geo.get_ip_geo_info(destination_ip)

    geolocation_info = {
        'country_code': geo.get('country_code'),
        'country_name': geo.get('country_name'),
        'region_name': geo.get('region_name'),
        'city_name': geo.get('city_name'),
        'zip_code': geo.get('zip_code'),
    }

    if 'geolocation_info' in transfer_record:
        # Already present from a prior pass of this same logic -- update in place,
        # no need to re-find a position for it.
        transfer_record['geolocation_info'] = geolocation_info
    else:
        rebuilt = {}
        inserted = False
        for k, v in transfer_record.items():
            rebuilt[k] = v
            if k == 'destination_ip':
                rebuilt['geolocation_info'] = geolocation_info
                inserted = True
        if not inserted:
            rebuilt['geolocation_info'] = geolocation_info
        transfer_record.clear()
        transfer_record.update(rebuilt)

    if 'provenance' in transfer_record:
        transfer_record['provenance'][UPDATER_PROVENANCE_KEY] = copy.deepcopy(run_provenance)

    return transfer_record

# Read the data file for one stage-2-complete marker (see get_processable_markers), add
# geolocation_info to every record using the shared IP2Geo instance, overwrite the same
# data file in place, and advance the marker to '.DONE.1.2.3'. Unlike
# globus_xfer_details_updater.py, this process has no provisional/retry state of its own to
# preserve -- IP2Geo's lookups are a deterministic, single-pass operation, so every file it
# touches always advances straight to a clean DONE marker, never a LOADED one. Returns the
# data file path on success, or None on failure.
#
# N.B. simple/hard-coded for now. Atomic temp-file replace for the data file,
# and the read-only permission handling to be evaluated in the future.
def process_marker_file(marker_filename: str, ip2geo: IP2Geo) -> str:
    if marker_filename.endswith('.DONE.1.2'):
        data_filename = re.sub(r'\.DONE\.1\.2$', '', marker_filename)
    else:
        logger.error(f"Marker '{marker_filename}' doesn't match a stage-2-done pattern; skipping.")
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

    for idx, transfer_record in enumerate(transfer_records):
        transfer_records[idx] = add_geolocation_info(transfer_record=transfer_record
                                                      , ip2geo=ip2geo
                                                      , run_provenance=run_provenance)

    with open(data_filename, "w") as jf:
        jf.write(json.dumps(transfer_records))

    new_marker = f"{data_filename}.DONE.1.2.3"
    os.rename(marker_filename, new_marker)
    logger.info(f"Added geolocation_info to {len(transfer_records)} records in"
                f" '{data_filename}'; advanced marker to '{new_marker}'.")
    return data_filename

if __name__ == '__main__':
    msg =   f":large_blue_circle: {portfolio_utils.get_slack_host_context()} :large_blue_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_blue_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to add geolocation fields to" \
            f" JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}\n" \
            f" using the ipaddress module and data.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_blue_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Loaded exactly once for the whole run, pulling the entire geolocation
    # database into memory. Every subsequent lookup reuses this same
    # instance (including its internal per-IP cache).
    logger.info(f"Loading IP2Geo from {NONPUBLIC_GEO_DB} -- this may take a while.")
    ip2geo = IP2Geo(NONPUBLIC_GEO_DB)
    logger.info("IP2Geo loaded.")

    marker_files = get_processable_markers()
    logger.info(f"Found {len(marker_files)} stage-2-complete files to process.")

    processed_count = 0
    failed_count = 0
    for marker_filename in marker_files:
        try:
            data_filename = process_marker_file(marker_filename=marker_filename, ip2geo=ip2geo)
            if not data_filename:
                failed_count += 1
                continue
            processed_count += 1
        except Exception as e:
            logger.exception(f"Error processing '{marker_filename}'.")
            failed_count += 1

    if failed_count > 0:
        bad_news = (f":large_blue_circle: {portfolio_utils.get_slack_host_context()} :large_blue_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_blue_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" {failed_count} of {failed_count + processed_count} files could not be advanced to stage 4. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_blue_square::skull_and_crossbones: ' * 5}\n"
                    f":large_blue_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":large_blue_circle: {portfolio_utils.get_slack_host_context()} :large_blue_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_blue_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" Advanced {processed_count} files to stage 4, {failed_count} failed.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':blue_heart: ' * 5}\n"
                 f":large_blue_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0 if failed_count == 0 else 2)
